# ServiceBusReceiver, ServiceBusSession and settlement.

const SESSION_FILTER = Symbol("com.microsoft:session-filter")
const SESSION_LOCKED_UNTIL = Symbol("com.microsoft:locked-until-utc")
const DEADLETTER_CONDITION = Symbol("com.microsoft:dead-letter")
const FURTHER_RECEIVE_WAIT = 0.2      # seconds to wait for more messages after the first
const PEEK_LOCK_DRAIN_TIMEOUT = 10.0  # upper bound on waiting for a drain in PEEK_LOCK mode
const RECEIVE_AND_DELETE_DRAIN_TIMEOUT = 60.0   # ... in RECEIVE_AND_DELETE mode (see receive_messages)

"""
    ServiceBusSession

The session a session receiver is locked to (`receiver.session`). Fields:
`session_id`, `locked_until_utc`. Operations: [`get_state`](@ref),
[`set_state`](@ref), [`renew_lock`](@ref).
"""
mutable struct ServiceBusSession
    receiver::Any
    session_id::Union{Nothing,String}
    locked_until_utc::Union{Nothing,Dates.DateTime}
    established::Bool
end
Base.show(io::IO, s::ServiceBusSession) =
    print(io, "ServiceBusSession(session_id=", repr(s.session_id), ", locked_until_utc=", s.locked_until_utc, ")")

"""
    ServiceBusReceiver

Receives messages from a queue or subscription. Create with
[`get_queue_receiver`](@ref) or [`get_subscription_receiver`](@ref).
Iterating over a receiver yields messages until `max_wait_time` passes
without a new message (or forever when `max_wait_time` is `nothing`).
"""
mutable struct ServiceBusReceiver <: ServiceBusHandler
    client::ServiceBusClient
    entity_path::String
    receive_mode::ServiceBusReceiveMode.T
    max_wait_time::Union{Nothing,Float64}
    prefetch_count::Int
    session::Union{Nothing,ServiceBusSession}
    requested_session::Any
    lock::ReentrantLock
    link::Union{Nothing,Tuple{AMQP.ReceiverLink,Int}}
    mgmt::Ref{Any}
    closed::Bool
    last_peeked::Int64
    auto_lock_renewer::Any
    http_locks::Dict{UUID,String}
end

function Base.show(io::IO, r::ServiceBusReceiver)
    print(io, "ServiceBusReceiver(", r.entity_path, ", ", r.receive_mode)
    r.session === nothing || print(io, ", session_id=", repr(r.session.session_id))
    print(io, r.closed ? ", closed)" : ")")
end

function _subqueue_path(base::String, sub_queue)
    sub_queue === nothing && return base
    sq = sub_queue isa ServiceBusSubQueue.T ? sub_queue :
         lowercase(string(sub_queue)) in ("deadletter", "dead_letter") ? ServiceBusSubQueue.DEAD_LETTER :
         lowercase(string(sub_queue)) in ("transferdeadletter", "transfer_dead_letter") ?
         ServiceBusSubQueue.TRANSFER_DEAD_LETTER : throw(ArgumentError("invalid sub_queue $(sub_queue)"))
    return sq == ServiceBusSubQueue.DEAD_LETTER ? base * "/\$DeadLetterQueue" : base * "/\$Transfer/\$DeadLetterQueue"
end

function _receive_mode(m)
    m isa ServiceBusReceiveMode.T && return m
    s = lowercase(replace(string(m), "_" => ""))
    s == "peeklock" && return ServiceBusReceiveMode.PEEK_LOCK
    s == "receiveanddelete" && return ServiceBusReceiveMode.RECEIVE_AND_DELETE
    throw(ArgumentError("invalid receive_mode $(m)"))
end

function _new_receiver(c::ServiceBusClient, path::String; session_id=nothing, sub_queue=nothing,
    receive_mode=ServiceBusReceiveMode.PEEK_LOCK, max_wait_time=nothing, prefetch_count::Integer=0,
    auto_lock_renewer=nothing)
    session_id !== nothing && sub_queue !== nothing &&
        throw(ArgumentError("session_id cannot be used together with sub_queue"))
    prefetch_count < 0 && throw(ArgumentError("prefetch_count must be >= 0"))
    max_wait_time !== nothing && max_wait_time <= 0 && throw(ArgumentError("max_wait_time must be positive"))
    mode = _receive_mode(receive_mode)
    session = session_id === nothing ? nothing :
              ServiceBusSession(nothing, session_id isa ServiceBusSessionFilter.T ? nothing : string(session_id), nothing, false)
    r = ServiceBusReceiver(c, _subqueue_path(path, sub_queue), mode,
        max_wait_time === nothing ? nothing : Float64(max_wait_time), Int(prefetch_count), session, session_id,
        ReentrantLock(), nothing, Ref{Any}(nothing), false, Int64(0), auto_lock_renewer, Dict{UUID,String}())
    session === nothing || (session.receiver = r)
    return _register!(c, r)
end

"""
    get_queue_receiver(client, queue_name; session_id=nothing, sub_queue=nothing,
                       receive_mode=ServiceBusReceiveMode.PEEK_LOCK, max_wait_time=nothing,
                       prefetch_count=0, auto_lock_renewer=nothing) -> ServiceBusReceiver

`session_id` may be a session id or [`NEXT_AVAILABLE_SESSION`](@ref);
`sub_queue` may be `ServiceBusSubQueue.DEAD_LETTER` or `TRANSFER_DEAD_LETTER`.
"""
get_queue_receiver(c::ServiceBusClient, queue_name::AbstractString; kwargs...) =
    _new_receiver(c, _check_entity(c, queue_name); kwargs...)

"""
    get_subscription_receiver(client, topic_name, subscription_name; kwargs...) -> ServiceBusReceiver

Same keyword arguments as [`get_queue_receiver`](@ref).
"""
function get_subscription_receiver(c::ServiceBusClient, topic_name::AbstractString, subscription_name::AbstractString;
    kwargs...)
    (isempty(subscription_name) || occursin(r"[\s/?#$]", subscription_name)) &&
        throw(ArgumentError("invalid subscription name $(repr(subscription_name))"))
    return _new_receiver(c, string(_check_entity(c, topic_name), "/Subscriptions/", subscription_name); kwargs...)
end

function get_queue_receiver(fn::Function, c::ServiceBusClient, args...; kwargs...)
    r = get_queue_receiver(c, args...; kwargs...)
    try
        return fn(r)
    finally
        close(r)
    end
end
function get_subscription_receiver(fn::Function, c::ServiceBusClient, args...; kwargs...)
    r = get_subscription_receiver(c, args...; kwargs...)
    try
        return fn(r)
    finally
        close(r)
    end
end

_is_http(r::ServiceBusHandler) = r.client.config.transport_type == TransportType.Http

function _receiver_link(r::ServiceBusReceiver; attach_timeout=nothing)
    conn = r.client.conn::ClientConnection
    AMQP.authorize!(conn, _audience(r))
    lock(r.lock)
    try
        _check_open(r)
        _, session, gen = AMQP.acquire!(conn)
        if r.link !== nothing && r.link[2] == gen && AMQP.is_attached(r.link[1])
            return r.link[1]
        end
        if r.session !== nothing && r.session.established
            err = r.link === nothing ? nothing : AMQP.link_error(r.link[1])
            throw(SessionLockLostError("The session lock has been lost; create a new session receiver."; error=err))
        end
        source = if r.session === nothing
            AMQP.Source(address=r.entity_path)
        else
            AMQP.Source(address=r.entity_path, filter=Dict{Any,Any}(SESSION_FILTER => r.session.session_id))
        end
        peek_lock = r.receive_mode == ServiceBusReceiveMode.PEEK_LOCK
        timeout = something(attach_timeout, r.client.config.auth_timeout)
        props = r.session === nothing ? nothing :
                Dict{Symbol,Any}(Symbol("com.microsoft:timeout") => UInt32(round(Int, min(timeout, 3600) * 1000)))
        link = try
            AMQP.attach_receiver(session, "SBReceiver-$(uuid4())", source;
                prefetch=r.prefetch_count,
                snd_settle_mode=peek_lock ? AMQP.SND_UNSETTLED : AMQP.SND_SETTLED,
                rcv_settle_mode=peek_lock ? AMQP.RCV_SECOND : AMQP.RCV_FIRST,
                properties=props, timeout=timeout)
        catch err
            if r.session !== nothing && err isa AMQPException && err.condition == Symbol("com.microsoft:timeout")
                # waiting longer (retrying) is the caller's decision
                throw(OperationTimeoutError(r.session.session_id === nothing ?
                    "No session became available within the timeout." :
                    "Session $(repr(r.session.session_id)) could not be locked within the timeout."; error=err,
                    condition=err.condition, retriable=false))
            end
            rethrow()
        end
        if r.session !== nothing
            src = AMQP.remote_source(link)
            if src isa AMQP.Source && src.filter !== nothing
                sid = get(src.filter, SESSION_FILTER, nothing)
                sid === nothing || (r.session.session_id = string(sid))
            end
            rp = AMQP.remote_properties(link)
            rp === nothing || (r.session.locked_until_utc = _to_datetime(get(rp, SESSION_LOCKED_UNTIL, nothing)))
            r.session.established = true
            r.auto_lock_renewer === nothing || register!(r.auto_lock_renewer, r, r.session)
        end
        r.link = (link, gen)
        return link
    finally
        unlock(r.lock)
    end
end

function _wrap(r::ServiceBusReceiver, d::AMQP.IncomingDelivery, gen::Int)
    m = ServiceBusReceivedMessage(AMQP.decode_delivery(d); delivery=d, receiver=r, receive_mode=r.receive_mode,
        link_generation=gen)
    if r.receive_mode == ServiceBusReceiveMode.RECEIVE_AND_DELETE
        m.settled = true
    end
    if m.sequence_number !== nothing
        r.last_peeked = max(r.last_peeked, m.sequence_number)
    end
    r.auto_lock_renewer === nothing || r.receive_mode != ServiceBusReceiveMode.PEEK_LOCK ||
        r.session !== nothing || register!(r.auto_lock_renewer, r, m)
    return m
end

"""
    receive_messages(receiver; max_message_count=1, max_wait_time=nothing) -> Vector{ServiceBusReceivedMessage}

Receive up to `max_message_count` messages. Waits up to `max_wait_time`
seconds (default: the receiver's `max_wait_time`; wait indefinitely if both
are `nothing`) for the first message, then briefly for more.
"""
function receive_messages(r::ServiceBusReceiver; max_message_count::Integer=1,
    max_wait_time::Union{Nothing,Real}=nothing)
    max_message_count < 1 && throw(ArgumentError("max_message_count must be at least 1"))
    wait = max_wait_time === nothing ? r.max_wait_time : Float64(max_wait_time)
    wait !== nothing && wait <= 0 && throw(ArgumentError("max_wait_time must be positive"))
    _is_http(r) && return _http_receive(r, Int(max_message_count), wait)
    return _run(r, "receive_messages") do
        link = _receiver_link(r; attach_timeout=r.session !== nothing && wait !== nothing ? wait : nothing)
        gen = r.link[2]
        n = Int(max_message_count)
        call_deadline = wait === nothing ? nothing : time() + wait
        got = AMQP.take_queued!(link, n)
        if isempty(got)
            # while an earlier drain is unanswered the broker still holds credit;
            # new credit would cancel the drain, so just wait for deliveries
            r.prefetch_count == 0 && !AMQP.drain_pending!(link) && AMQP.flow!(link, n - AMQP.queued_count(link))
            first = AMQP.receive_delivery(link; timeout=something(wait, 0.0))
            if first !== nothing
                push!(got, first)
                deadline = time() + FURTHER_RECEIVE_WAIT
                while length(got) < n
                    append!(got, AMQP.take_queued!(link, n - length(got)))
                    remaining = deadline - time()
                    (length(got) >= n || remaining <= 0) && break
                    d = AMQP.receive_delivery(link; timeout=remaining)
                    d === nothing && break
                    push!(got, d)
                end
            end
        end
        if r.prefetch_count == 0 && (AMQP.link_credit(link) > 0 || AMQP.drain_pending!(link))
            # Take back unused credit so no further messages are sent to us. In
            # RECEIVE_AND_DELETE mode a message the broker sends after we return is
            # already deleted and would be lost if the receiver is closed, so wait
            # for the broker to confirm. In PEEK_LOCK mode late messages are only
            # locked: they wait here for the next call or are released on close,
            # so the drain may not hold the caller up beyond its max_wait_time.
            timeout = if r.receive_mode == ServiceBusReceiveMode.RECEIVE_AND_DELETE
                RECEIVE_AND_DELETE_DRAIN_TIMEOUT
            else
                clamp(call_deadline === nothing ? PEEK_LOCK_DRAIN_TIMEOUT : call_deadline - time(),
                    FURTHER_RECEIVE_WAIT, PEEK_LOCK_DRAIN_TIMEOUT)
            end
            AMQP.drain!(link; timeout)
            length(got) < n && append!(got, AMQP.take_queued!(link, n - length(got)))
        end
        [_wrap(r, d, gen) for d in got]
    end
end

function Base.iterate(r::ServiceBusReceiver, state=nothing)
    while true
        r.closed && return nothing
        msgs = try
            receive_messages(r; max_message_count=1)
        catch err
            (r.closed || err isa ServiceBusClientClosedError) && return nothing
            rethrow()
        end
        isempty(msgs) || return msgs[1], nothing
        r.max_wait_time === nothing || return nothing
    end
end
Base.IteratorSize(::Type{ServiceBusReceiver}) = Base.SizeUnknown()
Base.eltype(::Type{ServiceBusReceiver}) = ServiceBusReceivedMessage

# ---------------------------------------------------------------------------
# Settlement
# ---------------------------------------------------------------------------

function _check_settleable(r::ServiceBusReceiver, m::ServiceBusReceivedMessage)
    _check_open(r)
    getfield(m, :receiver) === r || throw(ArgumentError("the message was not received by this receiver"))
    r.receive_mode == ServiceBusReceiveMode.RECEIVE_AND_DELETE &&
        throw(ArgumentError("messages received in RECEIVE_AND_DELETE mode cannot be settled"))
    getfield(m, :lock_token) === nothing && throw(ArgumentError("the message has no lock token (peeked messages cannot be settled)"))
    getfield(m, :settled) && throw(MessageAlreadySettled("The message has already been settled."))
end

function _link_outcome(op::Symbol; reason=nothing, description=nothing)
    op === :complete && return AMQP.Accepted()
    op === :abandon && return AMQP.Modified(delivery_failed=true, undeliverable_here=false)
    op === :defer && return AMQP.Modified(delivery_failed=true, undeliverable_here=true)
    info = Dict{Any,Any}()
    reason === nothing || (info[PROP_DEAD_LETTER_REASON] = String(reason))
    description === nothing || (info[PROP_DEAD_LETTER_ERROR_DESCRIPTION] = String(description))
    return AMQP.Rejected(error=AMQP.AMQPError(condition=DEADLETTER_CONDITION, description=description === nothing ? nothing : String(description),
        info=isempty(info) ? nothing : info))
end

const _MGMT_DISPOSITION = Dict(:complete => "completed", :abandon => "abandoned", :defer => "defered",
    :dead_letter => "suspended")

function _settle(r::ServiceBusReceiver, m::ServiceBusReceivedMessage, op::Symbol; reason=nothing, description=nothing,
    timeout::Real=60.0)
    _check_settleable(r, m)
    if _is_http(r)
        _http_settle(r, m, op)
        m.settled = true
        return nothing
    end
    d = getfield(m, :delivery)
    current = @lock r.lock r.link
    via_link = d !== nothing && current !== nothing && d.link === current[1] && AMQP.is_attached(current[1])
    if via_link
        outcome = _link_outcome(op; reason, description)
        state = try
            AMQP.settle!(d, outcome; wait=true, timeout=timeout)
        catch err
            throw(map_servicebus_error(err))
        end
        if state isa AMQP.Rejected && state.error !== nothing && state.error.condition != DEADLETTER_CONDITION
            throw(map_servicebus_error(AMQPException(:delivery, state.error, "settlement failed")))
        end
    else
        if r.session === nothing && d !== nothing
            # the link that delivered the message is gone; the lock may still be valid
            @debug "settling message through the management link"
        end
        body = Dict{String,Any}("disposition-status" => _MGMT_DISPOSITION[op],
            "lock-tokens" => AMQPArray(UUID[getfield(m, :lock_token)]))
        if op === :dead_letter
            reason === nothing || (body["deadletter-reason"] = String(reason))
            description === nothing || (body["deadletter-description"] = String(description))
        end
        r.session === nothing || (body["session-id"] = r.session.session_id)
        _run(r, "settle_message"; retry=false) do
            _mgmt_request(r, "com.microsoft:update-disposition", body; timeout=timeout)
        end
    end
    m.settled = true
    return nothing
end

"""
    complete_message(receiver, message)

Complete the message: it is removed from the queue.
"""
complete_message(r::ServiceBusReceiver, m::ServiceBusReceivedMessage) = _settle(r, m, :complete)

"""
    abandon_message(receiver, message)

Abandon the message: release the lock so it can be received again (its
delivery count increases).
"""
abandon_message(r::ServiceBusReceiver, m::ServiceBusReceivedMessage) = _settle(r, m, :abandon)

"""
    defer_message(receiver, message)

Defer the message; it can later be received only by sequence number with
[`receive_deferred_messages`](@ref).
"""
defer_message(r::ServiceBusReceiver, m::ServiceBusReceivedMessage) = _settle(r, m, :defer)

"""
    dead_letter_message(receiver, message; reason=nothing, error_description=nothing)

Move the message to the dead-letter sub-queue.
"""
dead_letter_message(r::ServiceBusReceiver, m::ServiceBusReceivedMessage; reason=nothing, error_description=nothing) =
    _settle(r, m, :dead_letter; reason=reason, description=error_description)

"""
    renew_message_lock(receiver, message; timeout=nothing) -> DateTime

Extend the message lock; returns (and stores in `message.locked_until_utc`)
the new expiry.
"""
function renew_message_lock(r::ServiceBusReceiver, m::ServiceBusReceivedMessage; timeout::Union{Nothing,Real}=nothing)
    _check_settleable(r, m)
    r.session === nothing || throw(ArgumentError("renew the session lock instead of message locks for session receivers"))
    if _is_http(r)
        return _http_renew(r, m)
    end
    body = _run(r, "renew_message_lock"; timeout=timeout) do
        link = _receiver_link(r)
        _mgmt_request(r, "com.microsoft:renew-lock",
            Dict{String,Any}("lock-tokens" => AMQPArray(UUID[getfield(m, :lock_token)]));
            timeout=something(timeout, 60.0), associated_link_name=link.name)
    end
    exps = body isa AbstractDict ? get(body, "expirations", nothing) : nothing
    (exps === nothing || isempty(exps)) && throw(ServiceBusServiceError("unexpected renew-lock response"))
    m.locked_until_utc = _to_datetime(exps[1])
    return m.locked_until_utc
end

# ---------------------------------------------------------------------------
# Peek and deferred messages
# ---------------------------------------------------------------------------

"""
    peek_messages(receiver; max_message_count=1, sequence_number=nothing, timeout=nothing) -> Vector{ServiceBusReceivedMessage}

Browse messages without locking or removing them, starting at
`sequence_number` (default: after the last peeked/received message).
"""
function peek_messages(r::ServiceBusReceiver; max_message_count::Integer=1, sequence_number=nothing,
    timeout::Union{Nothing,Real}=nothing)
    max_message_count < 1 && throw(ArgumentError("max_message_count must be at least 1"))
    _is_http(r) && throw(ArgumentError("peek_messages requires an AMQP transport"))
    from = sequence_number === nothing ? r.last_peeked + 1 : Int64(sequence_number)
    body = Dict{String,Any}("from-sequence-number" => Int64(from), "message-count" => Int32(max_message_count))
    if r.session !== nothing
        _receiver_link(r)
        body["session-id"] = r.session.session_id
    end
    resp = _run(r, "peek_messages"; timeout=timeout) do
        _mgmt_request(r, "com.microsoft:peek-message", body; timeout=something(timeout, 60.0))
    end
    msgs = ServiceBusReceivedMessage[]
    resp isa AbstractDict || return msgs
    for entry in get(resp, "messages", Any[])
        raw = AMQP.decode_message(entry["message"])
        m = ServiceBusReceivedMessage(raw; receiver=r, receive_mode=nothing)
        m.settled = true
        m.lock_token = nothing
        push!(msgs, m)
        seq = m.sequence_number
        seq === nothing || (r.last_peeked = max(r.last_peeked, seq))
    end
    return msgs
end

"""
    receive_deferred_messages(receiver, sequence_numbers; timeout=nothing) -> Vector{ServiceBusReceivedMessage}

Receive previously deferred messages by sequence number.
"""
function receive_deferred_messages(r::ServiceBusReceiver, sequence_numbers; timeout::Union{Nothing,Real}=nothing)
    nums = sequence_numbers isa Integer ? Int64[sequence_numbers] : Int64[Int64(x) for x in sequence_numbers]
    isempty(nums) && return ServiceBusReceivedMessage[]
    _is_http(r) && throw(ArgumentError("receive_deferred_messages requires an AMQP transport"))
    body = Dict{String,Any}("sequence-numbers" => AMQPArray(nums),
        "receiver-settle-mode" => UInt32(r.receive_mode == ServiceBusReceiveMode.PEEK_LOCK ? 1 : 0))
    resp = _run(r, "receive_deferred_messages"; timeout=timeout) do
        link = _receiver_link(r)
        r.session === nothing || (body["session-id"] = r.session.session_id)
        _mgmt_request(r, "com.microsoft:receive-by-sequence-number", body; timeout=something(timeout, 60.0),
            associated_link_name=link.name)
    end
    msgs = ServiceBusReceivedMessage[]
    resp isa AbstractDict || return msgs
    for entry in get(resp, "messages", Any[])
        raw = AMQP.decode_message(entry["message"])
        tok = get(entry, "lock-token", nothing)
        m = ServiceBusReceivedMessage(raw; receiver=r, receive_mode=r.receive_mode,
            lock_token=tok isa UUID ? tok : nothing)
        r.receive_mode == ServiceBusReceiveMode.RECEIVE_AND_DELETE && (m.settled = true)
        push!(msgs, m)
    end
    return msgs
end

# ---------------------------------------------------------------------------
# Sessions
# ---------------------------------------------------------------------------

function _session_receiver(s::ServiceBusSession)
    r = s.receiver::ServiceBusReceiver
    _receiver_link(r)          # establishes the session lock if not yet done
    return r
end

"""
    get_state(session::ServiceBusSession; timeout=nothing) -> Union{Vector{UInt8},Nothing}
"""
function get_state(s::ServiceBusSession; timeout::Union{Nothing,Real}=nothing)
    r = _session_receiver(s)
    resp = _run(r, "get_session_state"; timeout=timeout) do
        _mgmt_request(r, "com.microsoft:get-session-state", Dict{String,Any}("session-id" => s.session_id);
            timeout=something(timeout, 60.0))
    end
    resp isa AbstractDict || return nothing
    st = get(resp, "session-state", nothing)
    return st === nothing ? nothing : Vector{UInt8}(st)
end

"""
    set_state(session::ServiceBusSession, state; timeout=nothing)

`state` is a `String`, byte vector or `nothing` (clears the state).
"""
function set_state(s::ServiceBusSession, state; timeout::Union{Nothing,Real}=nothing)
    r = _session_receiver(s)
    st = state === nothing ? nothing : _sb_bytes(state)
    _run(r, "set_session_state"; timeout=timeout) do
        _mgmt_request(r, "com.microsoft:set-session-state",
            Dict{String,Any}("session-id" => s.session_id, "session-state" => st); timeout=something(timeout, 60.0))
    end
    return nothing
end

"""
    renew_lock(session::ServiceBusSession; timeout=nothing) -> DateTime
"""
function renew_lock(s::ServiceBusSession; timeout::Union{Nothing,Real}=nothing)
    r = _session_receiver(s)
    resp = _run(r, "renew_session_lock"; timeout=timeout) do
        _mgmt_request(r, "com.microsoft:renew-session-lock", Dict{String,Any}("session-id" => s.session_id);
            timeout=something(timeout, 60.0))
    end
    exp = resp isa AbstractDict ? get(resp, "expiration", nothing) : nothing
    exp === nothing && throw(ServiceBusServiceError("unexpected renew-session-lock response"))
    s.locked_until_utc = _to_datetime(exp)
    return s.locked_until_utc
end

"""
    close(receiver)
"""
function Base.close(r::ServiceBusReceiver)
    link = @lock r.lock begin
        r.closed && return nothing
        r.closed = true
        l = r.link
        r.link = nothing
        l
    end
    if link !== nothing
        l = link[1]
        # messages delivered to this link that the application never received
        left = try AMQP.take_queued!(l) catch; AMQP.IncomingDelivery[] end
        if r.receive_mode == ServiceBusReceiveMode.PEEK_LOCK
            # release them now instead of leaving them locked until the lock expires
            for d in left
                d.settled || try AMQP.settle!(d, AMQP.Released()) catch end
            end
        elseif !isempty(left)
            @warn "Closing a RECEIVE_AND_DELETE receiver discarded $(length(left)) message(s) that had been \
                   delivered (e.g. prefetched) but not received" entity = r.entity_path
        end
        try AMQP.detach(l; timeout=5.0) catch end
    end
    m = r.mgmt[]
    m === nothing || try close(m[1]) catch end
    return nothing
end
