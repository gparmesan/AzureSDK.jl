# ServiceBusSender.

"""
    ServiceBusSender

Sends messages to a queue or topic. Create with [`get_queue_sender`](@ref) or
[`get_topic_sender`](@ref).
"""
mutable struct ServiceBusSender <: ServiceBusHandler
    client::ServiceBusClient
    entity_path::String
    lock::ReentrantLock
    link::Union{Nothing,Tuple{AMQP.SenderLink,Int}}
    mgmt::Ref{Any}
    closed::Bool
end

Base.show(io::IO, s::ServiceBusSender) = print(io, "ServiceBusSender(", s.entity_path, s.closed ? ", closed" : "", ")")

"""
    get_queue_sender(client, queue_name) -> ServiceBusSender
"""
get_queue_sender(c::ServiceBusClient, queue_name::AbstractString) =
    _register!(c, ServiceBusSender(c, _check_entity(c, queue_name), ReentrantLock(), nothing, Ref{Any}(nothing), false))

"""
    get_topic_sender(client, topic_name) -> ServiceBusSender
"""
get_topic_sender(c::ServiceBusClient, topic_name::AbstractString) =
    _register!(c, ServiceBusSender(c, _check_entity(c, topic_name), ReentrantLock(), nothing, Ref{Any}(nothing), false))

for f in (:get_queue_sender, :get_topic_sender)
    @eval function $f(fn::Function, c::ServiceBusClient, name::AbstractString)
        h = $f(c, name)
        try
            return fn(h)
        finally
            close(h)
        end
    end
end

function _sender_link(s::ServiceBusSender)
    conn = s.client.conn::ClientConnection
    AMQP.authorize!(conn, _audience(s))
    lock(s.lock)
    try
        _check_open(s)
        _, session, gen = AMQP.acquire!(conn)
        if s.link !== nothing && s.link[2] == gen && AMQP.is_attached(s.link[1])
            return s.link[1]
        end
        link = AMQP.attach_sender(session, "SBSender-$(uuid4())", s.entity_path;
            snd_settle_mode=AMQP.SND_UNSETTLED, rcv_settle_mode=AMQP.RCV_FIRST, timeout=s.client.config.auth_timeout)
        s.link = (link, gen)
        return link
    finally
        unlock(s.lock)
    end
end

function _check_outcome(outcome)
    (outcome isa AMQP.Accepted || outcome === nothing) && return nothing
    outcome isa AMQP.Rejected && throw(AMQPException(:delivery, outcome.error, "the service rejected the message"))
    throw(AMQPException(:delivery, Symbol("com.microsoft:server-busy"), "the service did not accept the message ($(typeof(outcome)))"))
end

"""
    create_message_batch(sender; max_size_in_bytes=nothing) -> ServiceBusMessageBatch
"""
function create_message_batch(s::ServiceBusSender; max_size_in_bytes::Union{Nothing,Integer}=nothing)
    _check_open(s)
    limit = if s.client.config.transport_type == TransportType.Http
        256 * 1024
    else
        link = _run(() -> _sender_link(s), s, "create_message_batch")
        link.max_message_size > 0 ? Int(min(link.max_message_size, typemax(Int))) : 256 * 1024
    end
    if max_size_in_bytes !== nothing
        max_size_in_bytes > limit && throw(ArgumentError(
            "max_size_in_bytes $(max_size_in_bytes) is too large; the maximum allowed is $(limit) bytes"))
        limit = Int(max_size_in_bytes)
    end
    return ServiceBusMessageBatch(limit)
end

"""
    send_messages(sender, message::ServiceBusMessage; timeout=nothing)
    send_messages(sender, messages::AbstractVector{ServiceBusMessage}; timeout=nothing)
    send_messages(sender, batch::ServiceBusMessageBatch; timeout=nothing)

Send one message, a list of messages (packed into one batch; throws
`MessageSizeExceededError` if they do not fit) or a batch.
"""
function send_messages(s::ServiceBusSender, batch::ServiceBusMessageBatch; timeout::Union{Nothing,Real}=nothing)
    isempty(batch) && return nothing
    if s.client.config.transport_type == TransportType.Http
        return _http_send(s, batch; timeout=timeout)
    end
    payload = length(batch) == 1 ? batch.messages[1] : batch_payload(batch)
    fmt = length(batch) == 1 ? UInt32(0) : UInt32(AMQP.BATCH_MESSAGE_FORMAT)
    _run(s, "send_messages"; timeout=timeout) do
        link = _sender_link(s)
        _check_outcome(AMQP.send_transfer(link, payload; message_format=fmt, timeout=something(timeout, 60.0)))
    end
    return nothing
end

function send_messages(s::ServiceBusSender, messages::AbstractVector{<:Union{ServiceBusMessage,AMQPMessage}}; timeout=nothing)
    isempty(messages) && return nothing
    batch = create_message_batch(s)
    for m in messages
        try_add_message!(batch, m) || throw(MessageSizeExceededError(
            "The messages do not fit into a single batch (max $(batch.max_size_in_bytes) bytes); use create_message_batch and send several batches."))
    end
    return send_messages(s, batch; timeout=timeout)
end

send_messages(s::ServiceBusSender, m::Union{ServiceBusMessage,AMQPMessage}; timeout=nothing) =
    send_messages(s, [m]; timeout=timeout)

"""
    schedule_messages(sender, messages, schedule_time_utc::DateTime; timeout=nothing) -> Vector{Int64}

Schedule messages for delivery at `schedule_time_utc` (UTC). Returns their
sequence numbers (use them with [`cancel_scheduled_messages`](@ref)).
"""
function schedule_messages(s::ServiceBusSender, messages, schedule_time_utc::Dates.DateTime;
    timeout::Union{Nothing,Real}=nothing)
    msgs = messages isa ServiceBusMessage ? [messages] : collect(ServiceBusMessage, messages)
    isempty(msgs) && return Int64[]
    s.client.config.transport_type == TransportType.Http &&
        throw(ArgumentError("schedule_messages requires an AMQP transport"))
    entries = map(msgs) do m
        mm = deepcopy(m)
        mm.scheduled_enqueue_time_utc = schedule_time_utc
        mm.message_id === nothing && (mm.message_id = string(uuid4()))
        e = Dict{String,Any}("message-id" => mm.message_id, "message" => AMQP.encode_message(to_amqp_message(mm)))
        mm.session_id === nothing || (e["session-id"] = mm.session_id)
        mm.partition_key === nothing || (e["partition-key"] = mm.partition_key)
        e
    end
    body = _run(s, "schedule_messages"; timeout=timeout) do
        link = _sender_link(s)
        _mgmt_request(s, "com.microsoft:schedule-message", Dict{String,Any}("messages" => entries);
            timeout=something(timeout, 60.0), associated_link_name=link.name)
    end
    body isa AbstractDict || return Int64[]
    return Int64[Int64(x) for x in get(body, "sequence-numbers", Int64[])]
end

"""
    cancel_scheduled_messages(sender, sequence_numbers; timeout=nothing)
"""
function cancel_scheduled_messages(s::ServiceBusSender, sequence_numbers; timeout::Union{Nothing,Real}=nothing)
    nums = sequence_numbers isa Integer ? Int64[sequence_numbers] : Int64[Int64(x) for x in sequence_numbers]
    isempty(nums) && return nothing
    s.client.config.transport_type == TransportType.Http &&
        throw(ArgumentError("cancel_scheduled_messages requires an AMQP transport"))
    _run(s, "cancel_scheduled_messages"; timeout=timeout) do
        link = _sender_link(s)
        _mgmt_request(s, "com.microsoft:cancel-scheduled-message",
            Dict{String,Any}("sequence-numbers" => AMQPArray(nums)); timeout=something(timeout, 60.0),
            associated_link_name=link.name)
    end
    return nothing
end

"""
    close(sender)
"""
function Base.close(s::ServiceBusSender)
    link = @lock s.lock begin
        s.closed && return nothing
        s.closed = true
        l = s.link
        s.link = nothing
        l
    end
    link === nothing || try AMQP.detach(link[1]; timeout=5.0) catch end
    m = s.mgmt[]
    m === nothing || try close(m[1]) catch end
    return nothing
end
