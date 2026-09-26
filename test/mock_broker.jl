# A small in-process AMQP 1.0 broker implementing the parts of the Azure
# Service Bus / Event Hubs protocol the clients use: SASL, CBS put-token,
# queues with peek-lock / receive-and-delete, settlement outcomes, drain,
# sessions, dead-lettering, deferral, scheduling and the $management
# operations. Used by the test suite; it is not a production broker.

module MockBroker

using AzureSDK
const A = AzureSDK.AMQP
using Dates
using UUIDs
import HTTP
import Reseau

const NOW() = Dates.now(Dates.UTC)

mutable struct Stored
    seq::Int64
    raw::A.AMQPMessage
    enqueued::DateTime
    delivery_count::Int
    lock_token::Union{Nothing,UUID}
    locked_until::DateTime
    state::Symbol               # :active, :deferred, :scheduled
    scheduled::Union{Nothing,DateTime}
    session_id::Union{Nothing,String}
    locked_by::Any              # link holding the lock
end

mutable struct Entity
    name::String
    messages::Vector{Stored}
    next_seq::Int64
    session_states::Dict{String,Vector{UInt8}}
    session_locks::Dict{String,Any}
    session_locked_until::Dict{String,DateTime}
    requires_session::Bool
end
Entity(name; requires_session=false) = Entity(name, Stored[], 1, Dict{String,Vector{UInt8}}(), Dict{String,Any}(),
    Dict{String,DateTime}(), requires_session)

mutable struct MLink
    conn::Any
    session::Any
    name::String
    handle::UInt32            # our handle
    remote_handle::UInt32
    role::Bool                # our role: false = sender (client receives), true = receiver
    address::String
    kind::Symbol              # :entity, :cbs, :mgmt
    entity::Union{Nothing,Entity}
    subqueue::Bool
    settled_mode::Bool        # sender settle mode settled (receive-and-delete)
    rcv_second::Bool
    credit::UInt32
    delivery_count::UInt32
    drain::Bool
    session_id::Union{Nothing,String}
    deliveries::Dict{UInt32,Stored}   # delivery id => message (for our sender links)
    partial::Vector{UInt8}
    partial_id::Union{Nothing,UInt32}
    partial_settled::Bool
    partial_format::UInt32
    target::Any
    source::Any
end

mutable struct MSession
    conn::Any
    channel::UInt16           # our channel
    remote_channel::UInt16
    next_outgoing_id::UInt32
    next_incoming_id::UInt32
    incoming_since_flow::Int
    next_delivery_id::UInt32
    links::Dict{UInt32,MLink} # by remote handle
    next_handle::UInt32
end

mutable struct MConn
    broker::Any
    transport::A.AMQPTransport
    reader::A.TransportReader
    wlock::ReentrantLock
    sessions::Dict{UInt16,MSession}   # by remote channel
    max_frame_size::UInt32            # client's
    closed::Bool
    heartbeats::Int
end

Base.@kwdef mutable struct Broker
    lock::ReentrantLock = ReentrantLock()
    entities::Dict{String,Entity} = Dict{String,Entity}()
    lock_duration::Float64 = 30.0
    listener::Any = nothing
    tls_listener::Any = nothing
    ws_server::Any = nothing
    wss_server::Any = nothing
    idle_timeout_ms::Int = 60_000
    port::Int = 0
    tls_port::Int = 0
    ws_port::Int = 0
    wss_port::Int = 0
    conns::Vector{MConn} = MConn[]
    tasks::Vector{Task} = Task[]
    max_frame_size::UInt32 = UInt32(64 * 1024)
    split_outgoing::Int = 0           # >0: split outgoing transfers into chunks of this many bytes
    max_message_size::UInt64 = UInt64(256 * 1024)
    put_tokens::Vector{Tuple{String,String}} = Tuple{String,String}[]   # (audience, type)
    reject_tokens::Bool = false
    received_links::Vector{String} = String[]
    stopped::Bool = false
end

entity!(b::Broker, name::AbstractString; requires_session=false) =
    @lock b.lock get!(() -> Entity(String(name); requires_session), b.entities, String(name))

# ---------------------------------------------------------------------------
# I/O helpers
# ---------------------------------------------------------------------------

function send!(c::MConn, perf; channel::UInt16=UInt16(0), payload=nothing, frame_type=A.FRAME_TYPE_AMQP)
    c.closed && return
    bytes = A.encode_frame(perf; channel=channel, payload=payload, frame_type=frame_type)
    lock(c.wlock)
    try
        A.write_all(c.transport, bytes)
    catch
        c.closed = true
    finally
        unlock(c.wlock)
    end
end

function read_frame(c::MConn)
    hdr = A.read_exact(c.reader, 8)
    size = A.frame_size(hdr)
    body = A.read_exact(c.reader, Int(size) - 8)
    return A.decode_frame(hdr, body)
end

# ---------------------------------------------------------------------------
# Connection handling
# ---------------------------------------------------------------------------

function serve_connection(b::Broker, t::A.AMQPTransport)
    c = MConn(b, t, A.TransportReader(t), ReentrantLock(), Dict{UInt16,MSession}(), UInt32(1024 * 1024), false, 0)
    @lock b.lock push!(b.conns, c)
    try
        hdr = A.read_exact(c.reader, 8)
        if hdr == A.SASL_HEADER
            A.write_all(t, copy(A.SASL_HEADER))
            send!(c, A.SaslMechanisms(sasl_server_mechanisms=[:ANONYMOUS, :PLAIN, :MSSBCBS]); frame_type=A.FRAME_TYPE_SASL)
            f = read_frame(c)
            f.body isa A.SaslInit || error("expected sasl-init")
            ok = f.body.mechanism in (:ANONYMOUS, :PLAIN, :MSSBCBS)
            send!(c, A.SaslOutcome(code=ok ? 0x00 : 0x01); frame_type=A.FRAME_TYPE_SASL)
            ok || return
            hdr = A.read_exact(c.reader, 8)
        end
        hdr == A.AMQP_HEADER || return
        A.write_all(t, copy(A.AMQP_HEADER))
        f = read_frame(c)
        f.body isa A.Open || return
        c.max_frame_size = something(f.body.max_frame_size, UInt32(1024 * 1024))
        send!(c, A.Open(container_id="mock-broker", max_frame_size=b.max_frame_size, channel_max=UInt16(255),
            idle_time_out=UInt32(b.idle_timeout_ms)))
        while !c.closed
            f = read_frame(c)
            if f.body === nothing
                c.heartbeats += 1
                continue
            end
            @lock b.lock handle!(b, c, f)
        end
    catch err
        if !(err isa EOFError || err isa Base.IOError || c.closed || b.stopped)
            @debug "mock broker connection error" exception = (err, catch_backtrace())
        end
    finally
        c.closed = true
        @lock b.lock begin
            release_connection!(b, c)
            filter!(x -> x !== c, b.conns)
        end
        close(t)
    end
end

function release_connection!(b::Broker, c::MConn)
    for s in values(c.sessions), l in values(s.links)
        release_link!(b, l)
    end
end

# Like Service Bus, message locks survive the link that acquired them (they
# expire on their own); session locks are released when the link closes.
function release_link!(b::Broker, l::MLink)
    l.entity === nothing && return
    if l.session_id !== nothing && get(l.entity.session_locks, l.session_id, nothing) === l
        delete!(l.entity.session_locks, l.session_id)
    end
    empty!(l.deliveries)
end

function unlock_msg!(m::Stored)
    m.lock_token = nothing
    m.locked_by = nothing
end

function handle!(b::Broker, c::MConn, f::A.Frame)
    p = f.body
    if p isa A.Begin
        s = MSession(c, f.channel, f.channel, 1, p.next_outgoing_id, 0, 0, Dict{UInt32,MLink}(), 0)
        c.sessions[f.channel] = s
        send!(c, A.Begin(remote_channel=f.channel, next_outgoing_id=1, incoming_window=10_000, outgoing_window=10_000,
            handle_max=UInt32(1023)); channel=s.channel)
    elseif p isa A.End
        s = pop!(c.sessions, f.channel, nothing)
        if s !== nothing
            for l in values(s.links)
                release_link!(b, l)
            end
            send!(c, A.End(); channel=s.channel)
        end
    elseif p isa A.Close
        send!(c, A.Close())
        c.closed = true
        close(c.transport)
    elseif p isa A.Attach
        on_attach!(b, c, c.sessions[f.channel], p)
    elseif p isa A.Detach
        s = c.sessions[f.channel]
        l = pop!(s.links, p.handle, nothing)
        if l !== nothing
            release_link!(b, l)
            send!(c, A.Detach(handle=l.handle, closed=true); channel=s.channel)
        end
    elseif p isa A.Flow
        s = c.sessions[f.channel]
        if p.handle !== nothing
            l = get(s.links, p.handle, nothing)
            if l !== nothing && !l.role
                dc = something(p.delivery_count, UInt32(0))
                l.credit = (dc + something(p.link_credit, UInt32(0)) - l.delivery_count) % UInt32
                l.drain = p.drain === true
                pump!(b, l)
            end
        end
    elseif p isa A.Transfer
        s = c.sessions[f.channel]
        s.next_incoming_id += 1
        s.incoming_since_flow += 1
        if s.incoming_since_flow >= 5000
            s.incoming_since_flow = 0
            send!(c, A.Flow(next_incoming_id=s.next_incoming_id, incoming_window=10_000,
                next_outgoing_id=s.next_outgoing_id, outgoing_window=10_000); channel=s.channel)
        end
        l = s.links[p.handle]
        on_transfer!(b, c, s, l, p, f.payload)
    elseif p isa A.Disposition
        s = c.sessions[f.channel]
        on_disposition!(b, c, s, p)
    end
end

function split_address(addr::String)
    parts = split(addr, '/')
    if length(parts) >= 2 && parts[end] == "\$management"
        return :mgmt, join(parts[1:end-1], '/')
    end
    addr == "\$cbs" && return :cbs, ""
    return :entity, addr
end

function on_attach!(b::Broker, c::MConn, s::MSession, a::A.Attach)
    our_role = !a.role
    addr = our_role ? string(something(a.target isa A.Target ? a.target.address : nothing, "")) :
           string(something(a.source isa A.Source ? a.source.address : nothing, ""))
    kind, ename = split_address(addr)
    handle = s.next_handle
    s.next_handle += 1
    push!(b.received_links, addr)
    entity = nothing
    subqueue = false
    if kind === :entity || kind === :mgmt
        base = replace(ename, r"/\$DeadLetterQueue$" => "")
        subqueue = base != ename
        if !haskey(b.entities, base) || startswith(base, "missing")
            # Azure style: attach with a null terminus followed by detach with the error
            send!(c, A.Attach(name=a.name, handle=handle, role=our_role); channel=s.channel)
            send!(c, A.Detach(handle=handle, closed=true, error=A.AMQPError(condition=Symbol("amqp:not-found"),
                description="The messaging entity '$(base)' could not be found.")); channel=s.channel)
            return
        end
        entity = b.entities[base]
        subqueue && (entity = entity!(b, base * "/\$DeadLetterQueue"))
    end
    l = MLink(c, s, a.name, handle, a.handle, our_role, addr, kind, entity, subqueue,
        a.snd_settle_mode == A.SND_SETTLED, a.rcv_settle_mode == A.RCV_SECOND, 0, 0, false, nothing,
        Dict{UInt32,Stored}(), UInt8[], nothing, false, 0, a.target, a.source)
    source = a.source
    props = nothing
    if !our_role && kind === :entity && source isa A.Source && source.filter !== nothing &&
       haskey(source.filter, Symbol("com.microsoft:session-filter"))
        sid = source.filter[Symbol("com.microsoft:session-filter")]
        if sid === nothing
            sid = next_available_session(entity)
            if sid === nothing
                send!(c, A.Attach(name=a.name, handle=handle, role=our_role); channel=s.channel)
                send!(c, A.Detach(handle=handle, closed=true, error=A.AMQPError(condition=Symbol("com.microsoft:timeout"),
                    description="no session available")); channel=s.channel)
                return
            end
        elseif haskey(entity.session_locks, sid)
            send!(c, A.Attach(name=a.name, handle=handle, role=our_role); channel=s.channel)
            send!(c, A.Detach(handle=handle, closed=true, error=A.AMQPError(condition=Symbol("com.microsoft:session-cannot-be-locked"),
                description="session is locked by another receiver")); channel=s.channel)
            return
        end
        l.session_id = string(sid)
        entity.session_locks[l.session_id] = l
        until = NOW() + Dates.Millisecond(round(Int, b.lock_duration * 1000))
        entity.session_locked_until[l.session_id] = until
        source = A.with_fields(source; filter=Dict{Any,Any}(Symbol("com.microsoft:session-filter") => l.session_id))
        props = Dict{Symbol,Any}(Symbol("com.microsoft:locked-until-utc") => until)
    end
    s.links[a.handle] = l
    send!(c, A.Attach(name=a.name, handle=handle, role=our_role, snd_settle_mode=a.snd_settle_mode,
        rcv_settle_mode=a.rcv_settle_mode, source=source, target=a.target,
        initial_delivery_count=our_role ? nothing : UInt32(0), max_message_size=b.max_message_size,
        properties=props); channel=s.channel)
    if our_role
        # grant credit to the client's sender
        l.credit = 1000
        send!(c, A.Flow(next_incoming_id=s.next_incoming_id, incoming_window=10_000 - s.incoming_since_flow,
            next_outgoing_id=s.next_outgoing_id, outgoing_window=10_000, handle=l.handle,
            delivery_count=something(a.initial_delivery_count, UInt32(0)), link_credit=UInt32(1000)); channel=s.channel)
    end
end

function next_available_session(e::Entity)
    for m in e.messages
        if m.session_id !== nothing && m.state === :active && !haskey(e.session_locks, m.session_id)
            return m.session_id
        end
    end
    return nothing
end

function on_transfer!(b::Broker, c::MConn, s::MSession, l::MLink, t::A.Transfer, payload)
    if l.partial_id === nothing
        l.partial_id = t.delivery_id
        l.partial_settled = something(t.settled, false)
        l.partial_format = something(t.message_format, UInt32(0))
        empty!(l.partial)
    end
    payload === nothing || append!(l.partial, payload)
    t.more === true && return
    id = l.partial_id::UInt32
    data = copy(l.partial)
    settled = l.partial_settled
    fmt = l.partial_format
    l.partial_id = nothing
    # replenish credit
    l.credit -= 1
    l.delivery_count += 1
    if l.credit < 500
        l.credit = 1000
        send!(c, A.Flow(next_incoming_id=s.next_incoming_id, incoming_window=10_000 - s.incoming_since_flow,
            next_outgoing_id=s.next_outgoing_id, outgoing_window=10_000, handle=l.handle,
            delivery_count=l.delivery_count, link_credit=l.credit); channel=s.channel)
    end
    outcome = if l.kind === :cbs
        cbs_request!(b, c, s, l, A.decode_message(data))
        A.Accepted()
    elseif l.kind === :mgmt
        mgmt_request!(b, c, s, l, A.decode_message(data))
        A.Accepted()
    else
        store!(b, l.entity, data, fmt)
    end
    settled || send!(c, A.Disposition(role=true, first=id, settled=true, state=outcome); channel=s.channel)
end

function store!(b::Broker, e::Entity, data::Vector{UInt8}, fmt::UInt32)
    if startswith(e.name, "reject")
        return A.Rejected(error=A.AMQPError(condition=Symbol("com.microsoft:server-busy"), description="busy (test)"))
    end
    msgs = if fmt == A.BATCH_MESSAGE_FORMAT
        env = A.decode_message(data)
        [A.decode_message(part) for part in env.body]
    else
        [A.decode_message(data)]
    end
    for m in msgs
        enqueue!(b, e, m)
    end
    pump_all!(b)
    return A.Accepted()
end

function enqueue!(b::Broker, e::Entity, m::A.AMQPMessage; state=:active)
    ann = something(m.message_annotations, Dict{Any,Any}())
    sched = get(ann, Symbol("x-opt-scheduled-enqueue-time"), nothing)
    st = sched !== nothing && sched > NOW() ? :scheduled : state
    sid = m.properties === nothing ? nothing : m.properties.group_id
    msg = Stored(e.next_seq, m, NOW(), 0, nothing, NOW(), st, sched, sid, nothing)
    e.next_seq += 1
    push!(e.messages, msg)
    return msg
end

function promote_scheduled!(e::Entity)
    now = NOW()
    for m in e.messages
        if m.state === :scheduled && m.scheduled !== nothing && m.scheduled <= now
            m.state = :active
        end
    end
end

function available(e::Entity, l::MLink)
    promote_scheduled!(e)
    now = NOW()
    for m in e.messages
        m.state === :active || continue
        if m.lock_token !== nothing
            m.locked_until > now && continue
            unlock_msg!(m)
        end
        if l.session_id !== nothing
            m.session_id == l.session_id || continue
        elseif e.requires_session || m.session_id !== nothing && haskey(e.session_locks, m.session_id)
            continue
        end
        return m
    end
    return nothing
end

function delivered_message(b::Broker, m::Stored; lock_token=nothing)
    raw = deepcopy(m.raw)
    ann = something(raw.message_annotations, Dict{Any,Any}())
    ann[Symbol("x-opt-sequence-number")] = m.seq
    ann[Symbol("x-opt-enqueued-time")] = m.enqueued
    if lock_token !== nothing
        ann[Symbol("x-opt-locked-until")] = m.locked_until
        ann[Symbol("x-opt-lock-token")] = lock_token
    end
    m.state === :deferred && (ann[Symbol("x-opt-message-state")] = Int32(1))
    raw.message_annotations = ann
    h = something(raw.header, A.Header())
    raw.header = A.with_fields(h; delivery_count=UInt32(m.delivery_count))
    return raw
end

function bytes_le(u::UUID)
    be = reverse(reinterpret(UInt8, [u.value]))    # big-endian bytes
    return UInt8[be[4], be[3], be[2], be[1], be[6], be[5], be[8], be[7], be[9:16]...]
end

function pump_all!(b::Broker)
    for c in b.conns, s in values(c.sessions), l in values(s.links)
        (!l.role && l.kind === :entity) && pump!(b, l)
    end
end

function pump!(b::Broker, l::MLink)
    l.role && return
    c = l.conn
    s = l.session
    if l.kind === :entity
        while l.credit > 0
            m = available(l.entity, l)
            m === nothing && break
            if l.settled_mode
                filter!(x -> x !== m, l.entity.messages)
                raw = delivered_message(b, m)
                send_delivery!(b, c, s, l, A.encode_message(raw), randuuid_bytes(), true)
            else
                m.lock_token = uuid4()
                m.locked_until = NOW() + Dates.Millisecond(round(Int, b.lock_duration * 1000))
                m.locked_by = l
                raw = delivered_message(b, m; lock_token=m.lock_token)
                m.delivery_count += 1
                id = send_delivery!(b, c, s, l, A.encode_message(raw), bytes_le(m.lock_token), false)
                l.deliveries[id] = m
            end
        end
    end
    if l.drain
        l.delivery_count += l.credit
        l.credit = 0
        l.drain = false
        send!(c, A.Flow(next_incoming_id=s.next_incoming_id, incoming_window=10_000 - s.incoming_since_flow,
            next_outgoing_id=s.next_outgoing_id, outgoing_window=10_000, handle=l.handle,
            delivery_count=l.delivery_count, link_credit=UInt32(0), drain=true); channel=s.channel)
    end
end

randuuid_bytes() = bytes_le(uuid4())

function send_delivery!(b::Broker, c::MConn, s::MSession, l::MLink, payload::Vector{UInt8}, tag::Vector{UInt8}, settled::Bool)
    id = s.next_delivery_id
    s.next_delivery_id += 1
    l.credit -= 1
    l.delivery_count += 1
    chunk = b.split_outgoing > 0 ? b.split_outgoing : length(payload) + 1
    pos = 1
    first = true
    while true
        stop = min(pos + chunk - 1, length(payload))
        more = stop < length(payload)
        perf = first ? A.Transfer(handle=l.handle, delivery_id=id, delivery_tag=tag, message_format=UInt32(0),
                                  settled=settled, more=more) :
                       A.Transfer(handle=l.handle, more=more)
        send!(c, perf; channel=s.channel, payload=view(payload, pos:stop))
        s.next_outgoing_id += 1
        first = false
        pos = stop + 1
        more || break
    end
    return id
end

function on_disposition!(b::Broker, c::MConn, s::MSession, d::A.Disposition)
    d.role == true || return
    last = something(d.last, d.first)
    for id in d.first:last
        for l in values(s.links)
            m = get(l.deliveries, id, nothing)
            m === nothing && continue
            delete!(l.deliveries, id)
            state = apply_outcome!(b, l.entity, m, d.state, l)
            if d.settled !== true
                send!(c, A.Disposition(role=false, first=id, settled=true, state=state); channel=s.channel)
            end
        end
    end
    pump_all!(b)
end

lock_lost() = A.Rejected(error=A.AMQPError(condition=Symbol("com.microsoft:message-lock-lost"),
    description="The lock supplied is invalid. Either the lock expired, or the message has already been removed from the queue."))

function apply_outcome!(b::Broker, e::Entity, m::Stored, state, link)
    if m.lock_token === nothing || m.locked_until < NOW() || !(m in e.messages)
        return lock_lost()
    end
    if state isa A.Accepted
        filter!(x -> x !== m, e.messages)
    elseif state isa A.Released
        unlock_msg!(m)
        m.delivery_count -= 1
    elseif state isa A.Modified
        if state.undeliverable_here === true
            m.state = :deferred
            unlock_msg!(m)
        else
            unlock_msg!(m)
        end
    elseif state isa A.Rejected
        filter!(x -> x !== m, e.messages)
        dlq = entity!(b, e.name * "/\$DeadLetterQueue")
        raw = deepcopy(m.raw)
        ap = something(raw.application_properties, Dict{String,Any}())
        info = state.error === nothing ? nothing : state.error.info
        if info !== nothing
            for (k, v) in info
                ap[string(k)] = v
            end
        end
        raw.application_properties = ap
        ann = something(raw.message_annotations, Dict{Any,Any}())
        ann[Symbol("x-opt-deadletter-source")] = e.name
        raw.message_annotations = ann
        enqueue!(b, dlq, raw)
    end
    return state
end

# ---------------------------------------------------------------------------
# $cbs and $management
# ---------------------------------------------------------------------------

function reply!(b::Broker, c::MConn, s::MSession, req::A.AMQPMessage, status::Int, body; desc="", condition=nothing)
    reply_to = string(req.properties.reply_to)
    ap = Dict{String,Any}("statusCode" => Int32(status), "status-code" => Int32(status), "statusDescription" => desc)
    condition === nothing || (ap["errorCondition"] = Symbol(condition))
    resp = A.AMQPMessage(properties=A.Properties(correlation_id=req.properties.message_id, to=reply_to),
        application_properties=ap, body_type=:value, body=body)
    for l in values(s.links)
        if !l.role && l.target isa A.Target && l.target.address == reply_to
            if l.credit > 0
                send_delivery!(b, c, s, l, A.encode_message(resp), randuuid_bytes(), true)
            end
            return
        end
    end
end

function cbs_request!(b::Broker, c::MConn, s::MSession, l::MLink, req::A.AMQPMessage)
    ap = req.application_properties
    push!(b.put_tokens, (string(ap["name"]), string(ap["type"])))
    status = b.reject_tokens ? 401 : 202
    reply!(b, c, s, req, status, nothing; desc=status == 202 ? "Accepted" : "Unauthorized")
end

function find_locked(e::Entity, tok)
    for m in e.messages
        m.lock_token == tok && return m
    end
    return nothing
end

function mgmt_request!(b::Broker, c::MConn, s::MSession, l::MLink, req::A.AMQPMessage)
    op = string(req.application_properties["operation"])
    e = l.entity::Entity
    body = req.body
    try
        if op == "com.microsoft:peek-message"
            from = Int64(body["from-sequence-number"])
            n = Int(body["message-count"])
            sid = get(body, "session-id", nothing)
            promote_scheduled!(e)
            found = [m for m in e.messages if m.seq >= from && (sid === nothing || m.session_id == sid)]
            found = found[1:min(n, length(found))]
            isempty(found) && return reply!(b, c, s, req, 204, nothing)
            reply!(b, c, s, req, 200, Dict{String,Any}("messages" =>
                Any[Dict{String,Any}("message" => A.encode_message(delivered_message(b, m))) for m in found]))
        elseif op == "com.microsoft:renew-lock"
            exps = DateTime[]
            for tok in body["lock-tokens"]
                m = find_locked(e, tok)
                (m === nothing || m.locked_until < NOW()) &&
                    return reply!(b, c, s, req, 410, nothing; desc="lock lost", condition="com.microsoft:message-lock-lost")
                m.locked_until = NOW() + Dates.Millisecond(round(Int, b.lock_duration * 1000))
                push!(exps, m.locked_until)
            end
            reply!(b, c, s, req, 200, Dict{String,Any}("expirations" => A.AMQPArray(exps)))
        elseif op == "com.microsoft:schedule-message"
            seqs = Int64[]
            for entry in body["messages"]
                m = enqueue!(b, e, A.decode_message(entry["message"]))
                push!(seqs, m.seq)
            end
            reply!(b, c, s, req, 200, Dict{String,Any}("sequence-numbers" => A.AMQPArray(seqs)))
        elseif op == "com.microsoft:cancel-scheduled-message"
            nums = Set(body["sequence-numbers"])
            filter!(m -> !(m.seq in nums && m.state === :scheduled), e.messages)
            reply!(b, c, s, req, 200, nothing)
        elseif op == "com.microsoft:receive-by-sequence-number"
            out = Any[]
            for seq in body["sequence-numbers"]
                idx = findfirst(m -> m.seq == seq && m.state === :deferred, e.messages)
                idx === nothing && return reply!(b, c, s, req, 404, nothing; desc="not found",
                    condition="com.microsoft:message-not-found")
                m = e.messages[idx]
                if Int(body["receiver-settle-mode"]) == 1
                    m.lock_token = uuid4()
                    m.locked_until = NOW() + Dates.Millisecond(round(Int, b.lock_duration * 1000))
                    push!(out, Dict{String,Any}("message" => A.encode_message(delivered_message(b, m; lock_token=m.lock_token)),
                        "lock-token" => m.lock_token))
                else
                    deleteat!(e.messages, idx)
                    push!(out, Dict{String,Any}("message" => A.encode_message(delivered_message(b, m))))
                end
            end
            reply!(b, c, s, req, 200, Dict{String,Any}("messages" => out))
        elseif op == "com.microsoft:update-disposition"
            status = string(body["disposition-status"])
            for tok in body["lock-tokens"]
                m = find_locked(e, tok)
                m === nothing && return reply!(b, c, s, req, 410, nothing; desc="lock lost",
                    condition="com.microsoft:message-lock-lost")
                state = status == "completed" ? A.Accepted() :
                        status == "abandoned" ? A.Modified(delivery_failed=true) :
                        status == "defered" ? A.Modified(delivery_failed=true, undeliverable_here=true) :
                        A.Rejected(error=A.AMQPError(condition=Symbol("com.microsoft:dead-letter"),
                            info=Dict{Any,Any}("DeadLetterReason" => get(body, "deadletter-reason", nothing),
                                "DeadLetterErrorDescription" => get(body, "deadletter-description", nothing))))
                if m.state === :deferred && state isa A.Accepted
                    filter!(x -> x !== m, e.messages)
                else
                    m.state = :active
                    apply_outcome!(b, e, m, state, nothing)
                end
            end
            reply!(b, c, s, req, 200, nothing)
            pump_all!(b)
        elseif op == "com.microsoft:get-session-state"
            sid = string(body["session-id"])
            st = get(e.session_states, sid, nothing)
            reply!(b, c, s, req, 200, Dict{String,Any}("session-state" => st))
        elseif op == "com.microsoft:set-session-state"
            sid = string(body["session-id"])
            st = body["session-state"]
            st === nothing ? delete!(e.session_states, sid) : (e.session_states[sid] = Vector{UInt8}(st))
            reply!(b, c, s, req, 200, nothing)
        elseif op == "com.microsoft:renew-session-lock"
            sid = string(body["session-id"])
            until = NOW() + Dates.Millisecond(round(Int, b.lock_duration * 1000))
            e.session_locked_until[sid] = until
            reply!(b, c, s, req, 200, Dict{String,Any}("expiration" => until))
        else
            reply!(b, c, s, req, 400, nothing; desc="unsupported operation $op")
        end
    catch err
        @error "mock broker management error" op exception = (err, catch_backtrace())
        reply!(b, c, s, req, 500, nothing; desc=sprint(showerror, err))
    end
end

# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------

function start!(b::Broker; tls_cert=nothing, tls_key=nothing, websocket::Bool=false)
    b.listener = Reseau.TCP.listen("127.0.0.1:0"; backlog=64)
    b.port = Reseau.TCP.addr(b.listener).port
    push!(b.tasks, errormonitor(@async accept_loop(b, b.listener, :tcp)))
    if tls_cert !== nothing
        cfg = Reseau.TLS.Config(cert_file=tls_cert, key_file=tls_key)
        b.tls_listener = Reseau.TLS.listen("tcp", "127.0.0.1:0", cfg)
        b.tls_port = Reseau.TLS.addr(b.tls_listener).port
        push!(b.tasks, errormonitor(@async accept_loop(b, b.tls_listener, :tls)))
    end
    if websocket
        b.ws_server = HTTP.WebSockets.listen!("127.0.0.1", 0; listenany=true, subprotocols=["AMQPWSB10"]) do ws
            serve_connection(b, A.WebSocketTransport(ws, UInt8[], 1, "ws-server"))
        end
        b.ws_port = parse(Int, split(HTTP.WebSockets.server_addr(b.ws_server), ':')[end])
        if tls_cert !== nothing
            cfg = Reseau.TLS.Config(cert_file=tls_cert, key_file=tls_key)
            b.wss_server = HTTP.WebSockets.listen!("127.0.0.1", 0; listenany=true, subprotocols=["AMQPWSB10"],
                tls_config=cfg) do ws
                serve_connection(b, A.WebSocketTransport(ws, UInt8[], 1, "wss-server"))
            end
            b.wss_port = parse(Int, split(HTTP.WebSockets.server_addr(b.wss_server), ':')[end])
        end
    end
    return b
end

function accept_loop(b::Broker, listener, kind)
    while !b.stopped
        conn = try
            kind === :tls ? Reseau.TLS.accept(listener) : Reseau.TCP.accept(listener)
        catch
            b.stopped && return
            continue
        end
        t = A.SocketTransport(conn, "mock-$(kind)")
        Threads.@spawn try
            kind === :tls && Reseau.TLS.handshake!(conn)
            serve_connection(b, t)
        catch err
            b.stopped || @debug "mock broker accept error" exception = err
            close(t)
        end
    end
end

function stop!(b::Broker)
    b.stopped = true
    try close(b.listener) catch end
    b.tls_listener === nothing || try close(b.tls_listener) catch end
    b.ws_server === nothing || try HTTP.WebSockets.forceclose(b.ws_server) catch end
    b.wss_server === nothing || try HTTP.WebSockets.forceclose(b.wss_server) catch end
    for c in copy(b.conns)
        c.closed = true
        close(c.transport)
    end
end

"Close every client connection abruptly (simulates a network failure)."
function kill_connections!(b::Broker)
    for c in @lock(b.lock, copy(b.conns))
        c.closed = true
        close(c.transport)
    end
end

connection_string(b::Broker; entity=nothing) =
    "Endpoint=sb://localhost:$(b.port);SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=secret;UseDevelopmentEmulator=true" *
    (entity === nothing ? "" : ";EntityPath=$entity")

end # module
