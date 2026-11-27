# In-memory transport and a minimal AMQP peer.
#
# Used by the precompile workload (so the whole client stack is compiled at
# install time without touching the network) and handy for unit tests of code
# built on top of this package: `TransportConfig(scheme=:loopback, host=...)`
# connects to an in-process peer that accepts every link, stores messages per
# address, delivers them to receivers and answers `$cbs` / `$management`
# requests with status 200.

"One end of an in-memory byte pipe."
mutable struct MemoryTransport <: AMQPTransport
    incoming::Channel{Vector{UInt8}}
    outgoing::Channel{Vector{UInt8}}
    pending::Vector{UInt8}
    pos::Int
end

"Create two connected in-memory transports."
function memory_pair()
    a2b = Channel{Vector{UInt8}}(typemax(Int))
    b2a = Channel{Vector{UInt8}}(typemax(Int))
    return MemoryTransport(b2a, a2b, UInt8[], 1), MemoryTransport(a2b, b2a, UInt8[], 1)
end

function read_some!(t::MemoryTransport, buf::Vector{UInt8})
    while t.pos > length(t.pending)
        t.pending = try
            take!(t.incoming)
        catch err
            err isa InvalidStateException && throw(EOFError())
            rethrow()
        end
        t.pos = 1
    end
    n = min(length(buf), length(t.pending) - t.pos + 1)
    copyto!(buf, 1, t.pending, t.pos, n)
    t.pos += n
    return n
end

function write_all(t::MemoryTransport, bytes::Vector{UInt8})
    isopen(t.outgoing) || throw(Base.IOError("memory transport closed", 0))
    put!(t.outgoing, copy(bytes))
    return nothing
end

function Base.close(t::MemoryTransport)
    close(t.outgoing)
    close(t.incoming)
    return nothing
end
Base.isopen(t::MemoryTransport) = isopen(t.outgoing)

# --- the peer -------------------------------------------------------------------

mutable struct LoopbackLink
    handle::UInt32
    role::Bool                   # our role
    address::String
    target_address::String
    credit::UInt32
    delivery_count::UInt32
    settled_mode::Bool
    partial::Vector{UInt8}
end

mutable struct LoopbackPeer
    transport::MemoryTransport
    reader::TransportReader
    queues::Dict{String, Vector{Vector{UInt8}}}
    links::Dict{Tuple{UInt16, UInt32}, LoopbackLink}    # (channel, remote handle)
    next_handle::UInt32
    next_delivery::UInt32
end

function _peer_send(p::LoopbackPeer, perf; channel::UInt16 = UInt16(0), payload = nothing, frame_type = FRAME_TYPE_AMQP)
    return write_all(p.transport, encode_frame(perf; channel = channel, payload = payload, frame_type = frame_type))
end

function _peer_frame(p::LoopbackPeer)
    hdr = read_exact(p.reader, FRAME_HEADER_SIZE)
    return decode_frame(hdr, read_exact(p.reader, Int(frame_size(hdr)) - FRAME_HEADER_SIZE))
end

"Serve one client connection until it closes."
function serve_loopback(t::MemoryTransport)
    p = LoopbackPeer(
        t, TransportReader(t), Dict{String, Vector{Vector{UInt8}}}(),
        Dict{Tuple{UInt16, UInt32}, LoopbackLink}(), 0, 0
    )
    try
        hdr = read_exact(p.reader, 8)
        if hdr == SASL_HEADER
            write_all(t, copy(SASL_HEADER))
            _peer_send(p, SaslMechanisms(sasl_server_mechanisms = [:ANONYMOUS, :PLAIN]); frame_type = FRAME_TYPE_SASL)
            _peer_frame(p)
            _peer_send(p, SaslOutcome(code = 0x00); frame_type = FRAME_TYPE_SASL)
            hdr = read_exact(p.reader, 8)
        end
        write_all(t, copy(AMQP_HEADER))
        _peer_frame(p)
        _peer_send(p, Open(container_id = "loopback", max_frame_size = UInt32(65536)))
        while true
            f = _peer_frame(p)
            f.body === nothing && continue
            _peer_handle!(p, f) || break
        end
    catch err
        err isa EOFError || err isa Base.IOError || err isa InvalidStateException ||
            @debug "loopback peer stopped" exception = err
    finally
        close(t)
    end
    return nothing
end

function _peer_flow(p::LoopbackPeer, ch::UInt16, l::LoopbackLink; drain::Bool = false)
    return _peer_send(
        p, Flow(
            next_incoming_id = UInt32(0), incoming_window = UInt32(100_000), next_outgoing_id = UInt32(0),
            outgoing_window = UInt32(100_000), handle = l.handle, delivery_count = l.delivery_count, link_credit = l.credit,
            drain = drain ? true : nothing
        ); channel = ch
    )
end

function _peer_pump!(p::LoopbackPeer, ch::UInt16, l::LoopbackLink)
    q = get(p.queues, l.address, nothing)
    while l.credit > 0 && q !== nothing && !isempty(q)
        payload = popfirst!(q)
        l.credit -= 1
        l.delivery_count += 1
        p.next_delivery += 1
        _peer_send(
            p, Transfer(
                handle = l.handle, delivery_id = p.next_delivery, delivery_tag = reinterpret(UInt8, [hton(p.next_delivery)]),
                message_format = UInt32(0), settled = l.settled_mode
            ); channel = ch, payload = payload
        )
    end
    return
end

function _peer_reply(p::LoopbackPeer, ch::UInt16, req::AMQPMessage)
    reply_to = req.properties === nothing ? nothing : req.properties.reply_to
    reply_to === nothing && return
    op = something(req.application_properties, Dict{String, Any}())
    body = get(op, "operation", "") == "READ" ?
        Dict{String, Any}(
            "name" => get(op, "name", ""), "partition_ids" => AMQPArray(["0"]),
            "partition" => get(op, "partition", "0"), "created_at" => UNIX_EPOCH
        ) : Dict{String, Any}()
    resp = AMQPMessage(
        properties = Properties(correlation_id = req.properties.message_id),
        application_properties = Dict{String, Any}("status-code" => Int32(200), "statusCode" => Int32(200)),
        body_type = :value, body = body
    )
    push!(get!(p.queues, "reply:" * string(reply_to), Vector{UInt8}[]), encode_message(resp))
    for ((c, _), l) in p.links
        (c == ch && !l.role && l.target_address == string(reply_to)) || continue
        l.address = "reply:" * string(reply_to)
        _peer_pump!(p, ch, l)
    end
    return
end

function _peer_handle!(p::LoopbackPeer, f::Frame)
    b = f.body
    ch = f.channel
    if b isa Begin
        _peer_send(
            p, Begin(
                remote_channel = ch, next_outgoing_id = UInt32(0), incoming_window = UInt32(100_000),
                outgoing_window = UInt32(100_000)
            ); channel = ch
        )
    elseif b isa Attach
        our_role = !b.role
        p.next_handle += 1
        src = b.source isa Source ? string(something(b.source.address, "")) : ""
        tgt = b.target isa Target ? string(something(b.target.address, "")) : ""
        l = LoopbackLink(
            p.next_handle, our_role, our_role ? tgt : src, tgt, 0, 0,
            b.snd_settle_mode == SND_SETTLED, UInt8[]
        )
        p.links[(ch, b.handle)] = l
        _peer_send(
            p, Attach(
                name = b.name, handle = l.handle, role = our_role, snd_settle_mode = b.snd_settle_mode,
                rcv_settle_mode = b.rcv_settle_mode, source = b.source, target = b.target,
                initial_delivery_count = our_role ? nothing : UInt32(0)
            ); channel = ch
        )
        if our_role
            l.credit = 1000
            _peer_flow(p, ch, l)
        end
    elseif b isa Flow
        b.handle === nothing && return true
        l = get(p.links, (ch, b.handle), nothing)
        l === nothing && return true
        if !l.role
            l.credit = (something(b.delivery_count, UInt32(0)) + something(b.link_credit, UInt32(0)) - l.delivery_count) % UInt32
            _peer_pump!(p, ch, l)
            if b.drain === true
                l.delivery_count += l.credit
                l.credit = 0
                _peer_flow(p, ch, l; drain = true)
            end
        end
    elseif b isa Transfer
        l = p.links[(ch, b.handle)]
        f.payload === nothing || append!(l.partial, f.payload)
        b.more === true && return true
        payload = l.partial
        l.partial = UInt8[]
        if l.address == "\$cbs" || endswith(l.address, "\$management")
            _peer_reply(p, ch, decode_message(payload))
        else
            push!(get!(p.queues, l.address, Vector{UInt8}[]), payload)
            for ((c, _), rl) in p.links
                (c == ch && !rl.role && rl.address == l.address) && _peer_pump!(p, ch, rl)
            end
        end
        l.delivery_count += 1
        l.credit -= 1
        if l.credit < 100
            l.credit = 1000
            _peer_flow(p, ch, l)
        end
        b.settled === true || b.delivery_id === nothing ||
            _peer_send(p, Disposition(role = true, first = b.delivery_id, settled = true, state = Accepted()); channel = ch)
    elseif b isa Disposition
        b.settled === true || _peer_send(
            p, Disposition(
                role = false, first = b.first, last = b.last, settled = true,
                state = b.state
            ); channel = ch
        )
    elseif b isa Detach
        l = pop!(p.links, (ch, b.handle), nothing)
        l === nothing || _peer_send(p, Detach(handle = l.handle, closed = true); channel = ch)
    elseif b isa End
        _peer_send(p, End(); channel = ch)
    elseif b isa Close
        _peer_send(p, Close())
        return false
    end
    return true
end

"Connect a new in-memory transport to a fresh loopback peer."
function open_loopback()
    client, server = memory_pair()
    Threads.@spawn serve_loopback(server)
    return client
end
