# AMQP 1.0 connection, session and link state machines (Part 2: Transport).
#
# Concurrency model
# -----------------
# * One reader task per connection decodes frames and updates state.
# * One writer task drains an unbounded outgoing frame queue and coalesces
#   frames into large socket writes. No state lock is ever held while doing
#   socket I/O, so a slow peer can never deadlock the reader.
# * All connection/session/link state is protected by the connection's single
#   `ReentrantLock`. Waiters use `Threads.Condition`s bound to that lock (one
#   per session/link/delivery so wake-ups are targeted).

"""
    AMQPException(scope, condition, description; info=nothing, cause=nothing)

Error raised by the AMQP layer. `scope` is one of `:connection`, `:session`,
`:link`, `:delivery`, `:transport`, `:timeout` or `:protocol`; `condition` is the
AMQP error condition symbol (e.g. `Symbol("amqp:not-found")`).
"""
struct AMQPException <: Exception
    scope::Symbol
    condition::Symbol
    description::String
    info::Any
    cause::Union{Nothing,Exception}
end
AMQPException(scope::Symbol, condition::Symbol, description::AbstractString; info=nothing, cause=nothing) =
    AMQPException(scope, condition, String(description), info, cause)

function AMQPException(scope::Symbol, err::Union{Nothing,AMQPError}, default_description::AbstractString)
    err === nothing && return AMQPException(scope, Symbol("amqp:internal-error"), default_description)
    return AMQPException(scope, err.condition, something(err.description, default_description); info=err.info)
end

function Base.showerror(io::IO, e::AMQPException)
    print(io, "AMQPException(", e.scope, "): ", e.condition, ": ", e.description)
    if e.cause !== nothing
        print(io, "\ncaused by: ")
        showerror(io, e.cause)
    end
end

const COND_INTERNAL = Symbol("amqp:internal-error")
const COND_NOT_FOUND = Symbol("amqp:not-found")
const COND_UNAUTHORIZED = Symbol("amqp:unauthorized-access")
const COND_DECODE = Symbol("amqp:decode-error")
const COND_NOT_ALLOWED = Symbol("amqp:not-allowed")
const COND_FRAMING = Symbol("amqp:connection:framing-error")
const COND_CONN_FORCED = Symbol("amqp:connection:forced")
const COND_DETACH_FORCED = Symbol("amqp:link:detach-forced")
const COND_TIMEOUT = Symbol("com.microsoft:timeout")
const COND_SOCKET = Symbol("amqp:socket-error")
const COND_CLOSED = Symbol("amqp:closed")
const COND_MSG_SIZE = Symbol("amqp:link:message-size-exceeded")

timeout_error(what::AbstractString) = AMQPException(:timeout, COND_TIMEOUT, "timed out waiting for $what")

# ---------------------------------------------------------------------------
# Waiting helpers
# ---------------------------------------------------------------------------

@inline deadline_after(timeout::Real) = timeout <= 0 || isinf(timeout) ? UInt64(0) : time_ns() + round(UInt64, timeout * 1e9)

"""
    wait_until(pred, cond, deadline) -> Bool

Wait on `cond` (whose lock must be held) until `pred()` is true. Returns
`false` if the deadline (`time_ns()` value, `0` = none) passes first.
"""
function wait_until(pred, cond::Threads.Condition, deadline::UInt64)
    pred() && return true
    timer = nothing
    try
        while !pred()
            if deadline != 0
                now = time_ns()
                now >= deadline && return false
                if timer === nothing
                    timer = Timer((deadline - now) / 1e9) do _
                        lock(cond) do
                            notify(cond)
                        end
                    end
                end
            end
            wait(cond)
        end
        return true
    finally
        timer === nothing || close(timer)
    end
end

# serial number arithmetic (RFC 1982) on 32-bit transfer/delivery ids
@inline serial_lt(a::UInt32, b::UInt32) = a != b && ((b - a) % UInt32) < 0x80000000
@inline serial_le(a::UInt32, b::UInt32) = a == b || serial_lt(a, b)

# ---------------------------------------------------------------------------
# Types
# ---------------------------------------------------------------------------

abstract type Link end

mutable struct Session
    conn::Any                       # Connection (declared below)
    channel::UInt16
    remote_channel::Union{Nothing,UInt16}
    state::Symbol                   # :begin_sent, :mapped, :end_sent, :ended
    error::Union{Nothing,Exception}
    cond::Threads.Condition
    # flow control
    next_outgoing_id::UInt32
    incoming_window_max::UInt32
    incoming_window::UInt32
    outgoing_window::UInt32
    next_incoming_id::UInt32
    remote_incoming_window::UInt32
    remote_outgoing_window::UInt32
    handle_max::UInt32
    next_delivery_id::UInt32
    links::Dict{UInt32,Link}            # by local handle
    remote_links::Dict{UInt32,Link}     # by remote handle
    outgoing_unsettled::Dict{UInt32,Any} # delivery id => OutgoingDelivery
end

mutable struct Connection
    transport::AMQPTransport
    config::Any
    hostname::String
    container_id::String
    lock::ReentrantLock
    cond::Threads.Condition
    state::Symbol                   # :opened, :close_sent, :closed
    error::Union{Nothing,Exception}
    local_max_frame_size::UInt32
    max_frame_size::UInt32          # max size of frames we may send
    channel_max::UInt16
    idle_timeout::Float64           # advertised (seconds, 0 = none)
    remote_idle_timeout::Float64
    remote_open::Union{Nothing,Open}
    sessions::Dict{UInt16,Session}
    remote_sessions::Dict{UInt16,Session}
    outq::Channel{Vector{UInt8}}
    reader::Union{Nothing,Task}
    writer::Union{Nothing,Task}
    heartbeat::Union{Nothing,Task}
    last_sent_ns::Threads.Atomic{UInt64}
    last_recv_ns::Threads.Atomic{UInt64}
    max_message_size::Int
    close_callbacks::Vector{Any}
end

"""
A message delivery received on a receiver link.
"""
mutable struct IncomingDelivery
    link::Any
    delivery_id::UInt32
    delivery_tag::Vector{UInt8}
    message_format::UInt32
    settled::Bool                   # settled by us or pre-settled by the sender
    remote_state::Any
    remote_settled::Bool
    payload::Vector{UInt8}
    waiting::Bool                   # a local settle! call awaits the peer's outcome
end

mutable struct SenderLink <: Link
    session::Session
    name::String
    handle::UInt32
    remote_handle::Union{Nothing,UInt32}
    state::Symbol                   # :attach_sent, :attached, :detach_sent, :detached
    error::Union{Nothing,Exception}
    cond::Threads.Condition
    source::Union{Nothing,Source}
    target::Union{Nothing,Target}
    remote_attach::Union{Nothing,Attach}
    snd_settle_mode::UInt8
    rcv_settle_mode::UInt8
    delivery_count::UInt32
    link_credit::UInt32
    send_lock::ReentrantLock
    next_tag::UInt64
    max_message_size::UInt64        # 0 = unlimited
end

mutable struct ReceiverLink <: Link
    session::Session
    name::String
    handle::UInt32
    remote_handle::Union{Nothing,UInt32}
    state::Symbol
    error::Union{Nothing,Exception}
    cond::Threads.Condition
    source::Union{Nothing,Source}
    target::Union{Nothing,Target}
    remote_attach::Union{Nothing,Attach}
    snd_settle_mode::UInt8
    rcv_settle_mode::UInt8
    delivery_count::UInt32
    link_credit::UInt32
    available::UInt32
    draining::Bool
    prefetch::Int                   # automatic credit target (0 = manual credit)
    queue::Vector{IncomingDelivery}
    partial::Union{Nothing,IncomingDelivery}
    unsettled::Dict{UInt32,IncomingDelivery}
    on_delivery::Union{Nothing,Function}
    max_message_size::Int
end

const SND_UNSETTLED = 0x00
const SND_SETTLED = 0x01
const SND_MIXED = 0x02
const RCV_FIRST = 0x00
const RCV_SECOND = 0x01

const ROLE_SENDER = false
const ROLE_RECEIVER = true

# ---------------------------------------------------------------------------
# Connection establishment
# ---------------------------------------------------------------------------

"""
    ConnectionConfig

Options for [`connect`](@ref).

- `sasl_mechanism`: `:anonymous` (Azure CBS authentication), `:plain`,
  `:external` or `:none` (no SASL layer).
- `idle_timeout`: seconds of inactivity after which we consider the peer
  dead (advertised to the peer; `0` disables).
- `max_frame_size`: largest frame we accept.
"""
Base.@kwdef struct ConnectionConfig
    hostname::String
    container_id::String = string(uuid4())
    sasl_mechanism::Symbol = :anonymous
    sasl_username::Union{Nothing,String} = nothing
    sasl_password::Union{Nothing,String} = nothing
    max_frame_size::UInt32 = UInt32(1024 * 1024)
    channel_max::UInt16 = UInt16(65535)
    idle_timeout::Float64 = 0.0
    open_timeout::Float64 = 60.0
    properties::Dict{Symbol,Any} = Dict{Symbol,Any}()
    max_message_size::Int = 256 * 1024 * 1024
end

function Base.show(io::IO, c::ConnectionConfig)
    print(io, "ConnectionConfig(hostname=", repr(c.hostname), ", sasl=", c.sasl_mechanism, ")")
end

function _write_raw(t::AMQPTransport, bytes::Vector{UInt8})
    write_all(t, bytes)
end

function _read_frame(reader::TransportReader, max_frame_size::UInt32)
    header = read_exact(reader, FRAME_HEADER_SIZE)
    size = frame_size(header)
    size < FRAME_HEADER_SIZE && throw(AMQPException(:protocol, COND_FRAMING, "frame size $size smaller than header"))
    size > max_frame_size && throw(AMQPException(:protocol, COND_FRAMING, "peer sent frame of $size bytes, exceeding max-frame-size $max_frame_size"))
    body = read_exact(reader, Int(size) - FRAME_HEADER_SIZE)
    return decode_frame(header, body)
end

function _sasl_handshake!(transport::AMQPTransport, reader::TransportReader, cfg::ConnectionConfig)
    _write_raw(transport, copy(SASL_HEADER))
    hdr = read_exact(reader, 8)
    hdr == SASL_HEADER || throw(AMQPException(:protocol, COND_NOT_ALLOWED, "peer rejected SASL protocol header"))
    frame = _read_frame(reader, UInt32(MIN_MAX_FRAME_SIZE * 128))
    mechs = frame.body
    mechs isa SaslMechanisms || throw(AMQPException(:protocol, COND_NOT_ALLOWED, "expected sasl-mechanisms frame"))
    mech = cfg.sasl_mechanism === :anonymous ? :ANONYMOUS :
           cfg.sasl_mechanism === :plain ? :PLAIN :
           cfg.sasl_mechanism === :external ? :EXTERNAL :
           cfg.sasl_mechanism === :mssbcbs ? :MSSBCBS :
           throw(ArgumentError("unsupported SASL mechanism $(cfg.sasl_mechanism)"))
    mech in mechs.sasl_server_mechanisms ||
        throw(AMQPException(:connection, COND_NOT_ALLOWED, "server does not offer SASL mechanism $mech (offered: $(join(mechs.sasl_server_mechanisms, ", ")))"))
    initial = if mech === :PLAIN
        user = something(cfg.sasl_username, "")
        pass = something(cfg.sasl_password, "")
        vcat(UInt8[0x00], Vector{UInt8}(codeunits(user)), UInt8[0x00], Vector{UInt8}(codeunits(pass)))
    else
        UInt8[]
    end
    _write_raw(transport, encode_frame(SaslInit(mechanism=mech, initial_response=initial, hostname=cfg.hostname);
        frame_type=FRAME_TYPE_SASL))
    fill!(initial, 0x00)
    while true
        frame = _read_frame(reader, UInt32(MIN_MAX_FRAME_SIZE * 128))
        b = frame.body
        if b isa SaslOutcome
            b.code == 0x00 && return nothing
            codes = ("ok", "auth", "sys", "sys-perm", "sys-temp")
            desc = b.code + 1 <= length(codes) ? codes[b.code+1] : string(b.code)
            throw(AMQPException(:connection, COND_UNAUTHORIZED, "SASL authentication failed (code: $desc)"))
        elseif b isa SaslChallenge
            # none of the supported mechanisms use challenges
            _write_raw(transport, encode_frame(SaslResponse(response=UInt8[]); frame_type=FRAME_TYPE_SASL))
        else
            throw(AMQPException(:protocol, COND_NOT_ALLOWED, "unexpected SASL frame $(typeof(b))"))
        end
    end
end

"""
    connect(transport_config, connection_config) -> Connection
    connect(transport::AMQPTransport, connection_config) -> Connection

Open an AMQP connection: transport, optional SASL layer, protocol header and
open handshake. The whole sequence is bounded by `connection_config.open_timeout`.
"""
function connect(tc::TransportConfig, cfg::ConnectionConfig)
    transport = open_transport(tc)
    try
        return connect(transport, cfg)
    catch
        close(transport)
        rethrow()
    end
end

function connect(transport::AMQPTransport, cfg::ConnectionConfig)
    reader = TransportReader(transport)
    handshake = Threads.@spawn begin
        cfg.sasl_mechanism === :none || _sasl_handshake!(transport, reader, cfg)
        open_frame = Open(
            container_id=cfg.container_id,
            hostname=cfg.hostname,
            max_frame_size=cfg.max_frame_size,
            channel_max=cfg.channel_max,
            idle_time_out=cfg.idle_timeout > 0 ? round(UInt32, cfg.idle_timeout * 1000) : nothing,
            properties=isempty(cfg.properties) ? nothing : cfg.properties,
        )
        _write_raw(transport, vcat(AMQP_HEADER, encode_frame(open_frame)))
        hdr = read_exact(reader, 8)
        hdr == AMQP_HEADER || throw(AMQPException(:protocol, COND_NOT_ALLOWED, "peer rejected AMQP protocol header"))
        local remote_open
        while true
            frame = _read_frame(reader, cfg.max_frame_size)
            frame.body === nothing && continue
            if frame.body isa Open
                remote_open = frame.body
                break
            elseif frame.body isa Close
                throw(AMQPException(:connection, frame.body.error, "connection closed by peer during open"))
            else
                throw(AMQPException(:protocol, COND_NOT_ALLOWED, "expected open frame, got $(typeof(frame.body))"))
            end
        end
        remote_open
    end
    timed_out = Threads.Atomic{Bool}(false)
    timer = Timer(cfg.open_timeout) do _
        if !istaskdone(handshake)
            timed_out[] = true
            close(transport)
        end
    end
    remote_open = try
        fetch(handshake)
    catch err
        inner = err isa TaskFailedException ? err.task.exception : err
        if timed_out[]
            throw(timeout_error("AMQP connection handshake"))
        end
        throw(inner)
    finally
        close(timer)
    end
    remote_open::Open

    lk = ReentrantLock()
    max_frame = min(cfg.max_frame_size, something(remote_open.max_frame_size, typemax(UInt32)))
    max_frame < MIN_MAX_FRAME_SIZE && (max_frame = UInt32(MIN_MAX_FRAME_SIZE))
    conn = Connection(
        transport, cfg, cfg.hostname, cfg.container_id, lk, Threads.Condition(lk), :opened, nothing,
        cfg.max_frame_size, max_frame,
        min(cfg.channel_max, something(remote_open.channel_max, typemax(UInt16))),
        cfg.idle_timeout,
        something(remote_open.idle_time_out, UInt32(0)) / 1000,
        remote_open, Dict{UInt16,Session}(), Dict{UInt16,Session}(),
        Channel{Vector{UInt8}}(typemax(Int)), nothing, nothing, nothing,
        Threads.Atomic{UInt64}(time_ns()), Threads.Atomic{UInt64}(time_ns()),
        cfg.max_message_size, Any[])
    conn.writer = errormonitor_quiet(Threads.@spawn _writer_loop(conn))
    conn.reader = errormonitor_quiet(Threads.@spawn _reader_loop(conn, reader))
    if conn.remote_idle_timeout > 0 || conn.idle_timeout > 0
        conn.heartbeat = errormonitor_quiet(Threads.@spawn _heartbeat_loop(conn))
    end
    return conn
end

# Tasks handle their own errors; this only prevents noisy "task failed" logs.
errormonitor_quiet(t::Task) = t

Base.isopen(c::Connection) = @lock c.lock c.state === :opened
connection_error(c::Connection) = @lock c.lock c.error

function Base.show(io::IO, c::Connection)
    print(io, "AMQP.Connection(", c.hostname, ", state=", c.state, ")")
end

"Register `f(conn)` to be called once when the connection terminates."
function on_close(f, c::Connection)
    run_now = @lock c.lock begin
        c.state === :closed ? true : (push!(c.close_callbacks, f); false)
    end
    run_now && f(c)
    return nothing
end

@inline function _send_frame!(c::Connection, bytes::Vector{UInt8})
    try
        put!(c.outq, bytes)
    catch err
        err isa InvalidStateException || rethrow()
        throw(something(c.error, AMQPException(:connection, COND_CLOSED, "connection is closed")))
    end
    return nothing
end

_send_performative!(c::Connection, perf; channel::UInt16=UInt16(0), payload=nothing) =
    _send_frame!(c, encode_frame(perf; channel=channel, payload=payload))

function _writer_loop(c::Connection)
    buf = ByteWriter(256 * 1024)
    try
        while true
            frame = take!(c.outq)
            if !isready(c.outq)
                write_all(c.transport, frame)
            else
                buf.len = 0
                writebytes!(buf, frame)
                while isready(c.outq) && buf.len < 256 * 1024
                    writebytes!(buf, take!(c.outq))
                end
                write_all(c.transport, buf.data[1:buf.len])
            end
            c.last_sent_ns[] = time_ns()
        end
    catch err
        if !(err isa InvalidStateException)   # queue closed: normal shutdown
            _connection_failed!(c, AMQPException(:transport, COND_SOCKET, "error writing to transport"; cause=_as_exception(err)))
        end
    end
end

_as_exception(err) = err isa Exception ? err : ErrorException(string(err))

function _reader_loop(c::Connection, reader::TransportReader)
    try
        while true
            frame = _read_frame(reader, c.local_max_frame_size)
            c.last_recv_ns[] = time_ns()
            frame.body === nothing && continue
            if frame.frame_type != FRAME_TYPE_AMQP
                throw(AMQPException(:protocol, COND_NOT_ALLOWED, "unexpected SASL frame after open"))
            end
            _dispatch!(c, frame)
            @lock c.lock (c.state === :closed && return)
        end
    catch err
        ex = _as_exception(err)
        closing = @lock c.lock c.state !== :opened
        if closing && (ex isa EOFError || ex isa Base.IOError)
            _connection_failed!(c, AMQPException(:connection, COND_CLOSED, "connection closed"))
        elseif ex isa AMQPException && ex.scope === :protocol
            _close_with_error!(c, AMQPError(condition=ex.condition, description=ex.description))
            _connection_failed!(c, ex)
        elseif ex isa AMQPDecodeError
            _close_with_error!(c, AMQPError(condition=COND_DECODE, description=ex.msg))
            _connection_failed!(c, AMQPException(:protocol, COND_DECODE, ex.msg))
        else
            _connection_failed!(c, AMQPException(:transport, COND_SOCKET, "connection lost"; cause=ex))
        end
    end
end

function _close_with_error!(c::Connection, err::AMQPError)
    try
        @lock c.lock begin
            if c.state === :opened
                c.state = :close_sent
                _send_performative!(c, Close(error=err))
            end
        end
    catch
    end
end

function _heartbeat_loop(c::Connection)
    send_interval = c.remote_idle_timeout > 0 ? c.remote_idle_timeout / 2 : Inf
    check_interval = min(send_interval, c.idle_timeout > 0 ? c.idle_timeout / 2 : Inf, 15.0)
    while true
        sleep(check_interval)
        @lock c.lock (c.state === :closed && return)
        now = time_ns()
        if send_interval < Inf && (now - c.last_sent_ns[]) / 1e9 >= send_interval
            try
                _send_frame!(c, copy(HEARTBEAT_FRAME))
            catch
                return
            end
        end
        if c.idle_timeout > 0 && (now - c.last_recv_ns[]) / 1e9 > 2 * c.idle_timeout
            _connection_failed!(c, AMQPException(:connection, COND_CONN_FORCED,
                "no frames received from peer for $(2 * c.idle_timeout) seconds"))
            return
        end
    end
end

"""
Tear down the connection after a failure or remote close: record the error,
fail every waiter, close the transport and run close callbacks.
"""
function _connection_failed!(c::Connection, err::Exception)
    callbacks = @lock c.lock begin
        c.state === :closed && return nothing
        c.state = :closed
        c.error === nothing && (c.error = err)
        for s in values(c.sessions)
            _session_failed!(s, c.error)
        end
        notify(c.cond)
        cbs = copy(c.close_callbacks)
        empty!(c.close_callbacks)
        cbs
    end
    close(c.outq)
    close(c.transport)
    for f in callbacks
        try
            f(c)
        catch cb_err
            @debug "AMQP connection close callback failed" exception = cb_err
        end
    end
    return nothing
end

"""
    close(conn::Connection; timeout=10)

Close the connection gracefully (sends `close` and waits briefly for the
peer's `close`).
"""
function Base.close(c::Connection; timeout::Real=10.0)
    lock(c.lock)
    try
        if c.state === :opened
            c.state = :close_sent
            try
                _send_performative!(c, Close())
            catch
            end
            wait_until(() -> c.state === :closed, c.cond, deadline_after(timeout))
        end
    finally
        unlock(c.lock)
    end
    _connection_failed!(c, AMQPException(:connection, COND_CLOSED, "connection closed by client"))
    return nothing
end

# ---------------------------------------------------------------------------
# Frame dispatch (runs on the reader task)
# ---------------------------------------------------------------------------

function _dispatch!(c::Connection, frame::Frame)
    perf = frame.body
    lock(c.lock)
    try
        if perf isa Transfer
            s = _remote_session(c, frame.channel)
            s === nothing || _on_transfer!(s, perf, frame.payload)
        elseif perf isa Disposition
            s = _remote_session(c, frame.channel)
            s === nothing || _on_disposition!(s, perf)
        elseif perf isa Flow
            s = _remote_session(c, frame.channel)
            s === nothing || _on_flow!(s, perf)
        elseif perf isa Attach
            s = _remote_session(c, frame.channel)
            s === nothing || _on_attach!(s, perf)
        elseif perf isa Detach
            s = _remote_session(c, frame.channel)
            s === nothing || _on_detach!(s, perf)
        elseif perf isa Begin
            _on_begin!(c, frame.channel, perf)
        elseif perf isa End
            _on_end!(c, frame.channel, perf)
        elseif perf isa Close
            _on_close!(c, perf)
        elseif perf isa Open
            throw(AMQPException(:protocol, COND_NOT_ALLOWED, "duplicate open frame"))
        else
            throw(AMQPException(:protocol, COND_NOT_ALLOWED, "unexpected frame body $(typeof(perf))"))
        end
    finally
        unlock(c.lock)
    end
end

function _remote_session(c::Connection, channel::UInt16)
    s = get(c.remote_sessions, channel, nothing)
    s === nothing && throw(AMQPException(:protocol, Symbol("amqp:connection:framing-error"),
        "frame received on unmapped channel $channel"))
    return s
end

function _on_close!(c::Connection, perf::Close)
    err = perf.error !== nothing ? AMQPException(:connection, perf.error, "connection closed by peer") :
          c.state === :close_sent ? AMQPException(:connection, COND_CLOSED, "connection closed by client") :
          AMQPException(:connection, COND_CLOSED, "connection closed by peer")
    if c.state === :opened
        # remote initiated: acknowledge
        try
            _send_performative!(c, Close())
        catch
        end
    end
    c.error === nothing && (c.error = err)
    # the writer needs a moment to flush our close; closing the queue lets it finish
    @async begin
        sleep(0.05)
        _connection_failed!(c, err)
    end
    c.state = :closed_pending
    notify(c.cond)
end

# ---------------------------------------------------------------------------
# Sessions
# ---------------------------------------------------------------------------

"""
    begin_session(conn; incoming_window=5000, outgoing_window=5000, timeout=60) -> Session
"""
function begin_session(c::Connection; incoming_window::Integer=5000, outgoing_window::Integer=5000,
    handle_max::Integer=255, timeout::Real=60.0)
    lock(c.lock)
    try
        c.state === :opened || throw(something(c.error, AMQPException(:connection, COND_CLOSED, "connection is not open")))
        ch = UInt16(0)
        while haskey(c.sessions, ch)
            ch == c.channel_max && throw(AMQPException(:connection, Symbol("amqp:resource-limit-exceeded"), "no free channels"))
            ch += UInt16(1)
        end
        s = Session(c, ch, nothing, :begin_sent, nothing, Threads.Condition(c.lock),
            UInt32(0), UInt32(incoming_window), UInt32(incoming_window), UInt32(outgoing_window),
            UInt32(0), UInt32(0), UInt32(0), UInt32(handle_max), UInt32(0),
            Dict{UInt32,Link}(), Dict{UInt32,Link}(), Dict{UInt32,Any}())
        c.sessions[ch] = s
        _send_performative!(c, Begin(next_outgoing_id=s.next_outgoing_id, incoming_window=s.incoming_window,
                outgoing_window=s.outgoing_window, handle_max=s.handle_max); channel=ch)
        ok = wait_until(() -> s.state !== :begin_sent, s.cond, deadline_after(timeout))
        if !ok
            delete!(c.sessions, ch)
            throw(timeout_error("session begin"))
        end
        s.state === :mapped || throw(something(s.error, AMQPException(:session, COND_CLOSED, "session could not be started")))
        return s
    finally
        unlock(c.lock)
    end
end

function _on_begin!(c::Connection, channel::UInt16, perf::Begin)
    perf.remote_channel === nothing &&
        throw(AMQPException(:protocol, COND_NOT_ALLOWED, "peer-initiated sessions are not supported"))
    s = get(c.sessions, perf.remote_channel, nothing)
    (s === nothing || s.state !== :begin_sent) &&
        throw(AMQPException(:protocol, COND_NOT_ALLOWED, "begin for unknown channel $(perf.remote_channel)"))
    s.remote_channel = channel
    c.remote_sessions[channel] = s
    s.next_incoming_id = perf.next_outgoing_id
    s.remote_incoming_window = perf.incoming_window
    s.remote_outgoing_window = perf.outgoing_window
    perf.handle_max !== nothing && (s.handle_max = min(s.handle_max, perf.handle_max))
    s.state = :mapped
    notify(s.cond)
end

function _on_end!(c::Connection, channel::UInt16, perf::End)
    s = get(c.remote_sessions, channel, nothing)
    s === nothing && return
    delete!(c.remote_sessions, channel)
    delete!(c.sessions, s.channel)
    if s.state !== :end_sent
        try
            _send_performative!(c, End(); channel=s.channel)
        catch
        end
    end
    err = perf.error === nothing ? AMQPException(:session, COND_CLOSED, "session ended") :
          AMQPException(:session, perf.error, "session ended by peer")
    _session_failed!(s, err)
end

function _session_failed!(s::Session, err::Exception)
    s.state = :ended
    s.error === nothing && (s.error = err)
    for l in values(s.links)
        _link_failed!(l, s.error)
    end
    for d in values(s.outgoing_unsettled)
        d.error = s.error
        notify(d.cond)
    end
    empty!(s.outgoing_unsettled)
    notify(s.cond)
end

"End a session (waits briefly for the peer's end)."
function end_session(s::Session; timeout::Real=10.0)
    c = s.conn::Connection
    lock(c.lock)
    try
        s.state === :mapped || return nothing
        s.state = :end_sent
        try
            _send_performative!(c, End(); channel=s.channel)
        catch
            return nothing
        end
        wait_until(() -> s.state === :ended, s.cond, deadline_after(timeout))
    finally
        unlock(c.lock)
    end
    return nothing
end

function _session_flow(s::Session; link=nothing, drain::Bool=false, echo::Bool=false)
    f = Flow(
        next_incoming_id=s.next_incoming_id,
        incoming_window=s.incoming_window,
        next_outgoing_id=s.next_outgoing_id,
        outgoing_window=s.outgoing_window,
        handle=link === nothing ? nothing : link.handle,
        delivery_count=link === nothing ? nothing : link.delivery_count,
        link_credit=link === nothing ? nothing : link.link_credit,
        available=link isa SenderLink ? UInt32(0) : nothing,
        drain=drain ? true : nothing,
        echo=echo ? true : nothing,
    )
    _send_performative!(s.conn, f; channel=s.channel)
end

function _on_flow!(s::Session, f::Flow)
    base = something(f.next_incoming_id, UInt32(0))
    s.remote_incoming_window = (base + f.incoming_window - s.next_outgoing_id) % UInt32
    s.remote_outgoing_window = f.outgoing_window
    notify(s.cond)
    if f.handle !== nothing
        l = get(s.remote_links, f.handle, nothing)
        l === nothing && return
        if l isa SenderLink
            dc = something(f.delivery_count, l.delivery_count)
            l.link_credit = f.link_credit === nothing ? UInt32(0) : (dc + f.link_credit - l.delivery_count) % UInt32
            if f.drain === true && !islocked(l.send_lock)
                l.delivery_count += l.link_credit
                l.link_credit = UInt32(0)
                _session_flow(s; link=l)
            elseif f.echo === true
                _session_flow(s; link=l)
            end
            notify(l.cond)
        else
            l = l::ReceiverLink
            l.available = something(f.available, UInt32(0))
            if l.draining && f.drain === true && f.delivery_count !== nothing
                # drain completed: the sender used up or returned all credit
                l.delivery_count = f.delivery_count
                l.link_credit = UInt32(0)
                l.draining = false
            elseif f.delivery_count !== nothing && serial_lt(l.delivery_count, f.delivery_count)
                # the sender advanced its delivery count without transfers; the
                # receiver's credit limit (delivery-count + link-credit) is unchanged
                limit = l.delivery_count + l.link_credit
                l.delivery_count = f.delivery_count
                l.link_credit = serial_lt(l.delivery_count, limit) ? (limit - l.delivery_count) % UInt32 : UInt32(0)
            end
            f.echo === true && _session_flow(s; link=l)
            notify(l.cond)
        end
    elseif f.echo === true
        _session_flow(s)
    end
end

# ---------------------------------------------------------------------------
# Links
# ---------------------------------------------------------------------------

function _alloc_handle(s::Session)
    h = UInt32(0)
    while haskey(s.links, h)
        h == s.handle_max && throw(AMQPException(:session, Symbol("amqp:resource-limit-exceeded"), "no free link handles"))
        h += UInt32(1)
    end
    return h
end

function _check_session(s::Session)
    s.state === :mapped || throw(something(s.error, AMQPException(:session, COND_CLOSED, "session is not active")))
end

function _await_attach!(l::Link, timeout::Real)
    s = l.session
    ok = wait_until(() -> l.state !== :attach_sent, l.cond, deadline_after(timeout))
    if !ok
        # give up on the link; detach it so the peer releases resources
        delete!(s.links, l.handle)
        l.state = :detached
        try
            _send_performative!(s.conn, Detach(handle=l.handle, closed=true); channel=s.channel)
        catch
        end
        throw(timeout_error("link attach ($(l.name))"))
    end
    l.state === :attached || throw(something(l.error, AMQPException(:link, COND_CLOSED, "link could not be attached")))
    # Azure responds to a failed attach with an attach carrying a null terminus
    # immediately followed by a detach with the actual error.
    ra = l.remote_attach
    terminus = l isa SenderLink ? ra.target : ra.source
    if terminus === nothing
        wait_until(() -> l.state !== :attached, l.cond, deadline_after(min(timeout, 10.0)))
        throw(something(l.error, AMQPException(:link, COND_NOT_FOUND, "peer refused link $(l.name)")))
    end
    return l
end

"""
    attach_sender(session, name, target; kwargs...) -> SenderLink

Attach a sending link. `target` is an address string or a [`Target`](@ref).
"""
function attach_sender(s::Session, name::AbstractString, target;
    source=nothing,
    snd_settle_mode::UInt8=SND_UNSETTLED,
    rcv_settle_mode::UInt8=RCV_FIRST,
    properties::Union{Nothing,Dict{Symbol,Any}}=nothing,
    desired_capabilities::Union{Nothing,Vector{Symbol}}=nothing,
    max_message_size::Union{Nothing,Integer}=nothing,
    timeout::Real=60.0)
    c = s.conn::Connection
    tgt = target isa Target ? target : Target(address=String(target))
    src = source === nothing ? Source(address=String(name)) : source isa Source ? source : Source(address=String(source))
    lock(c.lock)
    try
        _check_session(s)
        h = _alloc_handle(s)
        l = SenderLink(s, String(name), h, nothing, :attach_sent, nothing, Threads.Condition(c.lock),
            src, tgt, nothing, snd_settle_mode, rcv_settle_mode, UInt32(0), UInt32(0),
            ReentrantLock(), UInt64(0), UInt64(0))
        s.links[h] = l
        _send_performative!(c, Attach(name=l.name, handle=h, role=ROLE_SENDER,
                snd_settle_mode=snd_settle_mode, rcv_settle_mode=rcv_settle_mode,
                source=src, target=tgt, initial_delivery_count=UInt32(0),
                max_message_size=max_message_size === nothing ? nothing : UInt64(max_message_size),
                desired_capabilities=desired_capabilities, properties=properties); channel=s.channel)
        return _await_attach!(l, timeout)
    finally
        unlock(c.lock)
    end
end

"""
    attach_receiver(session, name, source; prefetch=0, kwargs...) -> ReceiverLink

Attach a receiving link. `source` is an address string or a [`Source`](@ref).
With `prefetch > 0` the link keeps that much credit outstanding automatically;
otherwise call [`flow!`](@ref) to grant credit.

`on_delivery(delivery)` (optional) is invoked on the connection's reader task
with the connection lock held for every completed delivery instead of queuing
it; it must not block.
"""
function attach_receiver(s::Session, name::AbstractString, source;
    target=nothing,
    snd_settle_mode::UInt8=SND_UNSETTLED,
    rcv_settle_mode::UInt8=RCV_FIRST,
    properties::Union{Nothing,Dict{Symbol,Any}}=nothing,
    desired_capabilities::Union{Nothing,Vector{Symbol}}=nothing,
    prefetch::Integer=0,
    on_delivery::Union{Nothing,Function}=nothing,
    max_message_size::Union{Nothing,Integer}=nothing,
    timeout::Real=60.0)
    c = s.conn::Connection
    src = source isa Source ? source : Source(address=String(source))
    tgt = target === nothing ? Target(address=String(name)) : target isa Target ? target : Target(address=String(target))
    lock(c.lock)
    try
        _check_session(s)
        h = _alloc_handle(s)
        l = ReceiverLink(s, String(name), h, nothing, :attach_sent, nothing, Threads.Condition(c.lock),
            src, tgt, nothing, snd_settle_mode, rcv_settle_mode, UInt32(0), UInt32(0), UInt32(0),
            false, Int(prefetch), IncomingDelivery[], nothing, Dict{UInt32,IncomingDelivery}(), on_delivery,
            max_message_size === nothing ? c.max_message_size : Int(max_message_size))
        s.links[h] = l
        _send_performative!(c, Attach(name=l.name, handle=h, role=ROLE_RECEIVER,
                snd_settle_mode=snd_settle_mode, rcv_settle_mode=rcv_settle_mode,
                source=src, target=tgt,
                max_message_size=max_message_size === nothing ? nothing : UInt64(max_message_size),
                desired_capabilities=desired_capabilities, properties=properties); channel=s.channel)
        _await_attach!(l, timeout)
        prefetch > 0 && _issue_credit!(l, UInt32(prefetch))
        return l
    finally
        unlock(c.lock)
    end
end

function _on_attach!(s::Session, a::Attach)
    # find our link by name
    l = nothing
    for cand in values(s.links)
        if cand.name == a.name && cand.state === :attach_sent
            l = cand
            break
        end
    end
    if l === nothing
        # peer-initiated link: refuse
        _send_performative!(s.conn, Attach(name=a.name, handle=_alloc_handle(s), role=!a.role); channel=s.channel)
        _send_performative!(s.conn, Detach(handle=a.handle, closed=true,
                error=AMQPError(condition=COND_NOT_ALLOWED, description="client does not accept links")); channel=s.channel)
        return
    end
    haskey(s.remote_links, a.handle) && throw(AMQPException(:protocol, Symbol("amqp:session:handle-in-use"), "handle $(a.handle) in use"))
    a.role == (l isa ReceiverLink) &&
        throw(AMQPException(:protocol, COND_NOT_ALLOWED, "peer attached link $(a.name) with the same role"))
    l.remote_handle = a.handle
    l.remote_attach = a
    s.remote_links[a.handle] = l
    if l isa ReceiverLink
        l.delivery_count = something(a.initial_delivery_count, UInt32(0))
        a.snd_settle_mode !== nothing && (l.snd_settle_mode = a.snd_settle_mode)
    else
        a.max_message_size !== nothing && (l.max_message_size = a.max_message_size)
        a.rcv_settle_mode !== nothing && (l.rcv_settle_mode = a.rcv_settle_mode)
    end
    l.state = :attached
    notify(l.cond)
end

function _on_detach!(s::Session, d::Detach)
    l = get(s.remote_links, d.handle, nothing)
    l === nothing && return
    delete!(s.remote_links, d.handle)
    delete!(s.links, l.handle)
    if l.state !== :detach_sent
        try
            _send_performative!(s.conn, Detach(handle=l.handle, closed=true); channel=s.channel)
        catch
        end
    end
    err = d.error === nothing ? AMQPException(:link, COND_DETACH_FORCED, "link detached by peer") :
          AMQPException(:link, d.error, "link detached by peer")
    _link_failed!(l, err)
end

function _link_failed!(l::Link, err::Exception)
    was_detaching = l.state === :detach_sent
    l.state = :detached
    if l.error === nothing && !was_detaching
        l.error = err
    end
    if l isa SenderLink
        # fail deliveries still awaiting settlement on this link
        s = l.session
        # (outgoing deliveries are keyed by delivery id at session level; a
        # detached link cannot receive dispositions any more)
        for (id, d) in collect(s.outgoing_unsettled)
            if d.link === l
                d.error = something(l.error, err)
                notify(d.cond)
                delete!(s.outgoing_unsettled, id)
            end
        end
    end
    notify(l.cond)
    notify(l.session.cond)
end

"""
    detach(link; closed=true, timeout=10)

Detach (close) a link and wait briefly for the peer's detach.
"""
function detach(l::Link; closed::Bool=true, timeout::Real=10.0, error::Union{Nothing,AMQPError}=nothing)
    s = l.session
    c = s.conn::Connection
    lock(c.lock)
    try
        l.state === :attached || return nothing
        l.state = :detach_sent
        try
            _send_performative!(c, Detach(handle=l.handle, closed=closed, error=error); channel=s.channel)
        catch
            l.state = :detached
            return nothing
        end
        wait_until(() -> l.state === :detached, l.cond, deadline_after(timeout))
        if l.state !== :detached
            l.state = :detached
            delete!(s.links, l.handle)
            l.remote_handle === nothing || delete!(s.remote_links, l.remote_handle)
        end
    finally
        unlock(c.lock)
    end
    return nothing
end

link_error(l::Link) = @lock l.session.conn.lock l.error
is_attached(l::Link) = @lock l.session.conn.lock (l.state === :attached && l.session.state === :mapped)

"Properties the peer sent in its attach frame (or `nothing`)."
remote_properties(l::Link) = l.remote_attach === nothing ? nothing : l.remote_attach.properties
remote_source(l::Link) = l.remote_attach === nothing ? nothing : l.remote_attach.source

# ---------------------------------------------------------------------------
# Sending
# ---------------------------------------------------------------------------

# OutgoingDelivery needs a back-reference to its link for detach handling.
mutable struct PendingSend
    id::UInt32
    link::SenderLink
    cond::Threads.Condition
    state::Any
    settled::Bool
    error::Union{Nothing,Exception}
end

function _link_usable(l::Link)
    l.state === :attached && l.session.state === :mapped && return nothing
    throw(something(l.error, l.session.error, (l.session.conn::Connection).error,
        AMQPException(:link, COND_DETACH_FORCED, "link $(l.name) is not attached")))
end

"""
    send_transfer(link, payload; message_format=0, settled=false, timeout=60) -> outcome

Transfer one message (already encoded) on `link`. Large payloads are split
across frames. Unless `settled` is true, waits for the receiver's outcome and
returns it (`Accepted`, `Rejected`, `Released`, `Modified`, or `nothing` if the
peer settled without an outcome).
"""
function send_transfer(l::SenderLink, payload::AbstractVector{UInt8};
    message_format::UInt32=UInt32(0), settled::Bool=false, timeout::Real=60.0)
    s = l.session
    c = s.conn::Connection
    deadline = deadline_after(timeout)
    if l.max_message_size > 0 && length(payload) > l.max_message_size
        throw(AMQPException(:link, COND_MSG_SIZE,
            "message of $(length(payload)) bytes exceeds the link's max message size of $(l.max_message_size) bytes"))
    end
    pending = nothing
    lock(l.send_lock)
    try
        lock(c.lock)
        try
            ok = wait_until(() -> l.state !== :attached || s.state !== :mapped || l.link_credit > 0, l.cond, deadline)
            _link_usable(l)
            ok || throw(timeout_error("link credit on $(l.name)"))
            id = s.next_delivery_id
            s.next_delivery_id += UInt32(1)
            l.next_tag += UInt64(1)
            tag = reinterpret(UInt8, [hton(l.next_tag)])
            l.delivery_count += UInt32(1)
            l.link_credit -= UInt32(1)
            if !settled
                pending = PendingSend(id, l, Threads.Condition(c.lock), nothing, false, nothing)
                s.outgoing_unsettled[id] = pending
            end
            first_perf = Transfer(handle=l.handle, delivery_id=id, delivery_tag=Vector{UInt8}(tag),
                message_format=message_format, settled=settled, more=true)
            overhead = length(encode(first_perf)) + FRAME_HEADER_SIZE
            n = length(payload)
            if overhead + n <= c.max_frame_size
                # common case: single frame
                _await_session_window!(s, l, deadline)
                perf = Transfer(handle=l.handle, delivery_id=id, delivery_tag=Vector{UInt8}(tag),
                    message_format=message_format, settled=settled)
                _emit_transfer!(s, perf, payload)
            else
                chunk = Int(c.max_frame_size) - overhead
                pos = firstindex(payload)
                last_index = lastindex(payload)
                first = true
                while pos <= last_index
                    stop = min(pos + chunk - 1, last_index)
                    more = stop < last_index
                    perf = first ? Transfer(handle=l.handle, delivery_id=id, delivery_tag=Vector{UInt8}(tag),
                                            message_format=message_format, settled=settled, more=more) :
                                   Transfer(handle=l.handle, more=more)
                    _await_session_window!(s, l, deadline)
                    _emit_transfer!(s, perf, view(payload, pos:stop))
                    pos = stop + 1
                    first = false
                end
            end
        catch
            pending === nothing || delete!(s.outgoing_unsettled, pending.id)
            rethrow()
        finally
            unlock(c.lock)
        end
    finally
        unlock(l.send_lock)
    end
    pending === nothing && return nothing
    lock(c.lock)
    try
        ok = wait_until(() -> pending.settled || pending.error !== nothing, pending.cond, deadline)
        if !ok
            delete!(s.outgoing_unsettled, pending.id)
            throw(timeout_error("delivery outcome on $(l.name)"))
        end
        pending.error === nothing || throw(pending.error)
        return pending.state
    finally
        unlock(c.lock)
    end
end

function _await_session_window!(s::Session, l::Link, deadline::UInt64)
    ok = wait_until(() -> s.state !== :mapped || l.state !== :attached || s.remote_incoming_window > 0, s.cond, deadline)
    _link_usable(l)
    ok || throw(timeout_error("session window"))
end

function _emit_transfer!(s::Session, perf::Transfer, payload)
    s.next_outgoing_id += UInt32(1)
    s.remote_incoming_window -= UInt32(1)
    _send_performative!(s.conn, perf; channel=s.channel, payload=payload)
end

function _on_disposition!(s::Session, d::Disposition)
    last = something(d.last, d.first)
    if d.role == ROLE_RECEIVER
        # outcome for deliveries we sent
        span = (last - d.first) % UInt32
        ids = span < length(s.outgoing_unsettled) ? (d.first + i for i in UInt32(0):span) :
              [id for id in keys(s.outgoing_unsettled) if serial_le(d.first, id) && serial_le(id, last)]
        for id in ids
            p = get(s.outgoing_unsettled, id % UInt32, nothing)
            p === nothing && continue
            p.state = d.state
            if d.settled === true || d.state isa Outcome
                if d.settled !== true
                    # peer awaits our settlement (rcv-settle-mode second)
                    _send_performative!(s.conn, Disposition(role=ROLE_SENDER, first=p.id, settled=true, state=d.state);
                        channel=s.channel)
                end
                p.settled = true
                delete!(s.outgoing_unsettled, p.id)
                notify(p.cond)
            end
        end
    else
        # the sender reacting to our disposition of a received delivery
        for l in values(s.links)
            l isa ReceiverLink || continue
            isempty(l.unsettled) && continue
            span = (last - d.first) % UInt32
            ids = span < length(l.unsettled) ? (d.first + i for i in UInt32(0):span) :
                  [id for id in keys(l.unsettled) if serial_le(d.first, id) && serial_le(id, last)]
            for id in ids
                del = get(l.unsettled, id % UInt32, nothing)
                del === nothing && continue
                del.remote_state = d.state
                if d.settled === true
                    del.remote_settled = true
                    del.settled = true
                    delete!(l.unsettled, del.delivery_id)
                end
            end
            notify(l.cond)
        end
    end
end

# ---------------------------------------------------------------------------
# Receiving
# ---------------------------------------------------------------------------

function _on_transfer!(s::Session, t::Transfer, payload)
    c = s.conn::Connection
    s.next_incoming_id += UInt32(1)
    s.remote_outgoing_window -= UInt32(1)
    if s.incoming_window == 0
        throw(AMQPException(:protocol, Symbol("amqp:session:window-violation"), "peer exceeded the session incoming window"))
    end
    s.incoming_window -= UInt32(1)
    if s.incoming_window <= s.incoming_window_max ÷ 2
        s.incoming_window = s.incoming_window_max
        _session_flow(s)
    end
    l = get(s.remote_links, t.handle, nothing)
    l isa ReceiverLink || throw(AMQPException(:protocol, Symbol("amqp:session:unattached-handle"),
        "transfer on unknown handle $(t.handle)"))
    d = l.partial
    if d === nothing
        t.delivery_id === nothing && throw(AMQPException(:protocol, COND_NOT_ALLOWED, "first transfer without delivery-id"))
        l.link_credit == 0 && !l.draining && @debug "peer sent a transfer without link credit" link = l.name
        d = IncomingDelivery(l, t.delivery_id, something(t.delivery_tag, UInt8[]),
            something(t.message_format, UInt32(0)),
            something(t.settled, false) || l.snd_settle_mode == SND_SETTLED,
            t.state, false, UInt8[], false)
        l.partial = d
    end
    if payload !== nothing
        length(d.payload) + length(payload) > l.max_message_size &&
            throw(AMQPException(:protocol, COND_MSG_SIZE, "incoming message exceeds $(l.max_message_size) bytes"))
        append!(d.payload, payload)
    end
    t.settled === true && (d.settled = true)
    if t.aborted === true
        l.partial = nothing
        l.delivery_count += UInt32(1)
        l.link_credit > 0 && (l.link_credit -= UInt32(1))
        return
    end
    t.more === true && return
    l.partial = nothing
    l.delivery_count += UInt32(1)
    l.link_credit > 0 && (l.link_credit -= UInt32(1))
    d.settled || (l.unsettled[d.delivery_id] = d)
    if l.on_delivery !== nothing
        l.on_delivery(d)
        # callback links never go through receive_delivery: replenish credit here
        if l.prefetch > 0 && l.state === :attached && l.link_credit <= l.prefetch ÷ 2
            _issue_credit!(l, UInt32(l.prefetch))
        end
    else
        push!(l.queue, d)
    end
    notify(l.cond)
end

function _issue_credit!(l::ReceiverLink, credit::UInt32; drain::Bool=false)
    l.link_credit = credit
    drain && (l.draining = true)
    _session_flow(l.session; link=l, drain=drain)
end

"""
    flow!(link::ReceiverLink, credit; drain=false)

Set the link credit (the number of further deliveries the sender may send).
"""
function flow!(l::ReceiverLink, credit::Integer; drain::Bool=false)
    c = l.session.conn::Connection
    @lock c.lock begin
        _link_usable(l)
        _issue_credit!(l, UInt32(credit); drain=drain)
    end
    return nothing
end

"""
    drain!(link::ReceiverLink; timeout=10) -> Bool

Ask the sender to use up or give back all outstanding credit and wait until it
has. Deliveries sent meanwhile are queued. Returns `false` on timeout; the
drain then stays pending (see [`drain_pending!`](@ref)) and a later call waits
for it without asking again.
"""
function drain!(l::ReceiverLink; timeout::Real=10.0)
    c = l.session.conn::Connection
    lock(c.lock)
    try
        _link_usable(l)
        if l.link_credit == 0
            l.draining = false          # nothing more can be delivered
            return true
        end
        l.draining || _issue_credit!(l, l.link_credit; drain=true)   # else wait for the pending drain
        return wait_until(() -> !l.draining || l.state !== :attached, l.cond, deadline_after(timeout))
    finally
        unlock(c.lock)
    end
end

"""
    drain_pending!(link::ReceiverLink) -> Bool

Whether a drain requested with [`drain!`](@ref) is still unanswered while the
sender holds credit. Issuing credit then would cancel the drain (a flow
without `drain` ends drain mode), so callers wait for deliveries or the
sender's reply instead. With no credit left the sender cannot deliver
anything more, so the drain is treated as complete.
"""
function drain_pending!(l::ReceiverLink)
    @lock l.session.conn.lock begin
        l.draining && l.link_credit == 0 && (l.draining = false)
        l.draining
    end
end

function _topup_credit!(l::ReceiverLink)
    l.prefetch > 0 || return
    l.state === :attached || return
    desired = l.prefetch - length(l.queue)
    desired <= 0 && return
    if desired > l.link_credit && (l.link_credit == 0 || desired - l.link_credit >= max(1, l.prefetch ÷ 2))
        _issue_credit!(l, UInt32(desired))
    end
end

"""
    receive_delivery(link; timeout=60) -> Union{IncomingDelivery,Nothing}

Take the next delivery from the link's queue, waiting up to `timeout`
seconds (`nothing` on timeout). Throws if the link fails while waiting.
"""
function receive_delivery(l::ReceiverLink; timeout::Real=60.0)
    c = l.session.conn::Connection
    lock(c.lock)
    try
        wait_until(() -> !isempty(l.queue) || l.state !== :attached || l.session.state !== :mapped,
            l.cond, deadline_after(timeout))
        if !isempty(l.queue)
            d = popfirst!(l.queue)
            _topup_credit!(l)
            return d
        end
        l.state === :attached && l.session.state === :mapped && return nothing
        _link_usable(l)
        return nothing
    finally
        unlock(c.lock)
    end
end

"Take all currently queued deliveries without waiting."
function take_queued!(l::ReceiverLink, max::Integer=typemax(Int))
    c = l.session.conn::Connection
    @lock c.lock begin
        n = min(max, length(l.queue))
        ds = l.queue[1:n]
        deleteat!(l.queue, 1:n)
        n > 0 && _topup_credit!(l)
        ds
    end
end

queued_count(l::ReceiverLink) = @lock l.session.conn.lock length(l.queue)
link_credit(l::Link) = @lock l.session.conn.lock l.link_credit

"""
    settle!(delivery, outcome; wait=false, timeout=60) -> remote state

Settle a received delivery with `outcome` (`Accepted()`, `Rejected(...)`,
`Released()`, `Modified(...)`). With `wait=true` the disposition is sent
unsettled and the call waits for the peer to confirm, returning the peer's
state; peer-reported failures (e.g. an expired lock) surface as the returned
`Rejected` state.
"""
function settle!(d::IncomingDelivery, outcome; wait::Bool=false, timeout::Real=60.0)
    l = d.link::ReceiverLink
    s = l.session
    c = s.conn::Connection
    lock(c.lock)
    try
        d.settled && throw(AMQPException(:delivery, Symbol("amqp:illegal-state"), "delivery is already settled"))
        _link_usable(l)
        if !wait
            d.settled = true
            delete!(l.unsettled, d.delivery_id)
            _send_performative!(c, Disposition(role=ROLE_RECEIVER, first=d.delivery_id, settled=true, state=outcome);
                channel=s.channel)
            return outcome
        end
        d.waiting = true
        _send_performative!(c, Disposition(role=ROLE_RECEIVER, first=d.delivery_id, settled=false, state=outcome);
            channel=s.channel)
        ok = wait_until(() -> d.remote_settled || d.remote_state !== nothing || l.state !== :attached, l.cond,
            deadline_after(timeout))
        d.waiting = false
        if d.remote_settled || d.remote_state !== nothing
            if !d.remote_settled
                # peer sent its outcome without settling: settle now
                _send_performative!(c, Disposition(role=ROLE_RECEIVER, first=d.delivery_id, settled=true,
                        state=something(d.remote_state, outcome)); channel=s.channel)
                delete!(l.unsettled, d.delivery_id)
            end
            d.settled = true
            return d.remote_state
        end
        ok || throw(timeout_error("settlement confirmation"))
        _link_usable(l)
        return nothing
    finally
        unlock(c.lock)
    end
end

"Decode the message carried by a delivery."
decode_delivery(d::IncomingDelivery) = decode_message(d.payload)
