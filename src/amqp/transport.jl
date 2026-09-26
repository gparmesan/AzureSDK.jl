# Byte-stream transports carrying AMQP frames: plain TCP, TLS, and AMQP over
# WebSockets (subprotocol AMQPWSB10, as used by Azure Event Hubs/Service Bus).

abstract type AMQPTransport end

"""
    read_some!(t::AMQPTransport, buf::Vector{UInt8}) -> Int

Read at least one and at most `length(buf)` bytes; throw `EOFError` on end of
stream.
"""
function read_some! end

"""
    write_all(t::AMQPTransport, bytes::Vector{UInt8})

Write all of `bytes` to the transport.
"""
function write_all end

"TCP or TLS stream provided by Reseau."
mutable struct SocketTransport{C} <: AMQPTransport
    conn::C
    description::String
end

function read_some!(t::SocketTransport, buf::Vector{UInt8})
    n = readbytes!(t.conn, buf, length(buf); all=false)
    n == 0 && throw(EOFError())
    return n
end
write_all(t::SocketTransport, bytes::Vector{UInt8}) = (write(t.conn, bytes); nothing)
Base.close(t::SocketTransport) = (try close(t.conn) catch end; nothing)
Base.isopen(t::SocketTransport) = isopen(t.conn)
Base.show(io::IO, t::SocketTransport) = print(io, "SocketTransport(", t.description, ")")

"AMQP stream tunnelled through binary WebSocket messages."
mutable struct WebSocketTransport <: AMQPTransport
    ws::Any                 # HTTP.WebSockets.WebSocket
    pending::Vector{UInt8}
    pos::Int
    description::String
end

function read_some!(t::WebSocketTransport, buf::Vector{UInt8})
    while t.pos > length(t.pending)
        msg = try
            HTTP.WebSockets.receive(t.ws)
        catch err
            err isa HTTP.WebSockets.WebSocketError && throw(EOFError())
            rethrow()
        end
        msg isa AbstractVector{UInt8} || throw(AMQPDecodeError("unexpected text message on AMQP WebSocket"))
        t.pending = msg
        t.pos = 1
    end
    n = min(length(buf), length(t.pending) - t.pos + 1)
    copyto!(buf, 1, t.pending, t.pos, n)
    t.pos += n
    return n
end
write_all(t::WebSocketTransport, bytes::Vector{UInt8}) = (HTTP.WebSockets.send(t.ws, bytes); nothing)
Base.close(t::WebSocketTransport) = (try close(t.ws) catch end; nothing)
Base.isopen(t::WebSocketTransport) = !HTTP.WebSockets.isclosed(t.ws)
Base.show(io::IO, t::WebSocketTransport) = print(io, "WebSocketTransport(", t.description, ")")

"""
Buffered reader on top of a transport, used by the connection's reader task.
"""
mutable struct TransportReader
    transport::AMQPTransport
    buf::Vector{UInt8}
    start::Int
    stop::Int
end
TransportReader(t::AMQPTransport; bufsize::Int=64 * 1024) = TransportReader(t, Vector{UInt8}(undef, bufsize), 1, 0)

"Read exactly `n` bytes into `dest[1:n]`."
function read_exact!(r::TransportReader, dest::Vector{UInt8}, n::Int=length(dest))
    filled = 0
    while filled < n
        avail = r.stop - r.start + 1
        if avail > 0
            k = min(avail, n - filled)
            copyto!(dest, filled + 1, r.buf, r.start, k)
            r.start += k
            filled += k
        elseif n - filled >= length(r.buf)
            # large read: bypass the buffer
            tmp = Vector{UInt8}(undef, n - filled)
            k = read_some!(r.transport, tmp)
            copyto!(dest, filled + 1, tmp, 1, k)
            filled += k
        else
            k = read_some!(r.transport, r.buf)
            r.start = 1
            r.stop = k
        end
    end
    return dest
end

read_exact(r::TransportReader, n::Int) = read_exact!(r, Vector{UInt8}(undef, n), n)

"""
    TransportConfig

How to reach an AMQP endpoint.

- `scheme`: `:amqps` (TLS, default port 5671), `:amqp` (plain TCP, port 5672;
  only for local emulators/tests), `:wss` (AMQP over WebSockets, port 443) or
  `:loopback` (in-process test peer, see `open_loopback`).
- `verify_tls`: verify the server certificate and host name (never disable in
  production).
- `ca_file`: optional custom CA bundle.
- `http_proxy`: WebSocket transport only; `NamedTuple` / `Dict` with
  `proxy_hostname`, `proxy_port`, and optionally `username` / `password`
  (same shape as the Python SDK), or a proxy URL string.
- `custom_endpoint_address`: connect to this host/port instead of the
  namespace host (e.g. through an application gateway), still using the
  namespace host name for AMQP and TLS SNI.
"""
Base.@kwdef struct TransportConfig
    scheme::Symbol = :amqps
    host::String
    port::Union{Nothing,Int} = nothing
    verify_tls::Bool = true
    ca_file::Union{Nothing,String} = nothing
    connect_timeout::Float64 = 60.0
    http_proxy::Any = nothing
    custom_endpoint_address::Union{Nothing,String} = nothing
    websocket_path::String = "/\$servicebus/websocket"
end

default_port(scheme::Symbol) = scheme === :amqps ? 5671 : scheme === :amqp ? 5672 : 443

function _endpoint(cfg::TransportConfig)
    host = cfg.host
    port = something(cfg.port, default_port(cfg.scheme))
    if cfg.custom_endpoint_address !== nothing
        u = URIs.URI(occursin("://", cfg.custom_endpoint_address) ? cfg.custom_endpoint_address :
                     "sb://" * cfg.custom_endpoint_address)
        host = u.host
        isempty(u.port) || (port = parse(Int, u.port))
    end
    return host, port
end

_hostport(host::AbstractString, port::Integer) = occursin(':', host) ? "[$host]:$port" : "$host:$port"

function _proxy_config(p)
    p === nothing && return nothing
    if p isa AbstractString
        return HTTP.ProxyURL(String(p))
    end
    get_(k) = p isa AbstractDict ? get(p, k, get(p, String(k), nothing)) : get(p, k, nothing)
    host = get_(:proxy_hostname)
    host === nothing && throw(ArgumentError("http_proxy requires proxy_hostname"))
    port = something(get_(:proxy_port), 3128)
    user = get_(:username)
    pass = get_(:password)
    auth = user === nothing ? "" : string(URIs.escapeuri(String(user)), ":", URIs.escapeuri(String(something(pass, ""))), "@")
    return HTTP.ProxyURL("http://$(auth)$(host):$(port)")
end

"""
    open_transport(cfg::TransportConfig) -> AMQPTransport
"""
function open_transport(cfg::TransportConfig)
    cfg.scheme === :loopback && return open_loopback()
    host, port = _endpoint(cfg)
    timeout_ns = round(Int64, cfg.connect_timeout * 1e9)
    if cfg.scheme === :amqp
        conn = Reseau.TCP.connect(_hostport(host, port); timeout_ns=timeout_ns)
        _tune_tcp!(conn)
        return SocketTransport(conn, "amqp://$(_hostport(host, port))")
    elseif cfg.scheme === :amqps
        conn = Reseau.TLS.connect(_hostport(host, port);
            timeout_ns=timeout_ns,
            server_name=cfg.host,
            verify_peer=cfg.verify_tls,
            ca_file=cfg.ca_file,
            handshake_timeout_ns=timeout_ns)
        _tune_tcp!(Reseau.TLS.net_conn(conn))
        return SocketTransport(conn, "amqps://$(_hostport(host, port))")
    elseif cfg.scheme === :wss || cfg.scheme === :ws
        proto = cfg.scheme === :wss ? "wss" : "ws"
        url = string(proto, "://", _hostport(host, port), cfg.websocket_path)
        kwargs = Dict{Symbol,Any}(
            :subprotocols => ["AMQPWSB10"],
            :connect_timeout => cfg.connect_timeout,
            :require_ssl_verification => cfg.verify_tls,
            :maxframesize => 64 * 1024 * 1024,
            :redirect => false,
            :cookies => false,
        )
        proxy = _proxy_config(cfg.http_proxy)
        proxy !== nothing && (kwargs[:proxy] = proxy)
        if cfg.custom_endpoint_address !== nothing
            kwargs[:headers] = ["Host" => cfg.host]
        end
        ws = HTTP.WebSockets.open(url; kwargs...)
        return WebSocketTransport(ws, UInt8[], 1, url)
    else
        throw(ArgumentError("unsupported AMQP transport scheme :$(cfg.scheme)"))
    end
end

function _tune_tcp!(conn)
    try
        Reseau.TCP.set_nodelay!(conn, true)
        Reseau.TCP.set_keepalive!(conn, true; idle_secs=30, interval_secs=10, count=6)
    catch err
        @debug "could not tune TCP socket options" exception = err
    end
    return nothing
end
