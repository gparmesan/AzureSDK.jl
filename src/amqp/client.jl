# A lazily (re)connecting AMQP connection shared by the links of one service
# client, with claims-based-security (CBS) authorization and automatic token
# refresh.

"""
    TokenProvider

Callable `audience -> (token::String, token_type::String, expires_on::Float64)`
used to authorize an audience on a connection through `\$cbs`. `expires_on`
is a Unix timestamp in seconds. `token_type` is `"jwt"` for Entra ID tokens or
`"servicebus.windows.net:sastoken"` for shared access signatures.
"""
const TokenProvider = Union{Nothing,Function}

mutable struct ClientConnection
    transport_config::TransportConfig
    hostname::String
    sasl_mechanism::Symbol
    sasl_username::Union{Nothing,String}
    sasl_password::Union{Nothing,String}
    properties::Dict{Symbol,Any}
    idle_timeout::Float64
    token_provider::TokenProvider
    operation_timeout::Float64
    lock::ReentrantLock
    cond::Threads.Condition
    conn::Union{Nothing,Connection}
    session::Union{Nothing,Session}
    cbs::Union{Nothing,ManagementLink}
    authorized::Dict{String,Float64}    # audience => refresh at (unix seconds)
    refresher::Union{Nothing,Task}
    closed::Bool
    generation::Int
end

function ClientConnection(tc::TransportConfig; hostname::AbstractString=tc.host,
    sasl_mechanism::Symbol=:anonymous, sasl_username=nothing, sasl_password=nothing,
    properties::Dict{Symbol,Any}=Dict{Symbol,Any}(), idle_timeout::Real=0.0,
    token_provider::TokenProvider=nothing, operation_timeout::Real=60.0)
    lk = ReentrantLock()
    return ClientConnection(tc, String(hostname), sasl_mechanism, sasl_username, sasl_password, properties,
        Float64(idle_timeout), token_provider, Float64(operation_timeout), lk, Threads.Condition(lk), nothing, nothing,
        nothing, Dict{String,Float64}(), nothing, false, 0)
end

function Base.show(io::IO, cc::ClientConnection)
    print(io, "ClientConnection(", cc.hostname, ", ", cc.transport_config.scheme, ", generation=", cc.generation,
        cc.closed ? ", closed" : "", ")")
end

struct ClientClosedError <: Exception
    msg::String
end
Base.showerror(io::IO, e::ClientClosedError) = print(io, "ClientClosedError: ", e.msg)

function _alive(cc::ClientConnection)
    cc.conn === nothing && return false
    c = cc.conn
    return @lock c.lock (c.state === :opened && cc.session !== nothing && cc.session.state === :mapped)
end

"""
    acquire!(cc) -> (connection, session, generation)

Return the live connection and session, reconnecting if necessary. The
generation number changes on every reconnect so callers can detect that their
links belong to a dead connection.
"""
function acquire!(cc::ClientConnection)
    lock(cc.lock)
    try
        cc.closed && throw(ClientClosedError("the client has been closed"))
        _alive(cc) && return cc.conn, cc.session, cc.generation
        if cc.conn !== nothing
            old = cc.conn
            cc.conn = nothing
            cc.session = nothing
            @async try close(old; timeout=1.0) catch end
        end
        cfg = ConnectionConfig(hostname=cc.hostname, sasl_mechanism=cc.sasl_mechanism,
            sasl_username=cc.sasl_username, sasl_password=cc.sasl_password,
            properties=cc.properties, idle_timeout=cc.idle_timeout, open_timeout=cc.operation_timeout)
        conn = connect(cc.transport_config, cfg)
        session = try
            begin_session(conn; timeout=cc.operation_timeout)
        catch
            try close(conn; timeout=1.0) catch end
            rethrow()
        end
        cc.conn = conn
        cc.session = session
        cc.cbs = nothing
        empty!(cc.authorized)
        cc.generation += 1
        return conn, session, cc.generation
    finally
        unlock(cc.lock)
    end
end

_refresh_at(expires_on::Float64, now::Float64=time()) =
    now + max(0.5 * (expires_on - now), (expires_on - now) - 300.0, 1.0)

"""
    authorize!(cc, audience)

Make sure `audience` is authorized on the current connection (no-op without a
token provider).
"""
function authorize!(cc::ClientConnection, audience::AbstractString)
    cc.token_provider === nothing && return nothing
    lock(cc.lock)
    try
        _, session, _ = acquire!(cc)
        haskey(cc.authorized, audience) && return nothing
        _put_token_locked!(cc, session, String(audience))
        _ensure_refresher!(cc)
    finally
        unlock(cc.lock)
    end
    return nothing
end

function _put_token_locked!(cc::ClientConnection, session::Session, audience::String)
    if cc.cbs === nothing || !is_open(cc.cbs)
        cc.cbs = open_management_link(session, "\$cbs"; timeout=cc.operation_timeout, reply_to="cbs")
    end
    token, token_type, expires_on = cc.token_provider(audience)
    put_token(cc.cbs, audience, token; token_type=token_type, expires_on=expires_on, timeout=cc.operation_timeout)
    cc.authorized[audience] = _refresh_at(Float64(expires_on))
    return nothing
end

function _ensure_refresher!(cc::ClientConnection)
    (cc.refresher === nothing || istaskdone(cc.refresher)) || return
    cc.refresher = Threads.@spawn _refresh_loop(cc)
    return
end

function _refresh_loop(cc::ClientConnection)
    while true
        lock(cc.lock)
        try
            cc.closed && return
            next = isempty(cc.authorized) ? time() + 30.0 : minimum(values(cc.authorized))
            # sleep until the next refresh is due, waking immediately on close
            wait_until(() -> cc.closed, cc.cond, deadline_after(clamp(next - time(), 0.5, 30.0)))
            cc.closed && return
            _alive(cc) || continue   # reconnect re-authorizes lazily
            now = time()
            for (audience, due) in collect(cc.authorized)
                due > now && continue
                try
                    _put_token_locked!(cc, cc.session, audience)
                    @debug "refreshed CBS token" audience
                catch err
                    @warn "failed to refresh CBS token; retrying in 10 s" audience exception = err
                    cc.authorized[audience] = now + 10.0
                end
            end
        catch err
            @debug "CBS refresh loop error" exception = err
        finally
            unlock(cc.lock)
        end
    end
end

"Management link to `address` on the current connection (cached per generation by callers)."
function open_management(cc::ClientConnection, address::AbstractString; audience::Union{Nothing,AbstractString}=nothing)
    audience === nothing || authorize!(cc, audience)
    _, session, gen = acquire!(cc)
    return open_management_link(session, address; timeout=cc.operation_timeout), gen
end

function Base.close(cc::ClientConnection)
    conn = @lock cc.lock begin
        cc.closed && return nothing
        cc.closed = true
        notify(cc.cond)
        c = cc.conn
        cc.conn = nothing
        cc.session = nothing
        cc.cbs = nothing
        empty!(cc.authorized)
        c
    end
    conn === nothing || try close(conn; timeout=5.0) catch end
    return nothing
end

Base.isopen(cc::ClientConnection) = !cc.closed
