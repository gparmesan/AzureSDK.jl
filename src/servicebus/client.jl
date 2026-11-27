# ServiceBusClient and shared handler plumbing.

const SERVICEBUS_SCOPE = "https://servicebus.azure.net/.default"

"""
    ServiceBusReceiveMode.PEEK_LOCK / ServiceBusReceiveMode.RECEIVE_AND_DELETE
"""
module ServiceBusReceiveMode
    @enum T PEEK_LOCK RECEIVE_AND_DELETE
end

"""
    ServiceBusSubQueue.DEAD_LETTER / ServiceBusSubQueue.TRANSFER_DEAD_LETTER
"""
module ServiceBusSubQueue
    @enum T DEAD_LETTER TRANSFER_DEAD_LETTER
end

module ServiceBusSessionFilter
    @enum T NEXT_AVAILABLE
end

"Pass as `session_id` to receive from the next available session."
const NEXT_AVAILABLE_SESSION = ServiceBusSessionFilter.NEXT_AVAILABLE

"""
    ServiceBusConnectionStringProperties

Result of [`parse_connection_string`](@ref): `fully_qualified_namespace`,
`endpoint`, `entity_path`, `shared_access_key_name`, `shared_access_key`,
`shared_access_signature`.
"""
struct ServiceBusConnectionStringProperties
    fully_qualified_namespace::String
    endpoint::String
    entity_path::Union{Nothing, String}
    shared_access_key_name::Union{Nothing, String}
    shared_access_key::Union{Nothing, String}
    shared_access_signature::Union{Nothing, String}
end
Base.show(io::IO, p::ServiceBusConnectionStringProperties) =
    print(
    io, "ServiceBusConnectionStringProperties(fully_qualified_namespace=", repr(p.fully_qualified_namespace),
    ", entity_path=", repr(p.entity_path), ", shared_access_key_name=", repr(p.shared_access_key_name), ")"
)

"""
    parse_connection_string(conn_str) -> ServiceBusConnectionStringProperties
"""
function parse_connection_string(conn_str::AbstractString)
    p = Messaging.parse_messaging_connection_string(conn_str)
    return ServiceBusConnectionStringProperties(
        p.fully_qualified_namespace, p.endpoint, p.entity_path,
        p.shared_access_key_name, p.shared_access_key, p.shared_access_signature
    )
end

"""
    ServiceBusClient(fully_qualified_namespace, credential; kwargs...)
    ServiceBusClient(f::Function, args...; kwargs...)   # do-block: closes the client afterwards

Entry point for Service Bus (`azure.servicebus.ServiceBusClient`). Create
senders and receivers with [`get_queue_sender`](@ref),
[`get_queue_receiver`](@ref), [`get_topic_sender`](@ref) and
[`get_subscription_receiver`](@ref). All handlers created from one client
share a single connection.

Keyword arguments: `transport_type` (`TransportType.Amqp`,
`TransportType.AmqpOverWebsocket` or `TransportType.Http`), `http_proxy`,
`custom_endpoint_address`, `connection_verify`, `retry_total` (3),
`retry_backoff_factor` (0.8), `retry_backoff_max` (120), `retry_mode`,
`auth_timeout` (60 s), `user_agent`.
"""
mutable struct ServiceBusClient
    config::ClientConfig
    conn::Union{Nothing, ClientConnection}
    entity_name::Union{Nothing, String}
    lock::ReentrantLock
    handlers::Vector{Any}
    closed::Bool
end

function ServiceBusClient(
        fully_qualified_namespace::AbstractString, credential; use_development_emulator::Bool = false,
        _entity_name = nothing, kwargs...
    )
    cfg = ClientConfig(
        fully_qualified_namespace, credential, SERVICEBUS_SCOPE, "servicebus";
        use_development_emulator = use_development_emulator, kwargs...
    )
    conn = cfg.transport_type == TransportType.Http ? nothing : Messaging.make_connection(cfg)
    return ServiceBusClient(cfg, conn, _entity_name, ReentrantLock(), Any[], false)
end

function ServiceBusClient(f::Function, args...; kwargs...)
    client = ServiceBusClient(args...; kwargs...)
    try
        return f(client)
    finally
        close(client)
    end
end

"""
    from_connection_string(ServiceBusClient, conn_str; kwargs...)

Create a client from a Service Bus connection string (including the local
emulator's `UseDevelopmentEmulator=true` strings).
"""
function from_connection_string(::Type{ServiceBusClient}, conn_str::AbstractString; kwargs...)
    p = Messaging.parse_messaging_connection_string(conn_str)
    host = p.port === nothing ? p.fully_qualified_namespace : "$(p.fully_qualified_namespace):$(p.port)"
    return ServiceBusClient(
        host, Messaging.connection_string_credential(p);
        use_development_emulator = p.use_development_emulator, _entity_name = p.entity_path, kwargs...
    )
end
function from_connection_string(f::Function, T::Type{ServiceBusClient}, conn_str::AbstractString; kwargs...)
    client = from_connection_string(T, conn_str; kwargs...)
    try
        return f(client)
    finally
        close(client)
    end
end

Base.show(io::IO, c::ServiceBusClient) = print(
    io, "ServiceBusClient(", c.config.fully_qualified_namespace, ", ",
    c.config.transport_type, c.closed ? ", closed" : "", ")"
)

"The namespace host name."
fully_qualified_namespace(c::ServiceBusClient) = c.config.fully_qualified_namespace

function _check_entity(c::ServiceBusClient, name::AbstractString)
    (isempty(name) || occursin(r"[\s?#]", name) || startswith(name, "/") || occursin("\$", name)) &&
        throw(ArgumentError("invalid entity name $(repr(name))"))
    if c.entity_name !== nothing && c.entity_name != name && !startswith(c.entity_name, name * "/")
        throw(ArgumentError("The queue or topic name provided does not match the EntityPath in the connection string passed to the ServiceBusClient constructor"))
    end
    return String(name)
end

function _register!(c::ServiceBusClient, h)
    @lock c.lock begin
        c.closed && throw(ServiceBusClientClosedError("The ServiceBusClient has been closed."))
        filter!(x -> !x.closed, c.handlers)
        push!(c.handlers, h)
    end
    return h
end

"""
    close(client::ServiceBusClient)

Close all senders and receivers created by the client, then its connection.
"""
function Base.close(c::ServiceBusClient)
    handlers = @lock c.lock begin
        c.closed && return nothing
        c.closed = true
        copy(c.handlers)
    end
    for h in handlers
        try
            close(h)
        catch err
            @debug "error closing handler" exception = err
        end
    end
    c.conn === nothing || close(c.conn)
    return nothing
end
Base.isopen(c::ServiceBusClient) = !c.closed

# ---------------------------------------------------------------------------
# Handler base: retries and management requests
# ---------------------------------------------------------------------------

abstract type ServiceBusHandler end

_check_open(h::ServiceBusHandler) = (h.closed || h.client.closed) &&
    throw(ServiceBusClientClosedError("The handler has been closed."))

function _run(f, h::ServiceBusHandler, op::AbstractString; timeout = nothing, retry::Bool = true)
    _check_open(h)
    deadline = timeout === nothing ? nothing : time() + timeout
    policy = retry ? h.client.config.retry : RetryOptions(retry_total = 0)
    return Messaging.run_with_retry(
        f, policy, () -> h.closed || h.client.closed; map_error = map_servicebus_error,
        operation = op, deadline = deadline
    )
end

_audience(h::ServiceBusHandler) = Messaging.resource_uri(h.client.config, h.entity_path)

function _status_condition(status::Int, cond)
    cond !== nothing && return Symbol(string(cond))
    status == 401 && return Symbol("amqp:unauthorized-access")
    status == 403 && return Symbol("amqp:unauthorized-access")
    status == 404 && return Symbol("amqp:not-found")
    status == 408 && return Symbol("com.microsoft:timeout")
    status == 410 && return Symbol("com.microsoft:message-lock-lost")
    status == 503 && return Symbol("com.microsoft:server-busy")
    status == 400 && return Symbol("com.microsoft:argument-error")
    return Symbol("amqp:internal-error")
end

"""
Send a request to the entity's `\$management` node. Returns the response
body (`nothing` for 204 No Content).
"""
function _mgmt_request(
        h::ServiceBusHandler, operation::AbstractString, body::Dict{String, Any};
        timeout::Real = 60.0, associated_link_name = nothing
    )
    conn = h.client.conn::ClientConnection
    AMQP.authorize!(conn, _audience(h))
    mgmt = @lock h.lock begin
        _, _, gen = AMQP.acquire!(conn)
        cached = h.mgmt[]
        if cached === nothing || cached[2] != gen || !AMQP.is_open(cached[1])
            m, g = AMQP.open_management(conn, string(h.entity_path, "/\$management"))
            h.mgmt[] = (m, g)
            m
        else
            cached[1]
        end
    end
    ap = Dict{String, Any}(
        "operation" => String(operation),
        "com.microsoft:server-timeout" => UInt32(round(Int, min(timeout, 3600) * 1000))
    )
    associated_link_name === nothing || (ap["associated-link-name"] = String(associated_link_name))
    resp = AMQP.request(mgmt, AMQPMessage(body_type = :value, body = body, application_properties = ap); timeout = timeout)
    status, desc, cond = AMQP.response_status(resp)
    status == 200 && return resp.body
    status == 204 && return nothing
    throw(AMQPException(:link, _status_condition(status, cond), "$(operation) failed with status $(status): $(desc)"))
end
