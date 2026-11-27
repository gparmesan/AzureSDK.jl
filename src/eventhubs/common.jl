# Pieces shared by the producer and consumer clients.

"Supertype of [`EventHubProducerClient`](@ref) and [`EventHubConsumerClient`](@ref)."
abstract type EventHubClient end

const EVENTHUBS_SCOPE = "https://eventhubs.azure.net/.default"
const MGMT_ADDRESS = "\$management"

"""
    EventHubConnectionStringProperties

Result of [`parse_connection_string`](@ref): `fully_qualified_namespace`,
`endpoint`, `eventhub_name`, `shared_access_key_name`, `shared_access_key`,
`shared_access_signature`.
"""
struct EventHubConnectionStringProperties
    fully_qualified_namespace::String
    endpoint::String
    eventhub_name::Union{Nothing, String}
    shared_access_key_name::Union{Nothing, String}
    shared_access_key::Union{Nothing, String}
    shared_access_signature::Union{Nothing, String}
end
function Base.show(io::IO, p::EventHubConnectionStringProperties)
    return print(
        io, "EventHubConnectionStringProperties(fully_qualified_namespace=", repr(p.fully_qualified_namespace),
        ", eventhub_name=", repr(p.eventhub_name), ", shared_access_key_name=", repr(p.shared_access_key_name), ")"
    )
end

"""
    parse_connection_string(conn_str) -> EventHubConnectionStringProperties
"""
function parse_connection_string(conn_str::AbstractString)
    p = Messaging.parse_messaging_connection_string(conn_str)
    return EventHubConnectionStringProperties(
        p.fully_qualified_namespace, p.endpoint, p.entity_path,
        p.shared_access_key_name, p.shared_access_key, p.shared_access_signature
    )
end

"Split a connection string into constructor arguments shared by both clients."
function _from_connection_string_args(conn_str::AbstractString, eventhub_name)
    p = Messaging.parse_messaging_connection_string(conn_str)
    name = eventhub_name === nothing ? p.entity_path : String(eventhub_name)
    if p.entity_path !== nothing && eventhub_name !== nothing && p.entity_path != eventhub_name
        throw(ArgumentError("The eventhub_name does not match the EntityPath in the connection string"))
    end
    name === nothing && throw(ArgumentError("eventhub_name must be given or present as EntityPath in the connection string"))
    return p, name, Messaging.connection_string_credential(p)
end

function _check_hub_name(name::AbstractString)
    (isempty(name) || occursin(r"[\s?#]", name) || startswith(name, "/")) &&
        throw(ArgumentError("invalid eventhub_name $(repr(name))"))
    return String(name)
end

# EnumX-style enums mirroring the Python SDK
module CloseReason
    @enum T SHUTDOWN OWNERSHIP_LOST
end

module LoadBalancingStrategy
    @enum T GREEDY BALANCED
end

"Run a management request on the hub's `\$management` node."
function _management_request(
        conn::ClientConnection, cfg::ClientConfig, hub::String, mgmt_ref::Ref{Any},
        lk::ReentrantLock, props::Dict{String, Any}; timeout::Real = 60.0
    )
    audience = Messaging.resource_uri(cfg, hub)
    AMQP.authorize!(conn, audience)
    mgmt = @lock lk begin
        cached = mgmt_ref[]
        _, _, gen = AMQP.acquire!(conn)
        if cached === nothing || cached[2] != gen || !AMQP.is_open(cached[1])
            m, g = AMQP.open_management(conn, MGMT_ADDRESS)
            mgmt_ref[] = (m, g)
            m
        else
            cached[1]
        end
    end
    token, _, _ = conn.token_provider(audience)
    ap = Dict{String, Any}("name" => hub, "security_token" => token)
    merge!(ap, props)
    msg = AMQPMessage(body_type = :value, body = nothing, application_properties = ap)
    resp = AMQP.request(mgmt, msg; timeout = timeout)
    status, desc, _ = AMQP.response_status(resp)
    if status != 200
        cond = status == 401 ? Symbol("amqp:unauthorized-access") : status == 404 ? Symbol("amqp:not-found") :
            status == 503 ? Symbol("com.microsoft:server-busy") : Symbol("amqp:internal-error")
        throw(AMQPException(:link, cond, "management request failed with status $status: $desc"))
    end
    return resp.body
end

function _eventhub_properties(body, hub)
    body isa AbstractDict || throw(EventHubServiceError("unexpected management response"))
    return Dict{String, Any}(
        "eventhub_name" => get(body, "name", hub),
        "created_at" => get(body, "created_at", nothing),
        "partition_ids" => String[string(x) for x in get(body, "partition_ids", String[])],
    )
end

function _partition_properties(body, hub)
    body isa AbstractDict || throw(EventHubServiceError("unexpected management response"))
    lo = get(body, "last_enqueued_offset", nothing)
    return Dict{String, Any}(
        "eventhub_name" => get(body, "name", hub),
        "id" => string(get(body, "partition", "")),
        "beginning_sequence_number" => get(body, "begin_sequence_number", nothing),
        "last_enqueued_sequence_number" => get(body, "last_enqueued_sequence_number", nothing),
        "last_enqueued_offset" => lo === nothing ? nothing : string(lo),
        "last_enqueued_time_utc" => get(body, "last_enqueued_time_utc", nothing),
        "is_empty" => get(body, "is_partition_empty", nothing),
    )
end

_check_open(p::EventHubClient) = p.closed && throw(ClientClosedError("The client has been closed."))

function _run(f, p::EventHubClient, op::AbstractString; timeout = nothing)
    _check_open(p)
    deadline = timeout === nothing ? nothing : time() + timeout
    return Messaging.run_with_retry(
        f, p.config.retry, () -> p.closed; map_error = map_eventhub_error,
        operation = op, deadline = deadline
    )
end

"""
    get_eventhub_properties(client) -> Dict{String,Any}

Keys: `eventhub_name`, `created_at`, `partition_ids`.
"""
function get_eventhub_properties(p::EventHubClient; timeout::Real = 60.0)
    p.config.transport_type == TransportType.Http &&
        throw(ArgumentError("get_eventhub_properties requires an AMQP transport"))
    body = _run(p, "get_eventhub_properties") do
        _management_request(
            p.conn, p.config, p.eventhub_name, p.mgmt, p.mgmt_lock,
            Dict{String, Any}("operation" => "READ", "type" => "com.microsoft:eventhub"); timeout = timeout
        )
    end
    return _eventhub_properties(body, p.eventhub_name)
end

"""
    get_partition_ids(client) -> Vector{String}
"""
get_partition_ids(p::EventHubClient; kwargs...) = get_eventhub_properties(p; kwargs...)["partition_ids"]

"""
    get_partition_properties(client, partition_id) -> Dict{String,Any}

Keys: `eventhub_name`, `id`, `beginning_sequence_number`,
`last_enqueued_sequence_number`, `last_enqueued_offset`,
`last_enqueued_time_utc`, `is_empty`.
"""
function get_partition_properties(p::EventHubClient, partition_id; timeout::Real = 60.0)
    p.config.transport_type == TransportType.Http &&
        throw(ArgumentError("get_partition_properties requires an AMQP transport"))
    body = _run(p, "get_partition_properties") do
        _management_request(
            p.conn, p.config, p.eventhub_name, p.mgmt, p.mgmt_lock,
            Dict{String, Any}(
                "operation" => "READ", "type" => "com.microsoft:partition",
                "partition" => string(partition_id)
            ); timeout = timeout
        )
    end
    return _partition_properties(body, p.eventhub_name)
end
