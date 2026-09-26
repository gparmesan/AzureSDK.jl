# EventHubProducerClient.

"""
    EventHubProducerClient(fully_qualified_namespace, eventhub_name, credential; kwargs...)
    EventHubProducerClient(f::Function, args...; kwargs...)   # do-block: closes the client afterwards

Publishes events to an event hub (`azure.eventhub.EventHubProducerClient`).

`credential` is a `TokenCredential` (e.g. `DefaultAzureCredential()`), an
`AzureNamedKeyCredential` or an `AzureSasCredential`.

Keyword arguments:

- `transport_type`: `TransportType.Amqp` (default), `TransportType.AmqpOverWebsocket`
  or `TransportType.Http` (REST; sending only)
- `http_proxy`: proxy for WebSockets / HTTP, e.g. `(proxy_hostname="proxy", proxy_port=3128)`
- `custom_endpoint_address`, `connection_verify` (CA bundle path)
- `retry_total` (3), `retry_backoff_factor` (0.8), `retry_backoff_max` (120), `retry_mode`
- `auth_timeout` (60 s), `user_agent`

See also [`from_connection_string`](@ref).
"""
mutable struct EventHubProducerClient <: EventHubClient
    config::ClientConfig
    eventhub_name::String
    conn::Union{Nothing,ClientConnection}
    lock::ReentrantLock
    senders::Dict{String,Tuple{AMQP.SenderLink,Int}}
    mgmt::Ref{Any}
    mgmt_lock::ReentrantLock
    closed::Bool
end

function EventHubProducerClient(fully_qualified_namespace::AbstractString, eventhub_name::AbstractString, credential;
    use_development_emulator::Bool=false, kwargs...)
    cfg = ClientConfig(fully_qualified_namespace, credential, EVENTHUBS_SCOPE, "eventhub";
        use_development_emulator=use_development_emulator, kwargs...)
    conn = cfg.transport_type == TransportType.Http ? nothing : Messaging.make_connection(cfg)
    return EventHubProducerClient(cfg, _check_hub_name(eventhub_name), conn, ReentrantLock(),
        Dict{String,Tuple{AMQP.SenderLink,Int}}(), Ref{Any}(nothing), ReentrantLock(), false)
end

function EventHubProducerClient(f::Function, args...; kwargs...)
    client = EventHubProducerClient(args...; kwargs...)
    try
        return f(client)
    finally
        close(client)
    end
end

"""
    from_connection_string(EventHubProducerClient, conn_str; eventhub_name=nothing, kwargs...)
    from_connection_string(EventHubConsumerClient, conn_str; consumer_group, eventhub_name=nothing, kwargs...)

Create a client from an Event Hubs connection string. Connection strings for
the local development emulator (`UseDevelopmentEmulator=true`) are supported.
"""
function from_connection_string(::Type{EventHubProducerClient}, conn_str::AbstractString; eventhub_name=nothing, kwargs...)
    p, name, cred = _from_connection_string_args(conn_str, eventhub_name)
    host = p.port === nothing ? p.fully_qualified_namespace : "$(p.fully_qualified_namespace):$(p.port)"
    return EventHubProducerClient(host, name, cred; use_development_emulator=p.use_development_emulator, kwargs...)
end
function from_connection_string(f::Function, T::Type{EventHubProducerClient}, conn_str::AbstractString; kwargs...)
    client = from_connection_string(T, conn_str; kwargs...)
    try
        return f(client)
    finally
        close(client)
    end
end

function Base.show(io::IO, p::EventHubProducerClient)
    print(io, "EventHubProducerClient(", p.config.fully_qualified_namespace, "/", p.eventhub_name, ", ",
        p.config.transport_type, p.closed ? ", closed" : "", ")")
end


_target(p::EventHubProducerClient, partition_id) =
    partition_id === nothing ? p.eventhub_name : string(p.eventhub_name, "/Partitions/", partition_id)

function _get_sender(p::EventHubProducerClient, partition_id)
    target = _target(p, partition_id)
    conn = p.conn::ClientConnection
    AMQP.authorize!(conn, Messaging.resource_uri(p.config, p.eventhub_name))
    lock(p.lock)
    try
        _check_open(p)
        _, session, gen = AMQP.acquire!(conn)
        cached = get(p.senders, target, nothing)
        if cached !== nothing && cached[2] == gen && AMQP.is_attached(cached[1])
            return cached[1]
        end
        link = AMQP.attach_sender(session, "EHProducer-$(uuid4())", target;
            snd_settle_mode=AMQP.SND_UNSETTLED, rcv_settle_mode=AMQP.RCV_FIRST, timeout=p.config.auth_timeout)
        p.senders[target] = (link, gen)
        return link
    finally
        unlock(p.lock)
    end
end

"""
    create_batch(producer; partition_id=nothing, partition_key=nothing, max_size_in_bytes=nothing) -> EventDataBatch

Create an empty batch bounded by the maximum message size the service allows
on the link (Python: `EventHubProducerClient.create_batch`).
"""
function create_batch(p::EventHubProducerClient; partition_id=nothing, partition_key=nothing,
    max_size_in_bytes::Union{Nothing,Integer}=nothing)
    _check_open(p)
    limit = if p.config.transport_type == TransportType.Http
        1024 * 1024
    else
        link = _run(() -> _get_sender(p, partition_id), p, "create_batch")
        link.max_message_size > 0 ? Int(link.max_message_size) : 1024 * 1024
    end
    if max_size_in_bytes !== nothing
        max_size_in_bytes > limit && throw(ArgumentError(
            "Max message size: $(max_size_in_bytes) is too large, acceptable max batch size is: $(limit) bytes."))
        limit = Int(max_size_in_bytes)
    end
    return EventDataBatch(max_size_in_bytes=limit, partition_id=partition_id, partition_key=partition_key)
end

function _outcome_error(outcome)
    outcome isa AMQP.Accepted && return nothing
    if outcome isa AMQP.Rejected
        return AMQPException(:delivery, outcome.error, "the service rejected the events")
    elseif outcome isa AMQP.Released || outcome isa AMQP.Modified
        return AMQPException(:delivery, Symbol("amqp:internal-error"), "the service released the events; they were not stored")
    elseif outcome === nothing
        return nothing
    end
    return AMQPException(:delivery, Symbol("amqp:internal-error"), "unexpected delivery outcome $(typeof(outcome))")
end

"""
    send_batch(producer, batch::EventDataBatch; timeout=nothing)
    send_batch(producer, events::AbstractVector{EventData}; partition_id=nothing, partition_key=nothing, timeout=nothing)

Send a batch of events. A vector of events is packed into a single batch
(throws `EventDataSendError` if it does not fit).
"""
function send_batch(p::EventHubProducerClient, batch::EventDataBatch; timeout::Union{Nothing,Real}=nothing)
    isempty(batch) && return nothing
    if p.config.transport_type == TransportType.Http
        return _http_send_batch(p, batch; timeout=timeout)
    end
    payload = batch_payload(batch)
    _run(p, "send_batch"; timeout=timeout) do
        link = _get_sender(p, batch.partition_id)
        outcome = AMQP.send_transfer(link, payload; message_format=UInt32(AMQP.BATCH_MESSAGE_FORMAT),
            timeout=something(timeout, 60.0))
        err = _outcome_error(outcome)
        err === nothing || throw(err)
        nothing
    end
    return nothing
end

function send_batch(p::EventHubProducerClient, events::AbstractVector{EventData}; partition_id=nothing,
    partition_key=nothing, timeout::Union{Nothing,Real}=nothing)
    isempty(events) && return nothing
    batch = create_batch(p; partition_id=partition_id, partition_key=partition_key)
    for e in events
        try_add!(batch, e) || throw(EventDataSendError(
            "The list of events is too large to fit into one batch (max $(batch.max_size_in_bytes) bytes); use create_batch and send several batches."))
    end
    return send_batch(p, batch; timeout=timeout)
end

"""
    send_event(producer, event::EventData; partition_id=nothing, partition_key=nothing, timeout=nothing)

Send a single event.
"""
function send_event(p::EventHubProducerClient, event::EventData; partition_id=nothing, partition_key=nothing,
    timeout::Union{Nothing,Real}=nothing)
    return send_batch(p, [event]; partition_id=partition_id, partition_key=partition_key, timeout=timeout)
end

"""
    close(producer)

Close the client and its connection. Safe to call more than once.
"""
function Base.close(p::EventHubProducerClient)
    @lock p.lock begin
        p.closed && return nothing
        p.closed = true
        empty!(p.senders)
    end
    p.conn === nothing || close(p.conn)
    return nothing
end
Base.isopen(p::EventHubProducerClient) = !p.closed

# ---------------------------------------------------------------------------
# HTTP (REST) transport
# ---------------------------------------------------------------------------

function _http_send_batch(p::EventHubProducerClient, batch::EventDataBatch; timeout=nothing)
    cfg = p.config
    path = batch.partition_id === nothing ? "$(p.eventhub_name)/messages" :
           "$(p.eventhub_name)/partitions/$(URIs.escapeuri(batch.partition_id))/messages"
    url = string(Messaging.https_base(cfg), "/", path, "?api-version=2014-01&timeout=60")
    events = [AMQP.decode_message(m) for m in batch.messages]
    all_text = all(m -> m.body_type === :data && isvalid(String(copy(AMQP.body_bytes(m)))), events)
    _run(p, "send_batch"; timeout=timeout) do
        auth = Messaging.http_authorization(cfg.credential, Messaging.resource_uri(cfg, p.eventhub_name), cfg.scope)
        if all_text && length(events) > 1
            items = map(events) do m
                d = Dict{String,Any}("Body" => String(copy(AMQP.body_bytes(m))))
                m.application_properties === nothing || (d["UserProperties"] = _http_props(m.application_properties))
                batch.partition_key === nothing || (d["BrokerProperties"] = Dict("PartitionKey" => batch.partition_key))
                d
            end
            _http_post(url, auth, "application/vnd.microsoft.servicebus.json", JSON.json(items), cfg,
                Pair{String,String}[])
        else
            for m in events
                headers = Pair{String,String}[]
                batch.partition_key === nothing ||
                    push!(headers, "BrokerProperties" => JSON.json(Dict("PartitionKey" => batch.partition_key)))
                if m.application_properties !== nothing
                    for (k, v) in m.application_properties
                        push!(headers, _http_header_name(k) => JSON.json(v))
                    end
                end
                ct = m.properties === nothing || m.properties.content_type === nothing ?
                     "application/atom+xml;type=entry;charset=utf-8" : string(m.properties.content_type)
                _http_post(url, auth, ct, AMQP.body_bytes(m), cfg, headers)
            end
        end
        nothing
    end
    return nothing
end

function _http_header_name(k::AbstractString)
    occursin(r"^[A-Za-z0-9\-_]+$", k) || throw(EventDataError("property name $(repr(k)) cannot be sent as an HTTP header"))
    return String(k)
end

_http_props(d::AbstractDict) = Dict{String,Any}(k => (v isa Dates.DateTime ? string(v, "Z") : v) for (k, v) in d)

function _http_post(url, auth, content_type, body, cfg::ClientConfig, extra_headers)
    headers = vcat(Pair{String,String}["Authorization" => auth, "Content-Type" => content_type,
            "User-Agent" => cfg.user_agent], extra_headers)
    resp = AzureCore.send_request("POST", url; headers=headers, body=body, retry=RetryOptions(retry_total=0),
        timeout=60.0, client=cfg.http_client)
    resp.status in (200, 201) && return resp
    cond = resp.status == 401 || resp.status == 403 ? Symbol("amqp:unauthorized-access") :
           resp.status == 404 ? Symbol("amqp:not-found") :
           resp.status == 413 ? Symbol("amqp:link:message-size-exceeded") :
           resp.status in (429, 503) ? Symbol("com.microsoft:server-busy") :
           resp.status in (500, 502, 504) ? Symbol("amqp:internal-error") : Symbol("com.microsoft:argument-error")
    throw(AMQPException(:link, cond, "HTTP $(resp.status) from $(AzureCore.redact_url(url))"))
end
