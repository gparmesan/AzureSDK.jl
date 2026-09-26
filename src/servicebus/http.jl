# Service Bus REST transport (TransportType.Http): send, receive (peek-lock
# and receive-and-delete), complete, abandon and lock renewal.

const SB_REST_API_VERSION = "2015-01"
const _RFC1123 = Dates.dateformat"e, d u y H:M:S"
const _STANDARD_HEADERS = Set(["content-type", "content-length", "date", "server", "brokerproperties", "location",
    "transfer-encoding", "strict-transport-security", "connection", "keep-alive", "cache-control", "expires",
    "pragma", "x-ms-request-id", "x-ms-version", "etag", "vary", "content-encoding", "x-content-type-options",
    "x-frame-options", "x-powered-by", "request-context", "x-ms-client-request-id"])

_rfc1123(dt::Dates.DateTime) = Dates.format(dt, _RFC1123) * " GMT"
function _parse_rfc1123(s)
    s isa AbstractString || return nothing
    return tryparse(Dates.DateTime, replace(strip(s), r"\s*GMT$" => ""), _RFC1123)
end

function _http_url(h::ServiceBusHandler, suffix::AbstractString; timeout=nothing)
    q = "api-version=$(SB_REST_API_VERSION)"
    timeout === nothing || (q *= "&timeout=$(clamp(round(Int, timeout), 1, 230))")
    return string(Messaging.https_base(h.client.config), "/", h.entity_path, suffix, "?", q)
end

_http_auth(h::ServiceBusHandler) = Messaging.http_authorization(h.client.config.credential, _audience(h), SERVICEBUS_SCOPE)

function _http_do(h::ServiceBusHandler, method::AbstractString, url::AbstractString; headers=Pair{String,String}[],
    body=nothing, ok=(200, 201), timeout::Real=60.0)
    cfg = h.client.config
    hdrs = vcat(Pair{String,String}["Authorization" => _http_auth(h), "User-Agent" => cfg.user_agent], headers)
    resp = AzureCore.send_request(method, url; headers=hdrs, body=body, retry=RetryOptions(retry_total=0),
        timeout=timeout, client=cfg.http_client)
    (resp.status in ok || resp.status == 204) && return resp
    cond = resp.status in (401, 403) ? Symbol("amqp:unauthorized-access") :
           resp.status == 404 ? Symbol("amqp:not-found") :
           resp.status == 410 ? Symbol("com.microsoft:message-lock-lost") :
           resp.status == 413 ? Symbol("amqp:link:message-size-exceeded") :
           resp.status in (429, 503) ? Symbol("com.microsoft:server-busy") :
           resp.status in (500, 502, 504) ? Symbol("amqp:internal-error") : Symbol("com.microsoft:argument-error")
    throw(AMQPException(:link, cond, "HTTP $(resp.status) from $(AzureCore.redact_url(url))"))
end

function _broker_properties(m::ServiceBusMessage)
    bp = Dict{String,Any}()
    m.message_id === nothing || (bp["MessageId"] = m.message_id)
    m.correlation_id === nothing || (bp["CorrelationId"] = m.correlation_id)
    m.session_id === nothing || (bp["SessionId"] = m.session_id)
    m.subject === nothing || (bp["Label"] = m.subject)
    m.reply_to === nothing || (bp["ReplyTo"] = m.reply_to)
    m.to === nothing || (bp["To"] = m.to)
    m.reply_to_session_id === nothing || (bp["ReplyToSessionId"] = m.reply_to_session_id)
    m.partition_key === nothing || (bp["PartitionKey"] = m.partition_key)
    m.time_to_live === nothing || (bp["TimeToLive"] = m.time_to_live)
    m.scheduled_enqueue_time_utc === nothing || (bp["ScheduledEnqueueTimeUtc"] = _rfc1123(m.scheduled_enqueue_time_utc))
    return bp
end

_json_value(v::Dates.DateTime) = _rfc1123(v)
_json_value(v) = v

function _message_from_amqp(raw::AMQPMessage)
    p = something(raw.properties, AMQP.Properties())
    ann = something(raw.message_annotations, Dict{Any,Any}())
    return ServiceBusMessage(AMQP.body_bytes(raw); application_properties=raw.application_properties,
        session_id=p.group_id, message_id=p.message_id, correlation_id=p.correlation_id, subject=p.subject,
        to=p.to, reply_to=p.reply_to, reply_to_session_id=p.reply_to_group_id,
        content_type=p.content_type === nothing ? nothing : string(p.content_type),
        partition_key=get(ann, ANN_PARTITION_KEY, nothing),
        scheduled_enqueue_time_utc=get(ann, ANN_SCHEDULED_ENQUEUE_TIME, nothing),
        time_to_live=raw.header === nothing || raw.header.ttl === nothing ? nothing : raw.header.ttl / 1000)
end

function _http_send(s::ServiceBusSender, batch::ServiceBusMessageBatch; timeout=nothing)
    msgs = [_message_from_amqp(AMQP.decode_message(e)) for e in batch.messages]
    url = _http_url(s, "/messages"; timeout=60)
    all_text = all(m -> isvalid(String(copy(m.body))), msgs)
    _run(s, "send_messages"; timeout=timeout) do
        if length(msgs) > 1 && all_text
            items = [Dict{String,Any}("Body" => String(copy(m.body)), "BrokerProperties" => _broker_properties(m),
                "UserProperties" => Dict{String,Any}(k => _json_value(v) for (k, v) in m.application_properties))
                     for m in msgs]
            _http_do(s, "POST", url; headers=["Content-Type" => "application/vnd.microsoft.servicebus.json"],
                body=JSON.json(items))
        else
            for m in msgs
                headers = Pair{String,String}["Content-Type" => something(m.content_type, "application/octet-stream"),
                    "BrokerProperties" => JSON.json(_broker_properties(m))]
                for (k, v) in m.application_properties
                    occursin(r"^[A-Za-z0-9\-_]+$", k) ||
                        throw(ArgumentError("application property name $(repr(k)) cannot be sent over HTTP"))
                    push!(headers, k => JSON.json(_json_value(v)))
                end
                _http_do(s, "POST", url; headers=headers, body=m.body)
            end
        end
        nothing
    end
    return nothing
end

function _http_received(r::ServiceBusReceiver, resp)
    bp = try
        JSON.parse(HTTP.header(resp, "BrokerProperties", "{}"); dicttype=Dict{String,Any})
    catch
        Dict{String,Any}()
    end
    ann = Dict{Any,Any}()
    haskey(bp, "SequenceNumber") && (ann[ANN_SEQUENCE_NUMBER] = Int64(bp["SequenceNumber"]))
    haskey(bp, "EnqueuedSequenceNumber") && (ann[ANN_ENQUEUE_SEQUENCE_NUMBER] = Int64(bp["EnqueuedSequenceNumber"]))
    et = _parse_rfc1123(get(bp, "EnqueuedTimeUtc", nothing)); et === nothing || (ann[ANN_ENQUEUED_TIME] = et)
    lu = _parse_rfc1123(get(bp, "LockedUntilUtc", nothing)); lu === nothing || (ann[ANN_LOCKED_UNTIL] = lu)
    haskey(bp, "PartitionKey") && (ann[ANN_PARTITION_KEY] = bp["PartitionKey"])
    haskey(bp, "DeadLetterSource") && (ann[ANN_DEADLETTER_SOURCE] = bp["DeadLetterSource"])
    tok = get(bp, "LockToken", nothing)
    lock_token = tok isa AbstractString ? tryparse(UUID, tok) : nothing
    ttl = get(bp, "TimeToLive", nothing)
    header = AMQP.Header(delivery_count=UInt32(max(0, Int(get(bp, "DeliveryCount", 1)) - 1)),
        ttl=ttl isa Real && ttl * 1000 < typemax(UInt32) ? UInt32(round(Int, ttl * 1000)) : nothing)
    ct = HTTP.header(resp, "Content-Type", "")
    props = AMQP.Properties(message_id=get(bp, "MessageId", nothing), correlation_id=get(bp, "CorrelationId", nothing),
        group_id=get(bp, "SessionId", nothing), subject=get(bp, "Label", nothing), reply_to=get(bp, "ReplyTo", nothing),
        to=get(bp, "To", nothing), reply_to_group_id=get(bp, "ReplyToSessionId", nothing),
        content_type=isempty(ct) ? nothing : Symbol(ct))
    app = Dict{String,Any}()
    for (k, v) in resp.headers
        lowercase(k) in _STANDARD_HEADERS && continue
        app[k] = try
            JSON.parse(v)
        catch
            v
        end
    end
    raw = AMQPMessage(header=header, message_annotations=ann, properties=props,
        application_properties=isempty(app) ? nothing : app, body_type=:data, body=[Vector{UInt8}(resp.body)])
    m = ServiceBusReceivedMessage(raw; receiver=r, receive_mode=r.receive_mode, lock_token=lock_token)
    if r.receive_mode == ServiceBusReceiveMode.RECEIVE_AND_DELETE
        m.settled = true
    else
        loc = HTTP.header(resp, "Location", "")
        _check_location(r, loc)
        lock_token === nothing || @lock r.lock (r.http_locks[lock_token] = loc)
    end
    return m
end

# Never send credentials to a host other than the namespace.
function _check_location(r::ServiceBusReceiver, loc::AbstractString)
    isempty(loc) && throw(ServiceBusServiceError("peek-lock response without Location header"))
    u = URIs.URI(loc)
    lowercase(u.host) == lowercase(r.client.config.fully_qualified_namespace) ||
        throw(ServiceBusServiceError("unexpected lock location host"))
    return nothing
end

function _http_receive(r::ServiceBusReceiver, n::Int, wait::Union{Nothing,Float64})
    r.session === nothing || throw(ArgumentError("session receivers require an AMQP transport"))
    peek_lock = r.receive_mode == ServiceBusReceiveMode.PEEK_LOCK
    out = ServiceBusReceivedMessage[]
    deadline = wait === nothing ? Inf : time() + wait
    while length(out) < n
        remaining = isempty(out) ? deadline - time() : 0.0
        isempty(out) && remaining <= 0 && break
        poll = isempty(out) ? min(isinf(remaining) ? 60.0 : remaining, 60.0) : 1.0
        url = _http_url(r, "/messages/head"; timeout=poll)
        resp = _run(r, "receive_messages") do
            _http_do(r, peek_lock ? "POST" : "DELETE", url; ok=(200, 201), timeout=poll + 30)
        end
        resp.status == 204 && (isempty(out) ? continue : break)
        push!(out, _http_received(r, resp))
    end
    return out
end

function _http_lock_url(r::ServiceBusReceiver, m::ServiceBusReceivedMessage)
    tok = getfield(m, :lock_token)
    loc = @lock r.lock get(r.http_locks, tok, nothing)
    loc === nothing && throw(MessageLockLostError("no lock information for this message"))
    return loc
end

function _http_settle(r::ServiceBusReceiver, m::ServiceBusReceivedMessage, op::Symbol)
    op in (:complete, :abandon) || throw(ArgumentError("$(op) is not supported over HTTP (use an AMQP transport)"))
    url = _http_lock_url(r, m)
    _run(r, "settle_message"; retry=false) do
        _http_do(r, op === :complete ? "DELETE" : "PUT", url; ok=(200,))
    end
    @lock r.lock delete!(r.http_locks, getfield(m, :lock_token))
    return nothing
end

function _http_renew(r::ServiceBusReceiver, m::ServiceBusReceivedMessage)
    url = _http_lock_url(r, m)
    resp = _run(r, "renew_message_lock") do
        _http_do(r, "POST", url; ok=(200,))
    end
    bp = try
        JSON.parse(HTTP.header(resp, "BrokerProperties", "{}"); dicttype=Dict{String,Any})
    catch
        Dict{String,Any}()
    end
    lu = _parse_rfc1123(get(bp, "LockedUntilUtc", nothing))
    m.locked_until_utc = something(lu, Dates.now(Dates.UTC) + Dates.Second(30))
    return m.locked_until_utc
end
