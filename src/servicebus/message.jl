# ServiceBusMessage, ServiceBusReceivedMessage and ServiceBusMessageBatch.

const ANN_PARTITION_KEY = Symbol("x-opt-partition-key")
const ANN_VIA_PARTITION_KEY = Symbol("x-opt-via-partition-key")
const ANN_SCHEDULED_ENQUEUE_TIME = Symbol("x-opt-scheduled-enqueue-time")
const ANN_SEQUENCE_NUMBER = Symbol("x-opt-sequence-number")
const ANN_ENQUEUE_SEQUENCE_NUMBER = Symbol("x-opt-enqueue-sequence-number")
const ANN_ENQUEUED_TIME = Symbol("x-opt-enqueued-time")
const ANN_LOCKED_UNTIL = Symbol("x-opt-locked-until")
const ANN_LOCK_TOKEN = Symbol("x-opt-lock-token")
const ANN_DEADLETTER_SOURCE = Symbol("x-opt-deadletter-source")
const ANN_MESSAGE_STATE = Symbol("x-opt-message-state")
const PROP_DEAD_LETTER_REASON = "DeadLetterReason"

"""
    ServiceBusMessageState.ACTIVE / DEFERRED / SCHEDULED
"""
module ServiceBusMessageState
@enum T ACTIVE = 0 DEFERRED = 1 SCHEDULED = 2
end
const PROP_DEAD_LETTER_ERROR_DESCRIPTION = "DeadLetterErrorDescription"

_sb_bytes(::Nothing) = UInt8[]
_sb_bytes(b::AbstractVector{UInt8}) = Vector{UInt8}(b)
_sb_bytes(s::AbstractString) = Vector{UInt8}(codeunits(String(s)))
_sb_bytes(x) = throw(ArgumentError("message body must be a String, a byte vector or nothing; got $(typeof(x))"))

_ttl_seconds(::Nothing) = nothing
_ttl_seconds(x::Real) = Float64(x)
_ttl_seconds(p::Dates.Period) = Dates.value(Dates.Millisecond(p)) / 1000

function _check_id(name, v, maxlen=128)
    v === nothing && return nothing
    s = string(v)
    length(s) > maxlen && throw(ArgumentError("$name cannot be longer than $maxlen characters"))
    return s
end

"""
    ServiceBusMessage(body=nothing; application_properties=nothing, session_id=nothing, message_id=nothing,
                      scheduled_enqueue_time_utc=nothing, time_to_live=nothing, content_type=nothing,
                      correlation_id=nothing, subject=nothing, partition_key=nothing, to=nothing,
                      reply_to=nothing, reply_to_session_id=nothing)

A message to send (`azure.servicebus.ServiceBusMessage`). `body` is a
`String` or byte vector; `time_to_live` is a `Dates.Period` or seconds;
`scheduled_enqueue_time_utc` a `DateTime` in UTC.
"""
mutable struct ServiceBusMessage
    body::Vector{UInt8}
    application_properties::Dict{String,Any}
    session_id::Union{Nothing,String}
    message_id::Union{Nothing,String}
    scheduled_enqueue_time_utc::Union{Nothing,Dates.DateTime}
    time_to_live::Union{Nothing,Float64}
    content_type::Union{Nothing,String}
    correlation_id::Union{Nothing,String}
    subject::Union{Nothing,String}
    partition_key::Union{Nothing,String}
    to::Union{Nothing,String}
    reply_to::Union{Nothing,String}
    reply_to_session_id::Union{Nothing,String}
end

function ServiceBusMessage(body=nothing; application_properties=nothing, session_id=nothing, message_id=nothing,
    scheduled_enqueue_time_utc=nothing, time_to_live=nothing, content_type=nothing, correlation_id=nothing,
    subject=nothing, partition_key=nothing, to=nothing, reply_to=nothing, reply_to_session_id=nothing)
    sid = _check_id("session_id", session_id)
    pk = _check_id("partition_key", partition_key)
    if sid !== nothing && pk !== nothing && sid != pk
        throw(ArgumentError("If session_id is set, partition_key must be equal to it"))
    end
    ap = application_properties === nothing ? Dict{String,Any}() :
         Dict{String,Any}(string(k) => v for (k, v) in application_properties)
    return ServiceBusMessage(_sb_bytes(body), ap, sid, _check_id("message_id", message_id),
        scheduled_enqueue_time_utc, _ttl_seconds(time_to_live),
        content_type === nothing ? nothing : String(content_type), correlation_id === nothing ? nothing : string(correlation_id),
        subject === nothing ? nothing : String(subject), pk, to === nothing ? nothing : String(to),
        reply_to === nothing ? nothing : String(reply_to), _check_id("reply_to_session_id", reply_to_session_id))
end

function Base.show(io::IO, m::ServiceBusMessage)
    s = String(copy(m.body))
    body = isvalid(s) ? repr(length(s) > 64 ? s[1:prevind(s, 64)] * "…" : s) : "<$(length(m.body)) bytes>"
    print(io, "ServiceBusMessage(body=", body)
    m.message_id === nothing || print(io, ", message_id=", repr(m.message_id))
    m.session_id === nothing || print(io, ", session_id=", repr(m.session_id))
    isempty(m.application_properties) || print(io, ", application_properties=", m.application_properties)
    print(io, ")")
end

"Convert an outgoing message to AMQP."
function to_amqp_message(m::ServiceBusMessage)
    ann = Dict{Any,Any}()
    m.partition_key === nothing || (ann[ANN_PARTITION_KEY] = m.partition_key)
    m.scheduled_enqueue_time_utc === nothing || (ann[ANN_SCHEDULED_ENQUEUE_TIME] = m.scheduled_enqueue_time_utc)
    header = nothing
    creation = nothing
    expiry = nothing
    if m.time_to_live !== nothing
        ttl_ms = round(Int64, m.time_to_live * 1000)
        ttl_ms <= 0 && throw(ArgumentError("time_to_live must be positive"))
        header = AMQP.Header(ttl=UInt32(min(ttl_ms, typemax(UInt32))))
        creation = Dates.now(Dates.UTC)
        expiry = creation + Dates.Millisecond(ttl_ms)
    end
    props = AMQP.Properties(message_id=m.message_id, to=m.to, subject=m.subject, reply_to=m.reply_to,
        correlation_id=m.correlation_id, content_type=m.content_type === nothing ? nothing : Symbol(m.content_type),
        absolute_expiry_time=expiry, creation_time=creation, group_id=m.session_id,
        reply_to_group_id=m.reply_to_session_id)
    return AMQPMessage(header=header, message_annotations=isempty(ann) ? nothing : ann, properties=props,
        application_properties=isempty(m.application_properties) ? nothing : m.application_properties,
        body_type=:data, body=[m.body])
end

"""
    ServiceBusReceivedMessage

A message received from a queue or subscription. Besides the fields of
[`ServiceBusMessage`](@ref) it exposes `sequence_number`,
`enqueued_sequence_number`, `enqueued_time_utc`, `locked_until_utc`,
`lock_token`, `delivery_count`, `dead_letter_reason`,
`dead_letter_error_description`, `dead_letter_source`, `state`,
`expires_at_utc` and `raw_amqp_message`.
"""
mutable struct ServiceBusReceivedMessage
    raw_amqp_message::AMQPMessage
    body::Any
    lock_token::Union{Nothing,UUID}
    locked_until_utc::Union{Nothing,Dates.DateTime}
    delivery::Any                         # AMQP.IncomingDelivery or nothing
    receiver::Any                         # the receiver that received it
    receive_mode::Any
    settled::Bool
    link_generation::Int
end

_ann(m::AMQPMessage, k) = m.message_annotations === nothing ? nothing : get(m.message_annotations, k, nothing)
_prop(m::AMQPMessage, f) = m.properties === nothing ? nothing : getfield(m.properties, f)

function Base.getproperty(m::ServiceBusReceivedMessage, s::Symbol)
    raw = getfield(m, :raw_amqp_message)
    if s === :application_properties
        return something(raw.application_properties, Dict{String,Any}())
    elseif s === :message_id
        v = _prop(raw, :message_id); return v === nothing ? nothing : string(v)
    elseif s === :session_id
        return _prop(raw, :group_id)
    elseif s === :reply_to_session_id
        return _prop(raw, :reply_to_group_id)
    elseif s === :content_type
        v = _prop(raw, :content_type); return v === nothing ? nothing : string(v)
    elseif s === :correlation_id
        v = _prop(raw, :correlation_id); return v === nothing ? nothing : string(v)
    elseif s === :subject
        return _prop(raw, :subject)
    elseif s === :to
        v = _prop(raw, :to); return v === nothing ? nothing : string(v)
    elseif s === :reply_to
        v = _prop(raw, :reply_to); return v === nothing ? nothing : string(v)
    elseif s === :partition_key
        return _ann(raw, ANN_PARTITION_KEY)
    elseif s === :scheduled_enqueue_time_utc
        return _ann(raw, ANN_SCHEDULED_ENQUEUE_TIME)
    elseif s === :time_to_live
        h = raw.header
        return h === nothing || h.ttl === nothing ? nothing : h.ttl / 1000
    elseif s === :sequence_number
        v = _ann(raw, ANN_SEQUENCE_NUMBER); return v === nothing ? nothing : Int64(v)
    elseif s === :enqueued_sequence_number
        v = _ann(raw, ANN_ENQUEUE_SEQUENCE_NUMBER); return v === nothing ? nothing : Int64(v)
    elseif s === :enqueued_time_utc
        return _ann(raw, ANN_ENQUEUED_TIME)
    elseif s === :delivery_count
        h = raw.header
        return h === nothing || h.delivery_count === nothing ? 0 : Int(h.delivery_count)
    elseif s === :dead_letter_reason
        return get(something(raw.application_properties, Dict{String,Any}()), PROP_DEAD_LETTER_REASON, nothing)
    elseif s === :dead_letter_error_description
        return get(something(raw.application_properties, Dict{String,Any}()), PROP_DEAD_LETTER_ERROR_DESCRIPTION, nothing)
    elseif s === :dead_letter_source
        return _ann(raw, ANN_DEADLETTER_SOURCE)
    elseif s === :state
        v = _ann(raw, ANN_MESSAGE_STATE)
        return v === nothing ? ServiceBusMessageState.ACTIVE : ServiceBusMessageState.T(Int(v))
    elseif s === :expires_at_utc
        et = _ann(raw, ANN_ENQUEUED_TIME)
        h = raw.header
        (et === nothing || h === nothing || h.ttl === nothing) && return nothing
        return et + Dates.Millisecond(h.ttl)
    end
    return getfield(m, s)
end

Base.propertynames(::ServiceBusReceivedMessage, private::Bool=false) = (:body, :application_properties, :message_id,
    :session_id, :reply_to_session_id, :content_type, :correlation_id, :subject, :to, :reply_to, :partition_key,
    :scheduled_enqueue_time_utc, :time_to_live, :sequence_number, :enqueued_sequence_number, :enqueued_time_utc,
    :locked_until_utc, :lock_token, :delivery_count, :dead_letter_reason, :dead_letter_error_description,
    :dead_letter_source, :state, :expires_at_utc, :raw_amqp_message)

function Base.show(io::IO, m::ServiceBusReceivedMessage)
    b = getfield(m, :body)
    body = if b isa Vector{UInt8}
        s = String(copy(b))
        isvalid(s) ? repr(length(s) > 64 ? s[1:prevind(s, 64)] * "…" : s) : "<$(length(b)) bytes>"
    else
        repr(b)
    end
    print(io, "ServiceBusReceivedMessage(body=", body, ", sequence_number=", m.sequence_number)
    m.message_id === nothing || print(io, ", message_id=", repr(m.message_id))
    m.session_id === nothing || print(io, ", session_id=", repr(m.session_id))
    print(io, ", delivery_count=", m.delivery_count, ")")
end

"""
    .NET GUID bytes (as used in Service Bus delivery tags) -> UUID
"""
function guid_from_bytes_le(b::AbstractVector{UInt8})
    length(b) == 16 || return nothing
    be = UInt8[b[4], b[3], b[2], b[1], b[6], b[5], b[8], b[7], b[9], b[10], b[11], b[12], b[13], b[14], b[15], b[16]]
    x = UInt128(0)
    for byte in be
        x = (x << 8) | byte
    end
    return UUID(x)
end

function _to_datetime(v)
    v === nothing && return nothing
    v isa Dates.DateTime && return v
    if v isa Integer
        # .NET ticks (100 ns since 0001-01-01) or Unix milliseconds
        if v > 100_000_000_000_000_000
            return Dates.DateTime(1, 1, 1) + Dates.Millisecond(div(Int64(v), 10_000))
        end
        return AMQP.UNIX_EPOCH + Dates.Millisecond(Int64(v))
    end
    return nothing
end

function ServiceBusReceivedMessage(raw::AMQPMessage; delivery=nothing, receiver=nothing, receive_mode=nothing,
    lock_token=nothing, link_generation::Int=0)
    body = raw.body_type === :data ? AMQP.body_bytes(raw) : raw.body
    tok = lock_token
    if tok === nothing && delivery !== nothing
        tok = guid_from_bytes_le(delivery.delivery_tag)
    end
    if tok === nothing
        v = _ann(raw, ANN_LOCK_TOKEN)
        v isa UUID && (tok = v)
    end
    return ServiceBusReceivedMessage(raw, body, tok, _to_datetime(_ann(raw, ANN_LOCKED_UNTIL)), delivery, receiver,
        receive_mode, false, link_generation)
end

"""
    body_as_str(message) -> String
"""
function body_as_str(m::Union{ServiceBusMessage,ServiceBusReceivedMessage})
    b = getfield(m, :body)
    b isa Vector{UInt8} && return String(copy(b))
    return string(b)
end

# ---------------------------------------------------------------------------
# Batches
# ---------------------------------------------------------------------------

"""
    ServiceBusMessageBatch

A size-bounded batch created with [`create_message_batch`](@ref). Add messages
with [`add_message!`](@ref).
"""
mutable struct ServiceBusMessageBatch
    max_size_in_bytes::Int
    messages::Vector{Vector{UInt8}}
    size_in_bytes::Int
    partition_key::Union{Nothing,String}
    session_id::Union{Nothing,String}
end
ServiceBusMessageBatch(max_size_in_bytes::Integer=256 * 1024) =
    ServiceBusMessageBatch(Int(max_size_in_bytes), Vector{UInt8}[], 0, nothing, nothing)

Base.length(b::ServiceBusMessageBatch) = length(b.messages)
Base.isempty(b::ServiceBusMessageBatch) = isempty(b.messages)
size_in_bytes(b::ServiceBusMessageBatch) = b.size_in_bytes
max_size_in_bytes(b::ServiceBusMessageBatch) = b.max_size_in_bytes
Base.show(io::IO, b::ServiceBusMessageBatch) = print(io, "ServiceBusMessageBatch(count=", length(b), ", size_in_bytes=",
    b.size_in_bytes, ", max_size_in_bytes=", b.max_size_in_bytes, ")")

function _envelope_prefix(b::ServiceBusMessageBatch)
    ann = Dict{Any,Any}()
    b.partition_key === nothing || (ann[ANN_PARTITION_KEY] = b.partition_key)
    props = b.session_id === nothing ? nothing : AMQP.Properties(group_id=b.session_id)
    (isempty(ann) && props === nothing) && return UInt8[]
    return AMQP.encode_message(AMQPMessage(message_annotations=isempty(ann) ? nothing : ann, properties=props,
        body_type=:data, body=Vector{UInt8}[]))
end

"""
    try_add_message!(batch, message) -> Bool

Add `message` if it fits in the batch.
"""
to_amqp_message(m::AMQPMessage) = m
_session_of(m::ServiceBusMessage) = m.session_id
_session_of(m::AMQPMessage) = m.properties === nothing ? nothing : m.properties.group_id
function _pkey_of(m::ServiceBusMessage)
    return m.partition_key
end
_pkey_of(m::AMQPMessage) = m.message_annotations === nothing ? nothing : get(m.message_annotations, ANN_PARTITION_KEY, nothing)

function try_add_message!(b::ServiceBusMessageBatch, m::Union{ServiceBusMessage,AMQPMessage})
    sid, pk = _session_of(m), _pkey_of(m)
    if isempty(b.messages)
        b.partition_key = something(pk, sid, Some(nothing))
        b.session_id = sid
    elseif sid != b.session_id || something(pk, sid, Some(nothing)) != b.partition_key
        throw(ArgumentError("all messages in a batch must have the same session_id and partition_key"))
    end
    enc = AMQP.encode_message(to_amqp_message(m))
    prefix = length(_envelope_prefix(b))
    body = sum(x -> length(x) + (length(x) < 256 ? 5 : 8), b.messages; init=0)
    new_size = prefix + body + length(enc) + (length(enc) < 256 ? 5 : 8)
    if new_size > b.max_size_in_bytes
        isempty(b.messages) && (b.partition_key = nothing; b.session_id = nothing)
        return false
    end
    push!(b.messages, enc)
    b.size_in_bytes = new_size
    return true
end

"""
    add_message!(batch, message)

Add a message; throws `MessageSizeExceededError` when it does not fit
(Python: `ServiceBusMessageBatch.add_message`).
"""
function add_message!(b::ServiceBusMessageBatch, m::Union{ServiceBusMessage,AMQPMessage})
    try_add_message!(b, m) || throw(MessageSizeExceededError(
        "ServiceBusMessageBatch has reached its size limit: $(b.max_size_in_bytes)"))
    return b
end

function batch_payload(b::ServiceBusMessageBatch)
    w = AMQP.ByteWriter(b.size_in_bytes + 16)
    AMQP.writebytes!(w, _envelope_prefix(b))
    for enc in b.messages
        AMQP.writebyte!(w, AMQP.TC_DESCRIBED); AMQP.writebyte!(w, AMQP.TC_SMALLULONG)
        AMQP.writebyte!(w, AMQP.SECTION_DATA)
        AMQP.encode!(w, enc)
    end
    return AMQP.takebytes!(w)
end
