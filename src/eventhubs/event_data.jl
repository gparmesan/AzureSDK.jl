# EventData and EventDataBatch.

const PROP_PARTITION_KEY = Symbol("x-opt-partition-key")
const PROP_SEQUENCE_NUMBER = Symbol("x-opt-sequence-number")
const PROP_OFFSET = Symbol("x-opt-offset")
const PROP_ENQUEUED_TIME = Symbol("x-opt-enqueued-time")
const PROP_LAST_ENQUEUED_SEQUENCE_NUMBER = Symbol("x-opt-last-enqueued-sequence-number")
const PROP_LAST_ENQUEUED_OFFSET = Symbol("x-opt-last-enqueued-offset")
const PROP_LAST_ENQUEUED_TIME_UTC = Symbol("x-opt-last-enqueued-time-utc")
const PROP_RUNTIME_INFO_RETRIEVAL_TIME_UTC = Symbol("x-opt-runtime-info-retrieval-time-utc")

_to_bytes(::Nothing) = UInt8[]
_to_bytes(b::AbstractVector{UInt8}) = Vector{UInt8}(b)
_to_bytes(s::AbstractString) = Vector{UInt8}(codeunits(String(s)))
_to_bytes(x) = throw(ArgumentError("EventData body must be a String, a byte vector or nothing; got $(typeof(x))"))

"""
    EventData(body=nothing; properties=nothing, content_type=nothing, correlation_id=nothing, message_id=nothing)

An event to send to, or received from, an event hub. `body` may be a
`String` or a byte vector.

Fields: `body` (bytes; for received events with an AMQP value/sequence body,
the decoded value), `properties` (application properties, `Dict{String,Any}`),
`content_type`, `correlation_id`, `message_id`, `raw_amqp_message`.

Received events also expose (as properties) `sequence_number`, `offset`,
`enqueued_time`, `partition_key` and `system_properties`.
"""
mutable struct EventData
    body::Any
    properties::Dict{String,Any}
    content_type::Union{Nothing,String}
    correlation_id::Any
    message_id::Any
    raw_amqp_message::Union{Nothing,AMQPMessage}
end

function EventData(body=nothing; properties=nothing, content_type=nothing, correlation_id=nothing, message_id=nothing)
    props = properties === nothing ? Dict{String,Any}() : Dict{String,Any}(string(k) => v for (k, v) in properties)
    return EventData(_to_bytes(body), props, content_type === nothing ? nothing : String(content_type),
        correlation_id, message_id, nothing)
end

function _annotation(e::EventData, key::Symbol)
    m = getfield(e, :raw_amqp_message)
    (m === nothing || m.message_annotations === nothing) && return nothing
    return get(m.message_annotations, key, nothing)
end

function Base.getproperty(e::EventData, s::Symbol)
    if s === :sequence_number
        v = _annotation(e, PROP_SEQUENCE_NUMBER)
        return v === nothing ? nothing : Int64(v)
    elseif s === :offset
        v = _annotation(e, PROP_OFFSET)
        return v === nothing ? nothing : string(v)
    elseif s === :enqueued_time
        return _annotation(e, PROP_ENQUEUED_TIME)
    elseif s === :partition_key
        return _annotation(e, PROP_PARTITION_KEY)
    elseif s === :system_properties
        return _system_properties(e)
    elseif s === :body_type
        m = getfield(e, :raw_amqp_message)
        return m === nothing ? :data : m.body_type
    end
    return getfield(e, s)
end
Base.propertynames(::EventData, private::Bool=false) =
    (fieldnames(EventData)..., :sequence_number, :offset, :enqueued_time, :partition_key, :system_properties, :body_type)

function _system_properties(e::EventData)
    m = getfield(e, :raw_amqp_message)
    d = Dict{String,Any}()
    m === nothing && return d
    if m.message_annotations !== nothing
        for (k, v) in m.message_annotations
            d[string(k)] = v
        end
    end
    p = m.properties
    if p !== nothing
        for f in fieldnames(AMQP.Properties)
            v = getfield(p, f)
            v === nothing || (d[replace(string(f), "_" => "-")] = v isa Symbol ? string(v) : v)
        end
    end
    return d
end

function Base.show(io::IO, e::EventData)
    print(io, "EventData(")
    b = getfield(e, :body)
    if b isa Vector{UInt8}
        s = String(copy(b))
        if isvalid(s)
            print(io, "body=", repr(length(s) > 64 ? s[1:prevind(s, 64)] * "…" : s))
        else
            print(io, "body=<", length(b), " bytes>")
        end
    else
        print(io, "body=", repr(b))
    end
    sn = e.sequence_number
    sn === nothing || print(io, ", sequence_number=", sn, ", offset=", repr(e.offset), ", enqueued_time=", e.enqueued_time)
    pk = e.partition_key
    pk === nothing || print(io, ", partition_key=", repr(pk))
    isempty(getfield(e, :properties)) || print(io, ", properties=", getfield(e, :properties))
    print(io, ")")
end

"""
    body_as_str(event; encoding="UTF-8") -> String

The event body decoded as text.
"""
function body_as_str(e::EventData; encoding::AbstractString="UTF-8")
    uppercase(replace(encoding, "-" => "")) == "UTF8" || throw(ArgumentError("only UTF-8 is supported"))
    b = getfield(e, :body)
    b isa Vector{UInt8} && return String(copy(b))
    b isa AbstractString && return String(b)
    return string(b)
end

"""
    body_as_json(event) -> Any

The event body parsed as JSON.
"""
body_as_json(e::EventData) = JSON.parse(body_as_str(e))

"Build the AMQP message for an outgoing event."
function to_amqp_message(e::EventData; partition_key::Union{Nothing,AbstractString}=nothing)
    raw = getfield(e, :raw_amqp_message)
    body = getfield(e, :body)
    m = if raw !== nothing
        # re-sending a received event: keep its body and properties, drop broker annotations
        AMQPMessage(header=raw.header, properties=raw.properties, application_properties=raw.application_properties,
            body_type=raw.body_type, body=raw.body, footer=raw.footer)
    else
        AMQPMessage(body_type=:data, body=[body::Vector{UInt8}])
    end
    props = getfield(e, :properties)
    m.application_properties = isempty(props) ? nothing : props
    mid, cid, ct = getfield(e, :message_id), getfield(e, :correlation_id), getfield(e, :content_type)
    if mid !== nothing || cid !== nothing || ct !== nothing || m.properties !== nothing
        base = something(m.properties, AMQP.Properties())
        m.properties = AMQP.with_fields(base; message_id=something(mid, base.message_id, Some(nothing)),
            correlation_id=something(cid, base.correlation_id, Some(nothing)),
            content_type=ct === nothing ? base.content_type : Symbol(ct))
    end
    if partition_key !== nothing
        m.message_annotations = Dict{Any,Any}(PROP_PARTITION_KEY => String(partition_key))
    end
    return m
end

function to_amqp_message(m::AMQPMessage; partition_key::Union{Nothing,AbstractString}=nothing)
    partition_key === nothing && return m
    out = deepcopy(m)
    ann = something(out.message_annotations, Dict{Any,Any}())
    ann[PROP_PARTITION_KEY] = String(partition_key)
    out.message_annotations = ann
    return out
end

"Wrap a received AMQP message."
function EventData(m::AMQPMessage)
    body = m.body_type === :data ? AMQP.body_bytes(m) : m.body
    props = something(m.application_properties, Dict{String,Any}())
    p = m.properties
    return EventData(body, props, p === nothing || p.content_type === nothing ? nothing : string(p.content_type),
        p === nothing ? nothing : p.correlation_id, p === nothing ? nothing : p.message_id, m)
end

# ---------------------------------------------------------------------------
# Batches
# ---------------------------------------------------------------------------

"""
    EventDataBatch

A size-bounded batch of events, created with [`create_batch`](@ref). All
events in a batch go to the same partition (the batch's `partition_id`, or the
partition chosen for `partition_key`, or one assigned by the service).
"""
mutable struct EventDataBatch
    max_size_in_bytes::Int
    partition_id::Union{Nothing,String}
    partition_key::Union{Nothing,String}
    messages::Vector{Vector{UInt8}}     # encoded inner messages
    size_in_bytes::Int
end

function EventDataBatch(; max_size_in_bytes::Integer=1024 * 1024, partition_id=nothing, partition_key=nothing)
    partition_id !== nothing && partition_key !== nothing &&
        throw(ArgumentError("partition_id and partition_key cannot be set at the same time"))
    b = EventDataBatch(Int(max_size_in_bytes), partition_id === nothing ? nothing : string(partition_id),
        partition_key === nothing ? nothing : String(partition_key), Vector{UInt8}[], 0)
    b.size_in_bytes = length(_batch_envelope_prefix(b))
    return b
end

function _batch_envelope_prefix(b::EventDataBatch)
    b.partition_key === nothing && return UInt8[]
    return AMQP.encode_message(AMQPMessage(message_annotations=Dict{Any,Any}(PROP_PARTITION_KEY => b.partition_key),
        body_type=:data, body=Vector{UInt8}[]))
end

@inline _data_section_overhead(n::Int) = 3 + (n < 256 ? 2 : 5)

Base.length(b::EventDataBatch) = length(b.messages)
Base.isempty(b::EventDataBatch) = isempty(b.messages)

"Current encoded size of the batch in bytes."
size_in_bytes(b::EventDataBatch) = b.size_in_bytes
"Maximum encoded size of the batch in bytes."
max_size_in_bytes(b::EventDataBatch) = b.max_size_in_bytes

function Base.show(io::IO, b::EventDataBatch)
    print(io, "EventDataBatch(count=", length(b), ", size_in_bytes=", b.size_in_bytes, ", max_size_in_bytes=",
        b.max_size_in_bytes)
    b.partition_id === nothing || print(io, ", partition_id=", repr(b.partition_id))
    b.partition_key === nothing || print(io, ", partition_key=", repr(b.partition_key))
    print(io, ")")
end

"""
    try_add!(batch, event) -> Bool

Add `event` if it fits; return `false` (leaving the batch unchanged) otherwise.
"""
function try_add!(b::EventDataBatch, e::Union{EventData,AMQPMessage})
    enc = AMQP.encode_message(to_amqp_message(e; partition_key=b.partition_key))
    new_size = b.size_in_bytes + length(enc) + _data_section_overhead(length(enc))
    new_size > b.max_size_in_bytes && return false
    push!(b.messages, enc)
    b.size_in_bytes = new_size
    return true
end

"""
    add!(batch, event)

Add `event` to the batch. Throws `ArgumentError` when the batch has reached
its size limit (Python: `ValueError`); use [`try_add!`](@ref) to test instead.
"""
function add!(b::EventDataBatch, e::Union{EventData,AMQPMessage})
    try_add!(b, e) || throw(ArgumentError(
        "EventDataBatch has reached its size limit: $(b.max_size_in_bytes)"))
    return b
end

"Encoded payload of the whole batch (Microsoft batch message format)."
function batch_payload(b::EventDataBatch)
    w = AMQP.ByteWriter(b.size_in_bytes + 16)
    AMQP.writebytes!(w, _batch_envelope_prefix(b))
    for enc in b.messages
        AMQP.writebyte!(w, AMQP.TC_DESCRIBED); AMQP.writebyte!(w, AMQP.TC_SMALLULONG)
        AMQP.writebyte!(w, AMQP.SECTION_DATA)
        AMQP.encode!(w, enc)
    end
    return AMQP.takebytes!(w)
end
