# AMQP 1.0 message format (Part 3: Messaging).

"""
    AMQPMessage

A bare AMQP 1.0 message with all standard sections.

`body_type` is one of `:data` (`body::Vector{Vector{UInt8}}`, one entry per
data section), `:value` (`body` is any AMQP value) or `:sequence`
(`body::Vector{Vector{Any}}`, one entry per amqp-sequence section).

Annotation maps use `Symbol` keys (AMQP symbols); application properties use
`String` keys.
"""
Base.@kwdef mutable struct AMQPMessage
    header::Union{Nothing,Header} = nothing
    delivery_annotations::Union{Nothing,Dict{Any,Any}} = nothing
    message_annotations::Union{Nothing,Dict{Any,Any}} = nothing
    properties::Union{Nothing,Properties} = nothing
    application_properties::Union{Nothing,AbstractDict{String,Any}} = nothing
    body_type::Symbol = :data
    body::Any = Vector{UInt8}[]
    footer::Union{Nothing,Dict{Any,Any}} = nothing
end

function Base.show(io::IO, m::AMQPMessage)
    print(io, "AMQPMessage(body_type=:", m.body_type)
    if m.body_type === :data
        print(io, ", data_bytes=", sum(length, m.body; init=0))
    end
    m.properties !== nothing && m.properties.message_id !== nothing && print(io, ", message_id=", repr(m.properties.message_id))
    print(io, ")")
end

"Build a message whose body is a single data section."
AMQPMessage(data::AbstractVector{UInt8}; kw...) = AMQPMessage(; body_type=:data, body=[Vector{UInt8}(data)], kw...)

"Return the concatenated data sections of a `:data` message body."
function body_bytes(m::AMQPMessage)
    m.body_type === :data || throw(ArgumentError("message body is not a data body (body_type = :$(m.body_type))"))
    parts = m.body::Vector{Vector{UInt8}}
    length(parts) == 1 && return parts[1]
    return reduce(vcat, parts; init=UInt8[])
end

# annotation keys are symbols (or ulong); convert strings for convenience
@inline annotation_key(k::Symbol) = k
@inline annotation_key(k::AbstractString) = Symbol(k)
@inline annotation_key(k::Unsigned) = UInt64(k)
annotation_key(k) = throw(ArgumentError("annotation keys must be symbols, strings or unsigned integers"))

function encode_annotations!(w::ByteWriter, code::UInt8, d::AbstractDict)
    writebyte!(w, TC_DESCRIBED); writebyte!(w, TC_SMALLULONG); writebyte!(w, code)
    start = begin_compound!(w)
    for (k, v) in d
        encode!(w, annotation_key(k))
        encode!(w, v)
    end
    end_compound!(w, start, 2 * length(d), TC_MAP8, TC_MAP32)
end

function encode_message!(w::ByteWriter, m::AMQPMessage)
    m.header !== nothing && encode!(w, m.header)
    m.delivery_annotations !== nothing && encode_annotations!(w, SECTION_DELIVERY_ANNOTATIONS, m.delivery_annotations)
    m.message_annotations !== nothing && encode_annotations!(w, SECTION_MESSAGE_ANNOTATIONS, m.message_annotations)
    m.properties !== nothing && encode!(w, m.properties)
    if m.application_properties !== nothing
        writebyte!(w, TC_DESCRIBED); writebyte!(w, TC_SMALLULONG); writebyte!(w, SECTION_APPLICATION_PROPERTIES)
        encode!(w, m.application_properties)
    end
    if m.body_type === :data
        for part in m.body
            writebyte!(w, TC_DESCRIBED); writebyte!(w, TC_SMALLULONG); writebyte!(w, SECTION_DATA)
            encode!(w, part::AbstractVector{UInt8})
        end
    elseif m.body_type === :value
        writebyte!(w, TC_DESCRIBED); writebyte!(w, TC_SMALLULONG); writebyte!(w, SECTION_AMQP_VALUE)
        encode!(w, m.body)
    elseif m.body_type === :sequence
        for seq in m.body
            writebyte!(w, TC_DESCRIBED); writebyte!(w, TC_SMALLULONG); writebyte!(w, SECTION_AMQP_SEQUENCE)
            encode_list!(w, seq)
        end
    else
        throw(ArgumentError("invalid body_type :$(m.body_type)"))
    end
    m.footer !== nothing && encode_annotations!(w, SECTION_FOOTER, m.footer)
    return w
end

"Encode a message to bytes (the payload of a transfer)."
function encode_message(m::AMQPMessage)
    w = ByteWriter(512)
    encode_message!(w, m)
    return takebytes!(w)
end

function _as_map(v, what)
    v === nothing && return Dict{Any,Any}()
    v isa AbstractDict || throw(AMQPDecodeError("$what section is not a map"))
    return v isa Dict{Any,Any} ? v : Dict{Any,Any}(v)
end

"""
    decode_message(bytes) -> AMQPMessage

Decode the sections of a message payload.
"""
function decode_message(bytes::AbstractVector{UInt8})
    r = ByteReader(bytes)
    m = AMQPMessage()
    data = Vector{UInt8}[]
    seqs = Vector{Any}[]
    has_value = false
    while !at_end(r)
        readbyte!(r) == TC_DESCRIBED || throw(AMQPDecodeError("message section is not a described type"))
        d = decode_value(r, readbyte!(r), 1)
        d isa UInt64 || throw(AMQPDecodeError("message section descriptor is not numeric"))
        code = d
        if code == 0x70
            m.header = from_list(Header, decode(r)::Vector{Any})
        elseif code == SECTION_DELIVERY_ANNOTATIONS
            m.delivery_annotations = _as_map(decode(r), "delivery-annotations")
        elseif code == SECTION_MESSAGE_ANNOTATIONS
            m.message_annotations = _as_map(decode(r), "message-annotations")
        elseif code == 0x73
            v = decode(r)
            v isa AbstractVector || throw(AMQPDecodeError("properties section is not a list"))
            m.properties = from_list(Properties, v)
        elseif code == SECTION_APPLICATION_PROPERTIES
            v = decode(r)
            v === nothing && continue
            v isa AbstractDict || throw(AMQPDecodeError("application-properties section is not a map"))
            ap = Dict{String,Any}()
            for (k, val) in v
                k isa AbstractString || throw(AMQPDecodeError("application-properties keys must be strings"))
                ap[k] = val
            end
            m.application_properties = ap
        elseif code == SECTION_DATA
            v = decode(r)
            v isa Vector{UInt8} || throw(AMQPDecodeError("data section is not binary"))
            push!(data, v)
        elseif code == SECTION_AMQP_SEQUENCE
            v = decode(r)
            v isa AbstractVector || throw(AMQPDecodeError("amqp-sequence section is not a list"))
            push!(seqs, Vector{Any}(v))
        elseif code == SECTION_AMQP_VALUE
            m.body = decode(r)
            has_value = true
        elseif code == SECTION_FOOTER
            m.footer = _as_map(decode(r), "footer")
        else
            throw(AMQPDecodeError("unknown message section 0x$(string(code, base=16))"))
        end
    end
    if has_value
        m.body_type = :value
    elseif !isempty(seqs)
        m.body_type = :sequence
        m.body = seqs
    else
        m.body_type = :data
        m.body = data
    end
    return m
end

# Microsoft batch message format: the body consists of data sections that each
# hold an encoded AMQP message.
const BATCH_MESSAGE_FORMAT = 0x80013700
