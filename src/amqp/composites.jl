# AMQP 1.0 composite types (described lists): performatives, SASL frames,
# delivery states, terminus definitions, error and message header sections.

abstract type AMQPComposite end

# Field conversion used when decoding composite lists received from the peer.
convert_field(::Type{T}, v) where {T} = v isa T ? v : _convert_field(T, v)

function _convert_field(::Type{T}, v) where {T}
    try
        return convert(T, v)
    catch
        throw(AMQPDecodeError("unexpected value of type $(typeof(v)) for field of type $T"))
    end
end

# "multiple" symbol fields may be sent either as a single symbol or an array.
_convert_field(::Type{Union{Nothing, Vector{Symbol}}}, v::Symbol) = Symbol[v]
function _convert_field(::Type{Union{Nothing, Vector{Symbol}}}, v::AbstractVector)
    all(x -> x isa Symbol, v) || throw(AMQPDecodeError("expected symbols"))
    return Symbol[x for x in v]
end
function _convert_field(::Type{Union{Nothing, Dict{Symbol, Any}}}, v::AbstractDict)
    d = Dict{Symbol, Any}()
    for (k, val) in v
        k isa Symbol || throw(AMQPDecodeError("expected symbol keys in fields map"))
        d[k] = val
    end
    return d
end
_convert_field(::Type{Union{Nothing, Dict{Any, Any}}}, v::AbstractDict) = Dict{Any, Any}(v)
_convert_field(::Type{Union{Nothing, Vector{UInt8}}}, v::AbstractVector{UInt8}) = Vector{UInt8}(v)

# Field encoding: multiple-symbol fields are encoded as arrays.
encode_field!(w::ByteWriter, x) = encode!(w, x)
encode_field!(w::ByteWriter, x::Vector{Symbol}) = encode!(w, AMQPArray{Symbol}(x))

"""
    @composite Name code begin
        field::Type [= default]
        ...
    end

Define an AMQP composite type encoded as a described list with the given
numeric descriptor. Fields without a default are mandatory. Trailing `nothing`
fields are omitted from the encoded list.
"""
macro composite(name, code, block)
    fields = Any[]
    for ex in block.args
        ex isa LineNumberNode && continue
        if ex isa Expr && ex.head == :(=)
            decl, default = ex.args
            push!(fields, (decl.args[1], decl.args[2], default, true))
        elseif ex isa Expr && ex.head == :(::)
            push!(fields, (ex.args[1], ex.args[2], nothing, false))
        else
            error("invalid @composite field: $ex")
        end
    end
    struct_fields = [f[4] ? :($(f[1])::$(f[2]) = $(f[3])) : :($(f[1])::$(f[2])) for f in fields]
    n = length(fields)
    getters = [:(x.$(f[1])) for f in fields]
    decoders = Any[]
    for (i, f) in enumerate(fields)
        fname, ftype, default, hasdefault = f
        missing_expr = hasdefault ? default :
            :(throw(AMQPDecodeError($("mandatory field $(fname) missing in $(name)"))))
        push!(
            decoders, :(
                $fname = let v = $i <= length(list) ? list[$i] : nothing
                    v === nothing ? $missing_expr : convert_field($ftype, v)
                end
            )
        )
    end
    return esc(
        quote
            Base.@kwdef struct $name <: AMQPComposite
                $(struct_fields...)
            end
            descriptor_code(::Type{$name}) = UInt64($code)
            descriptor_code(::$name) = UInt64($code)
            function encode!(w::ByteWriter, x::$name)
                writebyte!(w, TC_DESCRIBED)
                writebyte!(w, TC_SMALLULONG)
                writebyte!(w, UInt8($code))
                vals = ($(getters...),)
                last = 0
                for i in 1:$n
                    vals[i] === nothing || (last = i)
                end
                if last == 0
                    writebyte!(w, TC_LIST0)
                    return w
                end
                start = begin_compound!(w)
                for i in 1:last
                    encode_field!(w, vals[i])
                end
                return end_compound!(w, start, last, TC_LIST8, TC_LIST32)
            end
            function from_list(::Type{$name}, list::AbstractVector)
                $(decoders...)
                return $name(; $([Expr(:kw, f[1], f[1]) for f in fields]...))
            end
            DESCRIBED_DECODERS[UInt64($code)] = function (v)
                v isa AbstractVector || throw(AMQPDecodeError($("expected list body for $(name)")))
                return from_list($name, v)
            end
        end
    )
end

const MaybeSymbols = Union{Nothing, Vector{Symbol}}
const MaybeFields = Union{Nothing, Dict{Symbol, Any}}
const MaybeMap = Union{Nothing, Dict{Any, Any}}

# --- error -------------------------------------------------------------------

@composite AMQPError 0x1d begin
    condition::Symbol
    description::Union{Nothing, String} = nothing
    info::MaybeMap = nothing
end

# --- delivery states ------------------------------------------------------------

@composite Received 0x23 begin
    section_number::UInt32
    section_offset::UInt64
end

@composite Accepted 0x24 begin end
@composite Rejected 0x25 begin
    error::Union{Nothing, AMQPError} = nothing
end
@composite Released 0x26 begin end
@composite Modified 0x27 begin
    delivery_failed::Union{Nothing, Bool} = nothing
    undeliverable_here::Union{Nothing, Bool} = nothing
    message_annotations::MaybeMap = nothing
end

const Outcome = Union{Accepted, Rejected, Released, Modified}

# --- terminus -----------------------------------------------------------------

@composite Source 0x28 begin
    address::Any = nothing
    durable::Union{Nothing, UInt32} = nothing
    expiry_policy::Union{Nothing, Symbol} = nothing
    timeout::Union{Nothing, UInt32} = nothing
    dynamic::Union{Nothing, Bool} = nothing
    dynamic_node_properties::MaybeFields = nothing
    distribution_mode::Union{Nothing, Symbol} = nothing
    filter::MaybeMap = nothing
    default_outcome::Any = nothing
    outcomes::MaybeSymbols = nothing
    capabilities::MaybeSymbols = nothing
end

@composite Target 0x29 begin
    address::Any = nothing
    durable::Union{Nothing, UInt32} = nothing
    expiry_policy::Union{Nothing, Symbol} = nothing
    timeout::Union{Nothing, UInt32} = nothing
    dynamic::Union{Nothing, Bool} = nothing
    dynamic_node_properties::MaybeFields = nothing
    capabilities::MaybeSymbols = nothing
end

# --- performatives ----------------------------------------------------------------

@composite Open 0x10 begin
    container_id::String
    hostname::Union{Nothing, String} = nothing
    max_frame_size::Union{Nothing, UInt32} = nothing
    channel_max::Union{Nothing, UInt16} = nothing
    idle_time_out::Union{Nothing, UInt32} = nothing
    outgoing_locales::MaybeSymbols = nothing
    incoming_locales::MaybeSymbols = nothing
    offered_capabilities::MaybeSymbols = nothing
    desired_capabilities::MaybeSymbols = nothing
    properties::MaybeFields = nothing
end

@composite Begin 0x11 begin
    remote_channel::Union{Nothing, UInt16} = nothing
    next_outgoing_id::UInt32
    incoming_window::UInt32
    outgoing_window::UInt32
    handle_max::Union{Nothing, UInt32} = nothing
    offered_capabilities::MaybeSymbols = nothing
    desired_capabilities::MaybeSymbols = nothing
    properties::MaybeFields = nothing
end

@composite Attach 0x12 begin
    name::String
    handle::UInt32
    role::Bool
    snd_settle_mode::Union{Nothing, UInt8} = nothing
    rcv_settle_mode::Union{Nothing, UInt8} = nothing
    source::Any = nothing
    target::Any = nothing
    unsettled::MaybeMap = nothing
    incomplete_unsettled::Union{Nothing, Bool} = nothing
    initial_delivery_count::Union{Nothing, UInt32} = nothing
    max_message_size::Union{Nothing, UInt64} = nothing
    offered_capabilities::MaybeSymbols = nothing
    desired_capabilities::MaybeSymbols = nothing
    properties::MaybeFields = nothing
end

@composite Flow 0x13 begin
    next_incoming_id::Union{Nothing, UInt32} = nothing
    incoming_window::UInt32
    next_outgoing_id::UInt32
    outgoing_window::UInt32
    handle::Union{Nothing, UInt32} = nothing
    delivery_count::Union{Nothing, UInt32} = nothing
    link_credit::Union{Nothing, UInt32} = nothing
    available::Union{Nothing, UInt32} = nothing
    drain::Union{Nothing, Bool} = nothing
    echo::Union{Nothing, Bool} = nothing
    properties::MaybeFields = nothing
end

@composite Transfer 0x14 begin
    handle::UInt32
    delivery_id::Union{Nothing, UInt32} = nothing
    delivery_tag::Union{Nothing, Vector{UInt8}} = nothing
    message_format::Union{Nothing, UInt32} = nothing
    settled::Union{Nothing, Bool} = nothing
    more::Union{Nothing, Bool} = nothing
    rcv_settle_mode::Union{Nothing, UInt8} = nothing
    state::Any = nothing
    resume::Union{Nothing, Bool} = nothing
    aborted::Union{Nothing, Bool} = nothing
    batchable::Union{Nothing, Bool} = nothing
end

@composite Disposition 0x15 begin
    role::Bool
    first::UInt32
    last::Union{Nothing, UInt32} = nothing
    settled::Union{Nothing, Bool} = nothing
    state::Any = nothing
    batchable::Union{Nothing, Bool} = nothing
end

@composite Detach 0x16 begin
    handle::UInt32
    closed::Union{Nothing, Bool} = nothing
    error::Union{Nothing, AMQPError} = nothing
end

@composite End 0x17 begin
    error::Union{Nothing, AMQPError} = nothing
end

@composite Close 0x18 begin
    error::Union{Nothing, AMQPError} = nothing
end

# --- SASL -----------------------------------------------------------------------

@composite SaslMechanisms 0x40 begin
    sasl_server_mechanisms::Vector{Symbol}
end

@composite SaslInit 0x41 begin
    mechanism::Symbol
    initial_response::Union{Nothing, Vector{UInt8}} = nothing
    hostname::Union{Nothing, String} = nothing
end

@composite SaslChallenge 0x42 begin
    challenge::Vector{UInt8}
end

@composite SaslResponse 0x43 begin
    response::Vector{UInt8}
end

@composite SaslOutcome 0x44 begin
    code::UInt8
    additional_data::Union{Nothing, Vector{UInt8}} = nothing
end

# SaslMechanisms has a mandatory "multiple symbol" field.
_convert_field(::Type{Vector{Symbol}}, v::Symbol) = Symbol[v]
_convert_field(::Type{Vector{Symbol}}, v::AbstractVector) = Symbol[Symbol(x) for x in v]

# --- message sections with list bodies ----------------------------------------------

@composite Header 0x70 begin
    durable::Union{Nothing, Bool} = nothing
    priority::Union{Nothing, UInt8} = nothing
    ttl::Union{Nothing, UInt32} = nothing
    first_acquirer::Union{Nothing, Bool} = nothing
    delivery_count::Union{Nothing, UInt32} = nothing
end

@composite Properties 0x73 begin
    message_id::Any = nothing
    user_id::Union{Nothing, Vector{UInt8}} = nothing
    to::Any = nothing
    subject::Union{Nothing, String} = nothing
    reply_to::Any = nothing
    correlation_id::Any = nothing
    content_type::Union{Nothing, Symbol} = nothing
    content_encoding::Union{Nothing, Symbol} = nothing
    absolute_expiry_time::Union{Nothing, Dates.DateTime} = nothing
    creation_time::Union{Nothing, Dates.DateTime} = nothing
    group_id::Union{Nothing, String} = nothing
    group_sequence::Union{Nothing, UInt32} = nothing
    reply_to_group_id::Union{Nothing, String} = nothing
end

# Sections whose bodies are not lists (maps, binary, arbitrary values) are
# handled by the message codec directly.
const SECTION_DELIVERY_ANNOTATIONS = 0x71
const SECTION_MESSAGE_ANNOTATIONS = 0x72
const SECTION_APPLICATION_PROPERTIES = 0x74
const SECTION_DATA = 0x75
const SECTION_AMQP_SEQUENCE = 0x76
const SECTION_AMQP_VALUE = 0x77
const SECTION_FOOTER = 0x78

Base.:(==)(a::T, b::T) where {T <: AMQPComposite} = all(getfield(a, f) == getfield(b, f) for f in fieldnames(T))
Base.hash(a::AMQPComposite, h::UInt) = hash(ntuple(i -> getfield(a, i), fieldcount(typeof(a))), hash(typeof(a), h))

"""
    with_fields(x::AMQPComposite; kwargs...)

Copy of the (immutable) composite `x` with the given fields replaced.
"""
function with_fields(x::T; kw...) where {T <: AMQPComposite}
    vals = Dict{Symbol, Any}(f => getfield(x, f) for f in fieldnames(T))
    for (k, v) in kw
        vals[k] = v
    end
    return T(; vals...)
end
