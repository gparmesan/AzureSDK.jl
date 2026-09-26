# AMQP 1.0 type system encoding and decoding (OASIS AMQP 1.0, Part 1: Types).
#
# Julia <-> AMQP mapping used by the generic encoder/decoder:
#
#   nothing            <-> null
#   Bool               <-> boolean
#   UInt8/16/32/64     <-> ubyte/ushort/uint/ulong
#   Int8/16/32/64      <-> byte/short/int/long
#   Float32/Float64    <-> float/double
#   Char               <-> char
#   Dates.DateTime     <-> timestamp (millisecond precision, UTC)
#   UUID               <-> uuid
#   Vector{UInt8}      <-> binary
#   String             <-> string
#   Symbol             <-> symbol
#   Vector{Any} / Tuple <-> list
#   AbstractDict       <-> map
#   AMQPArray{T}       <-> array (decoded arrays become plain `Vector{T}`)
#   AMQPDescribed      <-> described type with an unknown descriptor
#
# The decoder is defensive: every read is bounds checked, declared element
# counts are validated against the bytes actually available before anything is
# allocated, and nesting depth is limited, so a malicious or corrupt frame can
# raise an `AMQPDecodeError` but cannot exhaust memory or the stack.

"""
    AMQPDecodeError(msg)

Raised when bytes received from the peer are not valid AMQP 1.0 encodings.
"""
struct AMQPDecodeError <: Exception
    msg::String
end
Base.showerror(io::IO, e::AMQPDecodeError) = print(io, "AMQPDecodeError: ", e.msg)

"""
    AMQPDescribed(descriptor, value)

A described value whose descriptor is not one of the composite types known to
this implementation (for example a filter such as
`apache.org:selector-filter:string`).
"""
struct AMQPDescribed
    descriptor::Any
    value::Any
end
Base.:(==)(a::AMQPDescribed, b::AMQPDescribed) = a.descriptor == b.descriptor && a.value == b.value
Base.hash(a::AMQPDescribed, h::UInt) = hash(a.value, hash(a.descriptor, hash(:AMQPDescribed, h)))

"""
    AMQPArray(elements::AbstractVector)

Wrapper that forces a Julia vector to be encoded as an AMQP *array* (all
elements share one constructor) instead of an AMQP *list*. Several Azure
management operations require arrays, e.g. `lock-tokens` (array of uuid) or
`sequence-numbers` (array of long).
"""
struct AMQPArray{T}
    elements::Vector{T}
end
AMQPArray(v::AbstractVector{T}) where {T} = AMQPArray{T}(collect(v))
Base.:(==)(a::AMQPArray, b::AMQPArray) = a.elements == b.elements

"""
    AMQPDecimal(code, bytes)

Opaque holder for the (rarely used) decimal32/64/128 AMQP types.
"""
struct AMQPDecimal
    code::UInt8
    bytes::Vector{UInt8}
end

# ---------------------------------------------------------------------------
# Writer
# ---------------------------------------------------------------------------

"""
Growable byte buffer used for encoding. Much cheaper than `IOBuffer` for the
many tiny writes AMQP encoding performs.
"""
mutable struct ByteWriter
    data::Vector{UInt8}
    len::Int
end
ByteWriter(capacity::Integer=256) = ByteWriter(Vector{UInt8}(undef, capacity), 0)

@inline function ensure!(w::ByteWriter, n::Int)
    need = w.len + n
    if need > length(w.data)
        resize!(w.data, max(need, 2 * length(w.data)))
    end
    return w
end

@inline function writebyte!(w::ByteWriter, b::UInt8)
    ensure!(w, 1)
    w.len += 1
    @inbounds w.data[w.len] = b
    return w
end

@inline function writebe!(w::ByteWriter, x::T) where {T<:Union{UInt16,UInt32,UInt64,UInt128}}
    n = sizeof(T)
    ensure!(w, n)
    base = w.len
    @inbounds for i in 1:n
        w.data[base+i] = (x >> (8 * (n - i))) % UInt8
    end
    w.len += n
    return w
end
@inline writebe!(w::ByteWriter, x::Int16) = writebe!(w, reinterpret(UInt16, x))
@inline writebe!(w::ByteWriter, x::Int32) = writebe!(w, reinterpret(UInt32, x))
@inline writebe!(w::ByteWriter, x::Int64) = writebe!(w, reinterpret(UInt64, x))
@inline writebe!(w::ByteWriter, x::Float32) = writebe!(w, reinterpret(UInt32, x))
@inline writebe!(w::ByteWriter, x::Float64) = writebe!(w, reinterpret(UInt64, x))

@inline function writebytes!(w::ByteWriter, v::AbstractVector{UInt8})
    n = length(v)
    ensure!(w, n)
    copyto!(w.data, w.len + 1, v, firstindex(v), n)
    w.len += n
    return w
end
function writebytes!(w::ByteWriter, s::String)
    n = sizeof(s)
    ensure!(w, n)
    GC.@preserve s w unsafe_copyto!(pointer(w.data, w.len + 1), pointer(s), n)
    w.len += n
    return w
end

@inline function patchbe32!(w::ByteWriter, pos::Int, x::UInt32)
    @inbounds for i in 1:4
        w.data[pos+i-1] = (x >> (8 * (4 - i))) % UInt8
    end
end

"Return the encoded bytes (the writer must not be used afterwards)."
function takebytes!(w::ByteWriter)
    data = w.data
    resize!(data, w.len)
    w.data = UInt8[]
    w.len = 0
    return data
end

# ---------------------------------------------------------------------------
# Constructor codes
# ---------------------------------------------------------------------------

const TC_DESCRIBED = 0x00
const TC_NULL = 0x40
const TC_TRUE = 0x41
const TC_FALSE = 0x42
const TC_BOOLEAN = 0x56
const TC_UBYTE = 0x50
const TC_USHORT = 0x60
const TC_UINT = 0x70
const TC_SMALLUINT = 0x52
const TC_UINT0 = 0x43
const TC_ULONG = 0x80
const TC_SMALLULONG = 0x53
const TC_ULONG0 = 0x44
const TC_BYTE = 0x51
const TC_SHORT = 0x61
const TC_INT = 0x71
const TC_SMALLINT = 0x54
const TC_LONG = 0x81
const TC_SMALLLONG = 0x55
const TC_FLOAT = 0x72
const TC_DOUBLE = 0x82
const TC_DECIMAL32 = 0x74
const TC_DECIMAL64 = 0x84
const TC_DECIMAL128 = 0x94
const TC_CHAR = 0x73
const TC_TIMESTAMP = 0x83
const TC_UUID = 0x98
const TC_VBIN8 = 0xa0
const TC_VBIN32 = 0xb0
const TC_STR8 = 0xa1
const TC_STR32 = 0xb1
const TC_SYM8 = 0xa3
const TC_SYM32 = 0xb3
const TC_LIST0 = 0x45
const TC_LIST8 = 0xc0
const TC_LIST32 = 0xd0
const TC_MAP8 = 0xc1
const TC_MAP32 = 0xd1
const TC_ARRAY8 = 0xe0
const TC_ARRAY32 = 0xf0

const UNIX_EPOCH = Dates.DateTime(1970, 1, 1)

@inline datetime_to_ms(dt::Dates.DateTime) = Int64(Dates.value(dt - UNIX_EPOCH))
@inline ms_to_datetime(ms::Int64) = UNIX_EPOCH + Dates.Millisecond(ms)

# ---------------------------------------------------------------------------
# Generic encoder
# ---------------------------------------------------------------------------

"""
    encode!(w::ByteWriter, value)

Append the AMQP encoding of `value` to `w`.
"""
function encode! end

encode!(w::ByteWriter, ::Nothing) = writebyte!(w, TC_NULL)
encode!(w::ByteWriter, x::Bool) = writebyte!(w, x ? TC_TRUE : TC_FALSE)
encode!(w::ByteWriter, x::UInt8) = (writebyte!(w, TC_UBYTE); writebyte!(w, x))
encode!(w::ByteWriter, x::UInt16) = (writebyte!(w, TC_USHORT); writebe!(w, x))
function encode!(w::ByteWriter, x::UInt32)
    if x == 0
        writebyte!(w, TC_UINT0)
    elseif x < 0x100
        writebyte!(w, TC_SMALLUINT); writebyte!(w, x % UInt8)
    else
        writebyte!(w, TC_UINT); writebe!(w, x)
    end
end
function encode!(w::ByteWriter, x::UInt64)
    if x == 0
        writebyte!(w, TC_ULONG0)
    elseif x < 0x100
        writebyte!(w, TC_SMALLULONG); writebyte!(w, x % UInt8)
    else
        writebyte!(w, TC_ULONG); writebe!(w, x)
    end
end
encode!(w::ByteWriter, x::Int8) = (writebyte!(w, TC_BYTE); writebyte!(w, reinterpret(UInt8, x)))
encode!(w::ByteWriter, x::Int16) = (writebyte!(w, TC_SHORT); writebe!(w, x))
function encode!(w::ByteWriter, x::Int32)
    if -128 <= x <= 127
        writebyte!(w, TC_SMALLINT); writebyte!(w, reinterpret(UInt8, x % Int8))
    else
        writebyte!(w, TC_INT); writebe!(w, x)
    end
end
function encode!(w::ByteWriter, x::Int64)
    if -128 <= x <= 127
        writebyte!(w, TC_SMALLLONG); writebyte!(w, reinterpret(UInt8, x % Int8))
    else
        writebyte!(w, TC_LONG); writebe!(w, x)
    end
end
encode!(w::ByteWriter, x::Float32) = (writebyte!(w, TC_FLOAT); writebe!(w, x))
encode!(w::ByteWriter, x::Float64) = (writebyte!(w, TC_DOUBLE); writebe!(w, x))
encode!(w::ByteWriter, x::Char) = (writebyte!(w, TC_CHAR); writebe!(w, UInt32(x)))
encode!(w::ByteWriter, x::Dates.DateTime) = (writebyte!(w, TC_TIMESTAMP); writebe!(w, datetime_to_ms(x)))
encode!(w::ByteWriter, x::UUID) = (writebyte!(w, TC_UUID); writebe!(w, x.value))
function encode!(w::ByteWriter, x::AMQPDecimal)
    writebyte!(w, x.code)
    writebytes!(w, x.bytes)
end

function encode!(w::ByteWriter, x::AbstractVector{UInt8})
    n = length(x)
    if n < 0x100
        writebyte!(w, TC_VBIN8); writebyte!(w, n % UInt8)
    else
        n <= typemax(UInt32) || throw(ArgumentError("binary value too large for AMQP"))
        writebyte!(w, TC_VBIN32); writebe!(w, UInt32(n))
    end
    writebytes!(w, x)
end

function encode!(w::ByteWriter, x::AbstractString)
    s = String(x)
    n = sizeof(s)
    if n < 0x100
        writebyte!(w, TC_STR8); writebyte!(w, n % UInt8)
    else
        writebyte!(w, TC_STR32); writebe!(w, UInt32(n))
    end
    writebytes!(w, s)
end

function encode!(w::ByteWriter, x::Symbol)
    s = String(x)
    n = sizeof(s)
    if n < 0x100
        writebyte!(w, TC_SYM8); writebyte!(w, n % UInt8)
    else
        writebyte!(w, TC_SYM32); writebe!(w, UInt32(n))
    end
    writebytes!(w, s)
end

function encode!(w::ByteWriter, x::AMQPDescribed)
    writebyte!(w, TC_DESCRIBED)
    encode!(w, x.descriptor)
    encode!(w, x.value)
end

# Composite (list/map) encoding reserves a list32/map32 header, writes the
# elements and then compacts to the 8-bit form when it fits.
@inline function begin_compound!(w::ByteWriter)
    start = w.len + 1
    ensure!(w, 9)
    w.len += 9
    return start
end

function end_compound!(w::ByteWriter, start::Int, count::Int, code8::UInt8, code32::UInt8)
    body = w.len - (start + 8)          # bytes of the encoded elements
    if body + 1 < 0x100 && count < 0x100
        # list8/map8: code, size(1), count(1)  -> shift elements 6 bytes left
        @inbounds w.data[start] = code8
        @inbounds w.data[start+1] = (body + 1) % UInt8
        @inbounds w.data[start+2] = count % UInt8
        if body > 0
            copyto!(w.data, start + 3, w.data, start + 9, body)
        end
        w.len -= 6
    else
        @inbounds w.data[start] = code32
        patchbe32!(w, start + 1, UInt32(body + 4))
        patchbe32!(w, start + 5, UInt32(count))
    end
    return w
end

function encode_list!(w::ByteWriter, items)
    if isempty(items)
        return writebyte!(w, TC_LIST0)
    end
    start = begin_compound!(w)
    count = 0
    for item in items
        encode!(w, item)
        count += 1
    end
    end_compound!(w, start, count, TC_LIST8, TC_LIST32)
end

encode!(w::ByteWriter, x::AbstractVector) = encode_list!(w, x)
encode!(w::ByteWriter, x::Tuple) = encode_list!(w, x)

function encode!(w::ByteWriter, x::AbstractDict)
    start = begin_compound!(w)
    for (k, v) in x
        encode!(w, k)
        encode!(w, v)
    end
    end_compound!(w, start, 2 * length(x), TC_MAP8, TC_MAP32)
end

# Map where the keys are encoded as AMQP symbols (annotations, fields).
struct SymbolKeyed{D}
    dict::D
end
function encode!(w::ByteWriter, x::SymbolKeyed)
    start = begin_compound!(w)
    for (k, v) in x.dict
        encode!(w, k isa Symbol ? k : Symbol(k))
        encode!(w, v)
    end
    end_compound!(w, start, 2 * length(x.dict), TC_MAP8, TC_MAP32)
end

# --- arrays -----------------------------------------------------------------

array_ctor(::Type{Bool}, _) = TC_BOOLEAN
array_ctor(::Type{UInt8}, _) = TC_UBYTE
array_ctor(::Type{UInt16}, _) = TC_USHORT
array_ctor(::Type{UInt32}, _) = TC_UINT
array_ctor(::Type{UInt64}, _) = TC_ULONG
array_ctor(::Type{Int8}, _) = TC_BYTE
array_ctor(::Type{Int16}, _) = TC_SHORT
array_ctor(::Type{Int32}, _) = TC_INT
array_ctor(::Type{Int64}, _) = TC_LONG
array_ctor(::Type{Float32}, _) = TC_FLOAT
array_ctor(::Type{Float64}, _) = TC_DOUBLE
array_ctor(::Type{Char}, _) = TC_CHAR
array_ctor(::Type{Dates.DateTime}, _) = TC_TIMESTAMP
array_ctor(::Type{UUID}, _) = TC_UUID
array_ctor(::Type{Symbol}, v) = all(s -> sizeof(String(s)) < 0x100, v) ? TC_SYM8 : TC_SYM32
array_ctor(::Type{<:AbstractString}, v) = all(s -> sizeof(s) < 0x100, v) ? TC_STR8 : TC_STR32
array_ctor(::Type{<:AbstractVector{UInt8}}, v) = all(s -> length(s) < 0x100, v) ? TC_VBIN8 : TC_VBIN32
array_ctor(::Type{T}, _) where {T} = throw(ArgumentError("cannot encode AMQP array of $T"))

function encode_array_element!(w::ByteWriter, code::UInt8, x)
    if code == TC_BOOLEAN
        writebyte!(w, x ? 0x01 : 0x00)
    elseif code == TC_UBYTE || code == TC_BYTE
        writebyte!(w, reinterpret(UInt8, x))
    elseif code == TC_SYM8 || code == TC_STR8
        s = String(x); writebyte!(w, sizeof(s) % UInt8); writebytes!(w, s)
    elseif code == TC_SYM32 || code == TC_STR32
        s = String(x); writebe!(w, UInt32(sizeof(s))); writebytes!(w, s)
    elseif code == TC_VBIN8
        writebyte!(w, length(x) % UInt8); writebytes!(w, x)
    elseif code == TC_VBIN32
        writebe!(w, UInt32(length(x))); writebytes!(w, x)
    elseif code == TC_TIMESTAMP
        writebe!(w, datetime_to_ms(x))
    elseif code == TC_UUID
        writebe!(w, x.value)
    elseif code == TC_CHAR
        writebe!(w, UInt32(x))
    else
        writebe!(w, x)
    end
end

function encode!(w::ByteWriter, x::AMQPArray{T}) where {T}
    code = array_ctor(T, x.elements)
    start = begin_compound!(w)
    writebyte!(w, code)
    for el in x.elements
        encode_array_element!(w, code, el)
    end
    end_compound!(w, start, length(x.elements), TC_ARRAY8, TC_ARRAY32)
end

"Encode `value` into a fresh byte vector."
function encode(value)
    w = ByteWriter()
    encode!(w, value)
    return takebytes!(w)
end

# ---------------------------------------------------------------------------
# Reader / decoder
# ---------------------------------------------------------------------------

const MAX_DECODE_DEPTH = 64

mutable struct ByteReader{V<:AbstractVector{UInt8}}
    data::V
    pos::Int      # index of next byte to read
    stop::Int     # last readable index
end
ByteReader(data::AbstractVector{UInt8}) = ByteReader(data, firstindex(data), lastindex(data))
ByteReader(data::AbstractVector{UInt8}, pos::Int) = ByteReader(data, pos, lastindex(data))

@inline remaining(r::ByteReader) = r.stop - r.pos + 1
@inline at_end(r::ByteReader) = r.pos > r.stop

@noinline throw_truncated() = throw(AMQPDecodeError("truncated AMQP data"))

@inline function need(r::ByteReader, n::Integer)
    (n < 0 || remaining(r) < n) && throw_truncated()
    return nothing
end

@inline function readbyte!(r::ByteReader)
    need(r, 1)
    @inbounds b = r.data[r.pos]
    r.pos += 1
    return b
end

@inline function readbe!(r::ByteReader, ::Type{T}) where {T<:Union{UInt16,UInt32,UInt64,UInt128}}
    n = sizeof(T)
    need(r, n)
    x = zero(T)
    @inbounds for i in 0:n-1
        x = (x << 8) | T(r.data[r.pos+i])
    end
    r.pos += n
    return x
end
@inline readbe!(r::ByteReader, ::Type{Int16}) = reinterpret(Int16, readbe!(r, UInt16))
@inline readbe!(r::ByteReader, ::Type{Int32}) = reinterpret(Int32, readbe!(r, UInt32))
@inline readbe!(r::ByteReader, ::Type{Int64}) = reinterpret(Int64, readbe!(r, UInt64))
@inline readbe!(r::ByteReader, ::Type{Float32}) = reinterpret(Float32, readbe!(r, UInt32))
@inline readbe!(r::ByteReader, ::Type{Float64}) = reinterpret(Float64, readbe!(r, UInt64))

@inline function read_nbytes!(r::ByteReader, n::Integer)
    need(r, n)
    v = r.data[r.pos:r.pos+n-1]
    r.pos += n
    return Vector{UInt8}(v)
end

function readstring!(r::ByteReader, n::Integer)
    need(r, n)
    s = GC.@preserve r unsafe_string(pointer(r.data, r.pos), n)
    r.pos += n
    isvalid(s) || throw(AMQPDecodeError("invalid UTF-8 in AMQP string"))
    return s
end

readsymbol!(r::ByteReader, n::Integer) = Symbol(readstring!(r, n))

@inline function checked_char(u::UInt32)
    (u > 0x10ffff || 0xd800 <= u <= 0xdfff) && throw(AMQPDecodeError("invalid AMQP char"))
    return Char(u)
end

# Hook for composite types: code => (list -> value). Populated by the
# performative/section definitions.
const DESCRIBED_DECODERS = Dict{UInt64,Function}()

"""
    decode(r::ByteReader) -> value

Decode one AMQP value from `r`.
"""
decode(r::ByteReader) = decode_value(r, readbyte!(r), 0)
decode(bytes::AbstractVector{UInt8}) = decode(ByteReader(bytes))

function decode_value(r::ByteReader, code::UInt8, depth::Int)
    depth > MAX_DECODE_DEPTH && throw(AMQPDecodeError("AMQP value nested too deeply"))
    if code == TC_DESCRIBED
        descriptor = decode_value(r, readbyte!(r), depth + 1)
        dcode = descriptor isa UInt64 ? descriptor : nothing
        if dcode !== nothing
            dec = get(DESCRIBED_DECODERS, dcode, nothing)
            if dec !== nothing
                value = decode_value(r, readbyte!(r), depth + 1)
                return dec(value)
            end
        end
        value = decode_value(r, readbyte!(r), depth + 1)
        return AMQPDescribed(descriptor, value)
    elseif code == TC_NULL
        return nothing
    elseif code == TC_TRUE
        return true
    elseif code == TC_FALSE
        return false
    elseif code == TC_BOOLEAN
        b = readbyte!(r)
        b > 0x01 && throw(AMQPDecodeError("invalid boolean"))
        return b == 0x01
    elseif code == TC_UBYTE
        return readbyte!(r)
    elseif code == TC_USHORT
        return readbe!(r, UInt16)
    elseif code == TC_UINT
        return readbe!(r, UInt32)
    elseif code == TC_SMALLUINT
        return UInt32(readbyte!(r))
    elseif code == TC_UINT0
        return UInt32(0)
    elseif code == TC_ULONG
        return readbe!(r, UInt64)
    elseif code == TC_SMALLULONG
        return UInt64(readbyte!(r))
    elseif code == TC_ULONG0
        return UInt64(0)
    elseif code == TC_BYTE
        return reinterpret(Int8, readbyte!(r))
    elseif code == TC_SHORT
        return readbe!(r, Int16)
    elseif code == TC_INT
        return readbe!(r, Int32)
    elseif code == TC_SMALLINT
        return Int32(reinterpret(Int8, readbyte!(r)))
    elseif code == TC_LONG
        return readbe!(r, Int64)
    elseif code == TC_SMALLLONG
        return Int64(reinterpret(Int8, readbyte!(r)))
    elseif code == TC_FLOAT
        return readbe!(r, Float32)
    elseif code == TC_DOUBLE
        return readbe!(r, Float64)
    elseif code == TC_CHAR
        return checked_char(readbe!(r, UInt32))
    elseif code == TC_TIMESTAMP
        return ms_to_datetime(readbe!(r, Int64))
    elseif code == TC_UUID
        return UUID(readbe!(r, UInt128))
    elseif code == TC_DECIMAL32
        return AMQPDecimal(code, read_nbytes!(r, 4))
    elseif code == TC_DECIMAL64
        return AMQPDecimal(code, read_nbytes!(r, 8))
    elseif code == TC_DECIMAL128
        return AMQPDecimal(code, read_nbytes!(r, 16))
    elseif code == TC_VBIN8
        return read_nbytes!(r, readbyte!(r))
    elseif code == TC_VBIN32
        return read_nbytes!(r, readbe!(r, UInt32))
    elseif code == TC_STR8
        return readstring!(r, readbyte!(r))
    elseif code == TC_STR32
        return readstring!(r, readbe!(r, UInt32))
    elseif code == TC_SYM8
        return readsymbol!(r, readbyte!(r))
    elseif code == TC_SYM32
        return readsymbol!(r, readbe!(r, UInt32))
    elseif code == TC_LIST0
        return Any[]
    elseif code == TC_LIST8 || code == TC_LIST32
        return decode_list(r, code == TC_LIST8, depth)
    elseif code == TC_MAP8 || code == TC_MAP32
        return decode_map(r, code == TC_MAP8, depth)
    elseif code == TC_ARRAY8 || code == TC_ARRAY32
        return decode_array(r, code == TC_ARRAY8, depth)
    else
        throw(AMQPDecodeError("unknown AMQP type constructor 0x$(string(code, base=16))"))
    end
end

# Reads a compound header and returns (count, end_pos).
@inline function compound_header!(r::ByteReader, small::Bool)
    if small
        size = Int(readbyte!(r))
        need(r, size)
        size < 1 && throw(AMQPDecodeError("invalid compound size"))
        count = Int(readbyte!(r))
        endpos = r.pos + size - 2
    else
        size = Int(readbe!(r, UInt32))
        need(r, size)
        size < 4 && throw(AMQPDecodeError("invalid compound size"))
        count = Int(readbe!(r, UInt32))
        endpos = r.pos + size - 5
    end
    # every element occupies at least one byte (arrays are checked separately)
    return count, endpos
end

function decode_list(r::ByteReader, small::Bool, depth::Int)
    count, endpos = compound_header!(r, small)
    count > endpos - r.pos + 1 && throw(AMQPDecodeError("AMQP list count exceeds its size"))
    items = Vector{Any}(undef, count)
    sub = ByteReader(r.data, r.pos, endpos)
    for i in 1:count
        items[i] = decode_value(sub, readbyte!(sub), depth + 1)
    end
    at_end(sub) || throw(AMQPDecodeError("trailing bytes in AMQP list"))
    r.pos = endpos + 1
    return items
end

function decode_map(r::ByteReader, small::Bool, depth::Int)
    count, endpos = compound_header!(r, small)
    isodd(count) && throw(AMQPDecodeError("AMQP map with odd element count"))
    count > endpos - r.pos + 1 && throw(AMQPDecodeError("AMQP map count exceeds its size"))
    d = Dict{Any,Any}()
    sizehint!(d, count ÷ 2)
    sub = ByteReader(r.data, r.pos, endpos)
    for _ in 1:(count÷2)
        k = decode_value(sub, readbyte!(sub), depth + 1)
        v = decode_value(sub, readbyte!(sub), depth + 1)
        d[k] = v
    end
    at_end(sub) || throw(AMQPDecodeError("trailing bytes in AMQP map"))
    r.pos = endpos + 1
    return d
end

# Minimum encoded size of one array element for a given constructor, used to
# validate the declared count before allocating.
function array_element_min_size(code::UInt8)
    code in (TC_NULL, TC_TRUE, TC_FALSE, TC_UINT0, TC_ULONG0, TC_LIST0) && return 0
    code in (TC_BOOLEAN, TC_UBYTE, TC_BYTE, TC_SMALLUINT, TC_SMALLULONG, TC_SMALLINT, TC_SMALLLONG,
        TC_VBIN8, TC_STR8, TC_SYM8) && return 1
    code in (TC_USHORT, TC_SHORT) && return 2
    code in (TC_UINT, TC_INT, TC_FLOAT, TC_CHAR, TC_DECIMAL32, TC_VBIN32, TC_STR32, TC_SYM32) && return 4
    code in (TC_ULONG, TC_LONG, TC_DOUBLE, TC_TIMESTAMP, TC_DECIMAL64) && return 8
    code in (TC_UUID, TC_DECIMAL128) && return 16
    code in (TC_LIST8, TC_MAP8, TC_ARRAY8) && return 2
    code in (TC_LIST32, TC_MAP32, TC_ARRAY32) && return 8
    return 1
end

function array_eltype(code::UInt8)
    code == TC_BOOLEAN && return Bool
    code in (TC_TRUE, TC_FALSE) && return Bool
    code == TC_UBYTE && return UInt8
    code == TC_USHORT && return UInt16
    code in (TC_UINT, TC_SMALLUINT, TC_UINT0) && return UInt32
    code in (TC_ULONG, TC_SMALLULONG, TC_ULONG0) && return UInt64
    code == TC_BYTE && return Int8
    code == TC_SHORT && return Int16
    code in (TC_INT, TC_SMALLINT) && return Int32
    code in (TC_LONG, TC_SMALLLONG) && return Int64
    code == TC_FLOAT && return Float32
    code == TC_DOUBLE && return Float64
    code == TC_CHAR && return Char
    code == TC_TIMESTAMP && return Dates.DateTime
    code == TC_UUID && return UUID
    code in (TC_VBIN8, TC_VBIN32) && return Vector{UInt8}
    code in (TC_STR8, TC_STR32) && return String
    code in (TC_SYM8, TC_SYM32) && return Symbol
    return Any
end

function decode_array(r::ByteReader, small::Bool, depth::Int)
    count, endpos = compound_header!(r, small)
    sub = ByteReader(r.data, r.pos, endpos)
    code = readbyte!(sub)
    descriptor = nothing
    if code == TC_DESCRIBED
        descriptor = decode_value(sub, readbyte!(sub), depth + 1)
        code = readbyte!(sub)
    end
    minsize = array_element_min_size(code)
    if minsize > 0
        count * minsize > remaining(sub) && throw(AMQPDecodeError("AMQP array count exceeds its size"))
    else
        count > 1 << 20 && throw(AMQPDecodeError("AMQP array too long"))
    end
    T = descriptor === nothing ? array_eltype(code) : Any
    items = Vector{T}(undef, count)
    for i in 1:count
        v = decode_value(sub, code, depth + 1)
        if descriptor !== nothing
            dec = descriptor isa UInt64 ? get(DESCRIBED_DECODERS, descriptor, nothing) : nothing
            v = dec === nothing ? AMQPDescribed(descriptor, v) : dec(v)
        end
        items[i] = v
    end
    at_end(sub) || throw(AMQPDecodeError("trailing bytes in AMQP array"))
    r.pos = endpos + 1
    return items
end
