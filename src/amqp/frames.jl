# AMQP frame layer (Part 2: Transport, section 2.3).

const FRAME_TYPE_AMQP = 0x00
const FRAME_TYPE_SASL = 0x01
const FRAME_HEADER_SIZE = 8

const AMQP_HEADER = UInt8['A', 'M', 'Q', 'P', 0x00, 0x01, 0x00, 0x00]
const SASL_HEADER = UInt8['A', 'M', 'Q', 'P', 0x03, 0x01, 0x00, 0x00]

"Minimum max-frame-size every AMQP peer must accept."
const MIN_MAX_FRAME_SIZE = 512

"""
    encode_frame(performative; channel=0, frame_type=FRAME_TYPE_AMQP, payload=nothing)

Encode a complete frame. `payload` (for transfer frames) is appended after the
performative.
"""
function encode_frame(performative; channel::UInt16=UInt16(0), frame_type::UInt8=FRAME_TYPE_AMQP,
    payload::Union{Nothing,AbstractVector{UInt8}}=nothing)
    w = ByteWriter(64 + (payload === nothing ? 0 : length(payload)))
    w.len = FRAME_HEADER_SIZE
    encode!(w, performative)
    payload !== nothing && writebytes!(w, payload)
    write_frame_header!(w, frame_type, channel)
    return takebytes!(w)
end

function write_frame_header!(w::ByteWriter, frame_type::UInt8, channel::UInt16)
    patchbe32!(w, 1, UInt32(w.len))
    @inbounds w.data[5] = 0x02          # doff: 2 * 4 = 8 byte header
    @inbounds w.data[6] = frame_type
    @inbounds w.data[7] = (channel >> 8) % UInt8
    @inbounds w.data[8] = channel % UInt8
    return w
end

"An empty frame, used as a heartbeat."
const HEARTBEAT_FRAME = UInt8[0x00, 0x00, 0x00, 0x08, 0x02, 0x00, 0x00, 0x00]

"""
A decoded frame. `body` is `nothing` for heartbeat frames; `payload` holds the
bytes following the performative (transfer frames only).
"""
struct Frame
    frame_type::UInt8
    channel::UInt16
    body::Any
    payload::Union{Nothing,SubArray{UInt8,1,Vector{UInt8},Tuple{UnitRange{Int}},true}}
end

"""
    decode_frame(header::AbstractVector{UInt8}, body::Vector{UInt8}) -> Frame

`header` holds the 8 fixed header bytes and `body` the remaining `size - 8`
bytes (extended header + frame body).
"""
function decode_frame(header::AbstractVector{UInt8}, body::Vector{UInt8})
    doff = Int(header[5])
    doff < 2 && throw(AMQPDecodeError("invalid frame data offset $doff"))
    frame_type = header[6]
    channel = (UInt16(header[7]) << 8) | UInt16(header[8])
    start = doff * 4 - FRAME_HEADER_SIZE + 1   # skip extended header
    start > length(body) + 1 && throw(AMQPDecodeError("frame data offset beyond frame size"))
    if start > length(body)
        return Frame(frame_type, channel, nothing, nothing)
    end
    r = ByteReader(body, start)
    performative = decode(r)
    payload = at_end(r) ? nothing : view(body, r.pos:length(body))
    return Frame(frame_type, channel, performative, payload)
end

frame_size(header::AbstractVector{UInt8}) =
    (UInt32(header[1]) << 24) | (UInt32(header[2]) << 16) | (UInt32(header[3]) << 8) | UInt32(header[4])
