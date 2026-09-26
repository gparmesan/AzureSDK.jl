"""
    AzureSDK.AMQP

A pure-Julia AMQP 1.0 client: type codec, framing, SASL, connection / session /
link state machines with flow control, request/response management links,
claims-based security, and TCP / TLS / WebSocket transports.

This module is the protocol engine behind `AzureSDK.EventHubs` and
`AzureSDK.ServiceBus`; it can also be used directly against any AMQP 1.0
broker.
"""
module AMQP

using Dates
using UUIDs: UUID, uuid4
import HTTP
import Reseau
import URIs

include("codec.jl")
include("composites.jl")
include("message.jl")
include("frames.jl")
include("transport.jl")
include("connection.jl")
include("management.jl")
include("client.jl")

export AMQPMessage, AMQPArray, AMQPDescribed, AMQPException, AMQPDecodeError

end # module
