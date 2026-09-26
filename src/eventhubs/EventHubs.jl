"""
    AzureSDK.EventHubs

Azure Event Hubs client modelled on Python's `azure.eventhub`:
[`EventHubProducerClient`](@ref), [`EventHubConsumerClient`](@ref),
[`EventData`](@ref), [`EventDataBatch`](@ref), checkpoint stores and load
balancing. Transports: AMQP over TLS, AMQP over WebSockets, and HTTP (REST,
sending only).
"""
module EventHubs

using Base64: base64encode, base64decode
using Dates
using Random: shuffle!
using UUIDs: uuid4
import HTTP
import JSON
import SHA
import URIs

using ..AzureCore
using ..AzureCore: parse_connection_string_pairs
using ..AMQP
using ..AMQP: AMQPException, ClientConnection, AMQPMessage, AMQPDescribed
using ..Messaging
using ..Messaging: ClientConfig, TransportType
import ..Messaging: from_connection_string, body_as_str, size_in_bytes, max_size_in_bytes

export EventHubProducerClient, EventHubConsumerClient, EventData, EventDataBatch, TransportType, RetryMode,
    from_connection_string, EventHubConnectionStringProperties,
    create_batch, add!, try_add!, send_batch, send_event, receive, receive_batch,
    get_eventhub_properties, get_partition_ids, get_partition_properties,
    body_as_str, body_as_json, size_in_bytes, max_size_in_bytes,
    CheckpointStore, InMemoryCheckpointStore, BlobCheckpointStore, PartitionContext, CloseReason, LoadBalancingStrategy,
    list_ownership, claim_ownership, update_checkpoint, list_checkpoints,
    EventHubError, EventHubServiceError, ClientClosedError, ConnectionLostError, ConnectError, AuthenticationError,
    EventDataError, EventDataSendError, OwnershipLostError,
    AzureNamedKeyCredential, AzureSasCredential

include("exceptions.jl")
include("event_data.jl")
include("common.jl")
include("checkpoint.jl")
include("producer.jl")
include("consumer.jl")
include("blob_checkpoint.jl")

end # module
