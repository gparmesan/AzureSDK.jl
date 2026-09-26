"""
    AzureSDK.ServiceBus

Azure Service Bus client modelled on Python's `azure.servicebus`:
[`ServiceBusClient`](@ref), [`ServiceBusSender`](@ref),
[`ServiceBusReceiver`](@ref), sessions, scheduling, deferral,
dead-lettering, peeking and automatic lock renewal. Transports: AMQP over
TLS, AMQP over WebSockets, and HTTP (REST; a subset of operations).
"""
module ServiceBus

using Dates
using UUIDs: UUID, uuid4
import HTTP
import JSON
import URIs

using ..AzureCore
using ..AMQP
using ..AMQP: AMQPException, ClientConnection, AMQPMessage, AMQPArray
using ..Messaging
using ..Messaging: ClientConfig, TransportType
import ..Messaging: from_connection_string, body_as_str, size_in_bytes, max_size_in_bytes

export ServiceBusClient, ServiceBusSender, ServiceBusReceiver, ServiceBusSession, ServiceBusMessage,
    ServiceBusReceivedMessage, ServiceBusMessageBatch, ServiceBusReceiveMode, ServiceBusSubQueue,
    ServiceBusMessageState, NEXT_AVAILABLE_SESSION, TransportType, RetryMode, AutoLockRenewer,
    from_connection_string, ServiceBusConnectionStringProperties,
    get_queue_sender, get_topic_sender, get_queue_receiver, get_subscription_receiver,
    send_messages, create_message_batch, add_message!, try_add_message!, schedule_messages,
    cancel_scheduled_messages, receive_messages, peek_messages, receive_deferred_messages,
    complete_message, abandon_message, defer_message, dead_letter_message, renew_message_lock,
    get_state, set_state, renew_lock, register!, body_as_str, size_in_bytes, max_size_in_bytes,
    ServiceBusError, ServiceBusServiceError, ServiceBusConnectionError, ServiceBusCommunicationError,
    ServiceBusAuthenticationError, ServiceBusAuthorizationError, MessageSizeExceededError,
    MessageAlreadySettled, MessageLockLostError, MessageNotFoundError, MessagingEntityNotFoundError,
    MessagingEntityDisabledError, MessagingEntityAlreadyExistsError, ServiceBusQuotaExceededError,
    ServiceBusServerBusyError, SessionLockLostError, SessionCannotBeLockedError, AutoLockRenewFailed,
    AutoLockRenewTimeout, ServiceBusClientClosedError, AzureNamedKeyCredential, AzureSasCredential

include("exceptions.jl")
include("message.jl")
include("client.jl")
include("sender.jl")
include("receiver.jl")
include("autolockrenew.jl")
include("http.jl")

end # module
