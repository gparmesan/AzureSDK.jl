# Event Hubs exceptions (azure.eventhub.exceptions).
#
#   AzureError
#   └── EventHubError (abstract)
#       ├── EventHubServiceError     generic service failure
#       ├── ClientClosedError
#       ├── ConnectionLostError
#       ├── ConnectError
#       ├── AuthenticationError
#       ├── EventDataError
#       ├── EventDataSendError
#       ├── OperationTimeoutError
#       └── OwnershipLostError

"""
    EventHubError

Abstract supertype of Event Hubs errors. Concrete errors carry `message`,
`error` (underlying exception), `condition` (AMQP error condition or
`nothing`) and `retriable`.
"""
abstract type EventHubError <: AzureError end

for T in (:EventHubServiceError, :ClientClosedError, :ConnectionLostError, :ConnectError, :AuthenticationError,
    :EventDataError, :EventDataSendError, :OperationTimeoutError, :OwnershipLostError)
    @eval begin
        struct $T <: EventHubError
            message::String
            error::Any
            condition::Union{Nothing,Symbol}
            retriable::Bool
        end
        $T(message::AbstractString=$(string(T)); error=nothing, condition=nothing, retriable::Bool=false) =
            $T(String(message), error, condition, retriable)
    end
end

"Map an internal exception to the public Event Hubs error hierarchy."
function map_eventhub_error(err)
    err isa EventHubError && return err
    err isa AzureCore.ClientAuthenticationError && return AuthenticationError(err.message; error=err)
    err isa AzureError && return err
    kind = Messaging.error_kind(err)
    cond = err isa AMQPException ? err.condition : nothing
    msg = err isa AMQPException ? string(err.description, " (", err.condition, ")") : sprint(showerror, err)
    retr = Messaging.is_retriable(err)
    kind === :tls && return ConnectError("TLS error: " * msg; error=err)
    kind === :closed && return ClientClosedError("The client has been closed."; error=err)
    kind === :auth && return AuthenticationError(msg; error=err, condition=cond)
    kind === :not_found && return ConnectError(msg; error=err, condition=cond)
    kind === :timeout && return OperationTimeoutError(msg; error=err, condition=cond, retriable=true)
    kind === :message_size && return EventDataSendError(msg; error=err, condition=cond)
    kind === :link_stolen && return OwnershipLostError(msg; error=err, condition=cond)
    kind === :connection && return ConnectionLostError(msg; error=err, condition=cond, retriable=true)
    return EventHubServiceError(msg; error=err, condition=cond, retriable=retr)
end
