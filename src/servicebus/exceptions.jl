# Service Bus exceptions (azure.servicebus.exceptions).

"""
    ServiceBusError

Abstract supertype of Service Bus errors. Concrete errors carry `message`,
`error` (underlying exception), `condition` (AMQP condition or `nothing`) and
`retriable`.
"""
abstract type ServiceBusError <: AzureError end

const _SB_ERRORS = (
    :ServiceBusServiceError, :ServiceBusConnectionError, :ServiceBusCommunicationError,
    :ServiceBusAuthenticationError, :ServiceBusAuthorizationError, :OperationTimeoutError,
    :MessageSizeExceededError, :MessageAlreadySettled, :MessageLockLostError, :MessageNotFoundError,
    :MessagingEntityNotFoundError, :MessagingEntityDisabledError, :MessagingEntityAlreadyExistsError,
    :ServiceBusQuotaExceededError, :ServiceBusServerBusyError, :SessionLockLostError,
    :SessionCannotBeLockedError, :AutoLockRenewFailed, :AutoLockRenewTimeout, :ServiceBusClientClosedError,
)

for T in _SB_ERRORS
    @eval begin
        struct $T <: ServiceBusError
            message::String
            error::Any
            condition::Union{Nothing, Symbol}
            retriable::Bool
        end
        $T(message::AbstractString = $(string(T)); error = nothing, condition = nothing, retriable::Bool = false) =
            $T(String(message), error, condition, retriable)
    end
end

"Map an internal exception to the public Service Bus error hierarchy."
function map_servicebus_error(err)
    err isa ServiceBusError && return err
    err isa AzureCore.ClientAuthenticationError && return ServiceBusAuthenticationError(err.message; error = err)
    err isa AzureError && return err
    kind = Messaging.error_kind(err)
    cond = err isa AMQPException ? err.condition : nothing
    msg = err isa AMQPException ? string(err.description, " (", err.condition, ")") : sprint(showerror, err)
    retr = Messaging.is_retriable(err)
    kind === :tls && return ServiceBusConnectionError("TLS error: " * msg; error = err)
    kind === :closed && return ServiceBusClientClosedError("The handler has been closed."; error = err)
    kind === :auth && return ServiceBusAuthenticationError(msg; error = err, condition = cond)
    kind === :not_found && return MessagingEntityNotFoundError(msg; error = err, condition = cond)
    kind === :entity_disabled && return MessagingEntityDisabledError(msg; error = err, condition = cond)
    kind === :timeout && return OperationTimeoutError(msg; error = err, condition = cond, retriable = true)
    kind === :message_size && return MessageSizeExceededError(msg; error = err, condition = cond)
    kind === :server_busy && return ServiceBusServerBusyError(msg; error = err, condition = cond, retriable = true)
    kind === :quota && return ServiceBusQuotaExceededError(msg; error = err, condition = cond)
    kind === :lock_lost && return MessageLockLostError(msg; error = err, condition = cond)
    kind === :session_lock_lost && return SessionLockLostError(msg; error = err, condition = cond)
    kind === :session_cannot_be_locked && return SessionCannotBeLockedError(msg; error = err, condition = cond, retriable = true)
    kind === :message_not_found && return MessageNotFoundError(msg; error = err, condition = cond)
    kind === :connection && return ServiceBusConnectionError(msg; error = err, condition = cond, retriable = true)
    kind === :link && return ServiceBusCommunicationError(msg; error = err, condition = cond, retriable = true)
    return ServiceBusServiceError(msg; error = err, condition = cond, retriable = retr)
end
