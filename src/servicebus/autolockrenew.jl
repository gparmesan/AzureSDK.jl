# AutoLockRenewer (azure.servicebus.AutoLockRenewer).

"""
    AutoLockRenewer(; max_lock_renewal_duration=300, on_lock_renew_failure=nothing)

Keeps message or session locks alive in the background. Register targets
with [`register!`](@ref), or pass the renewer as `auto_lock_renewer` when
creating a receiver to register every received message (or the session)
automatically.

`on_lock_renew_failure(target, error)` is called when renewal fails.
"""
mutable struct AutoLockRenewer
    max_lock_renewal_duration::Float64
    on_lock_renew_failure::Union{Nothing,Function}
    lock::ReentrantLock
    tasks::Vector{Task}
    closed::Bool
end
AutoLockRenewer(; max_lock_renewal_duration::Real=300, on_lock_renew_failure=nothing) =
    AutoLockRenewer(Float64(max_lock_renewal_duration), on_lock_renew_failure, ReentrantLock(), Task[], false)

Base.show(io::IO, a::AutoLockRenewer) = print(io, "AutoLockRenewer(max_lock_renewal_duration=",
    a.max_lock_renewal_duration, ", active=", count(!istaskdone, a.tasks), a.closed ? ", closed" : "", ")")

_locked_until(m::ServiceBusReceivedMessage) = getfield(m, :locked_until_utc)
_locked_until(s::ServiceBusSession) = s.locked_until_utc
_done(m::ServiceBusReceivedMessage) = getfield(m, :settled)
_done(::ServiceBusSession) = false
_renew!(r, m::ServiceBusReceivedMessage) = renew_message_lock(r, m)
_renew!(r, s::ServiceBusSession) = renew_lock(s)

"""
    register!(renewer, receiver, target; max_lock_renewal_duration=nothing, on_lock_renew_failure=nothing)

Keep renewing the lock of `target` (a received message or a
`ServiceBusSession`) until it is settled, the receiver closes, or the
duration elapses.
"""
function register!(a::AutoLockRenewer, r::ServiceBusReceiver, target::Union{ServiceBusReceivedMessage,ServiceBusSession};
    max_lock_renewal_duration::Union{Nothing,Real}=nothing, on_lock_renew_failure=nothing)
    a.closed && throw(ServiceBusServiceError("The AutoLockRenewer has been closed."))
    duration = Float64(something(max_lock_renewal_duration, a.max_lock_renewal_duration))
    callback = something(on_lock_renew_failure, a.on_lock_renew_failure, Some(nothing))
    t = Threads.@spawn _renew_loop(a, r, target, time() + duration, callback)
    @lock a.lock begin
        filter!(!istaskdone, a.tasks)
        push!(a.tasks, t)
    end
    return nothing
end

function _renew_loop(a::AutoLockRenewer, r::ServiceBusReceiver, target, stop_at::Float64, callback)
    buffer = 10.0
    while !a.closed && !r.closed && !_done(target) && time() < stop_at
        until = _locked_until(target)
        due = until === nothing ? time() :
              Dates.datetime2unix(until) - buffer
        if time() >= due
            try
                _renew!(r, target)
            catch err
                (a.closed || r.closed || _done(target)) && return
                if callback !== nothing
                    try
                        callback(target, AutoLockRenewFailed("Failed to auto-renew the lock"; error=err))
                    catch cb_err
                        @error "on_lock_renew_failure raised an exception" exception = cb_err
                    end
                else
                    @warn "automatic lock renewal failed" exception = err
                end
                return
            end
        else
            sleep(min(0.5, max(0.05, due - time())))
        end
    end
end

"""
    close(renewer)

Stop all renewals.
"""
function Base.close(a::AutoLockRenewer)
    a.closed = true
    return nothing
end
