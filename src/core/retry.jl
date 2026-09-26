# Retry configuration shared by the service clients.

"""
    RetryMode.Exponential / RetryMode.Fixed

Back-off strategy between retries (Python: `azure.eventhub.RetryMode`,
`azure.servicebus.RetryMode`).
"""
module RetryMode
@enum T Exponential Fixed
end

"""
    RetryOptions(; retry_total=3, retry_backoff_factor=0.8, retry_backoff_max=120, retry_mode=RetryMode.Exponential)

Retry policy for transient failures. With exponential back-off the delay
before retry *n* (1-based) is `retry_backoff_factor * 2^(n-1)` seconds (with
±20 % jitter), capped at `retry_backoff_max`.
"""
Base.@kwdef struct RetryOptions
    retry_total::Int = 3
    retry_backoff_factor::Float64 = 0.8
    retry_backoff_max::Float64 = 120.0
    retry_mode::RetryMode.T = RetryMode.Exponential
end

function retry_options(; retry_total::Integer=3, retry_backoff_factor::Real=0.8, retry_backoff_max::Real=120.0,
    retry_mode=RetryMode.Exponential, kwargs...)
    mode = retry_mode isa RetryMode.T ? retry_mode :
           lowercase(string(retry_mode)) == "fixed" ? RetryMode.Fixed : RetryMode.Exponential
    retry_total < 0 && throw(ArgumentError("retry_total must be >= 0"))
    return RetryOptions(Int(retry_total), Float64(retry_backoff_factor), Float64(retry_backoff_max), mode)
end

"Seconds to sleep before retry number `attempt` (1-based)."
function backoff_delay(opts::RetryOptions, attempt::Integer)
    base = opts.retry_mode == RetryMode.Fixed ? opts.retry_backoff_factor :
           opts.retry_backoff_factor * 2.0^(attempt - 1)
    jitter = 0.8 + 0.4 * rand()
    return min(base * jitter, opts.retry_backoff_max)
end
