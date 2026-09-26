# Minimal HTTP pipeline: retries with back-off, Retry-After support, no
# redirects or cookies (so credentials are never forwarded to another host),
# and error messages that never include query strings (which may carry SAS
# signatures) or request headers.

const PACKAGE_VERSION = "0.1.0"
const RETRY_STATUS_CODES = (408, 429, 500, 502, 503, 504)

user_agent(component::AbstractString) = "azsdk-julia-$(component)/$(PACKAGE_VERSION) Julia/$(VERSION)"

"URL without query string or fragment, safe for error messages and logs."
function redact_url(url::AbstractString)
    u = try
        URIs.URI(url)
    catch
        return "<url>"
    end
    port = isempty(u.port) ? "" : ":" * u.port
    return string(u.scheme, "://", u.host, port, u.path)
end

function _retry_after_seconds(resp)
    for (name, scale) in (("retry-after-ms", 1e-3), ("x-ms-retry-after-ms", 1e-3), ("retry-after", 1.0))
        v = HTTP.header(resp, name, "")
        isempty(v) && continue
        n = tryparse(Float64, strip(v))
        n !== nothing && return n * scale
        if scale == 1.0
            # HTTP-date
            try
                dt = Dates.DateTime(strip(v)[1:end-4], Dates.dateformat"e, d u y H:M:S")
                return max(0.0, Dates.value(dt - Dates.now(Dates.UTC)) / 1000)
            catch
            end
        end
    end
    return nothing
end

"""
    send_request(method, url; headers=[], body=nothing, retry=RetryOptions(), timeout=30,
                 connect_timeout=min(timeout, 30), retry_status_codes=RETRY_STATUS_CODES,
                 retry_on_connection_error=true, proxy=nothing, client=nothing) -> HTTP.Response

Send an HTTP request and return the final response (any status). Transient
failures are retried according to `retry`. Connection failures after the last
attempt raise [`ServiceRequestError`](@ref).
"""
function send_request(method::AbstractString, url::AbstractString;
    headers=Pair{String,String}[], body=nothing, retry::RetryOptions=RetryOptions(),
    timeout::Real=30.0, connect_timeout::Real=min(timeout, 30.0),
    retry_status_codes=RETRY_STATUS_CODES, retry_on_connection_error::Bool=true,
    proxy=nothing, client=nothing, max_retry_after::Real=60.0)
    attempt = 0
    kwargs = Dict{Symbol,Any}(:retry => false, :status_exception => false, :redirect => false,
        :cookies => false, :connect_timeout => connect_timeout, :request_timeout => timeout)
    if client !== nothing
        kwargs[:client] = client        # carries TLS (custom CA) and proxy settings
    elseif proxy !== nothing
        kwargs[:proxy] = proxy
    end
    while true
        resp = try
            HTTP.request(method, url, headers, body === nothing ? UInt8[] : body; kwargs...)
        catch err
            err isa InterruptException && rethrow()
            attempt += 1
            if !retry_on_connection_error || attempt > retry.retry_total
                throw(ServiceRequestError("$(method) $(redact_url(url)) failed: $(sprint(showerror, err; context=:compact=>true) |> _first_line)";
                    error=err))
            end
            sleep(backoff_delay(retry, attempt))
            continue
        end
        if resp.status in retry_status_codes && attempt < retry.retry_total
            attempt += 1
            ra = _retry_after_seconds(resp)
            sleep(ra === nothing ? backoff_delay(retry, attempt) : min(ra, max_retry_after))
            continue
        end
        return resp
    end
end

_first_line(s::AbstractString) = first(split(s, '\n'))

"Raise the appropriate `AbstractHttpResponseError` for an unsuccessful response."
function raise_for_status(resp; message::AbstractString="")
    200 <= resp.status < 300 && return resp
    body = String(copy(resp.body))
    length(body) > 2048 && (body = body[1:prevind(body, 2048)] * "…")
    msg = isempty(message) ? "Operation returned an invalid status $(resp.status)" : String(message)
    T = resp.status in (401, 403) ? ClientAuthenticationError :
        resp.status == 404 ? ResourceNotFoundError :
        resp.status == 409 ? ResourceExistsError :
        resp.status == 304 ? ResourceNotModifiedError : HttpResponseError
    throw(T(msg; status_code=resp.status, reason=HTTP.StatusCodes.statustext(resp.status), response_body=body))
end
