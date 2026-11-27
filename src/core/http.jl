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

const _STATUS_TEXT = Dict(200 => "OK", 201 => "Created", 202 => "Accepted", 204 => "No Content",
    206 => "Partial Content", 304 => "Not Modified", 400 => "Bad Request", 401 => "Unauthorized", 403 => "Forbidden",
    404 => "Not Found", 405 => "Method Not Allowed", 408 => "Request Timeout", 409 => "Conflict", 410 => "Gone",
    411 => "Length Required", 412 => "Precondition Failed", 413 => "Payload Too Large", 416 => "Range Not Satisfiable",
    429 => "Too Many Requests", 500 => "Internal Server Error", 501 => "Not Implemented", 502 => "Bad Gateway",
    503 => "Service Unavailable", 504 => "Gateway Timeout")

"Reason phrase of a response (from the response, or the standard text for its status)."
function status_text(resp)
    r = hasproperty(resp, :reason) ? string(getproperty(resp, :reason)) : ""
    return isempty(r) ? get(_STATUS_TEXT, resp.status, "") : r
end

"Raise the appropriate `AbstractHttpResponseError` for an unsuccessful response."
function raise_for_status(resp; message::AbstractString="")
    200 <= resp.status < 300 && return resp
    body = String(copy(resp.body))
    length(body) > 2048 && (body = body[1:prevind(body, 2048)] * "…")
    msg = isempty(message) ? "Operation returned an invalid status $(resp.status)" : String(message)
    T = resp.status in (401, 403) ? ClientAuthenticationError :
        resp.status == 404 ? ResourceNotFoundError :
        resp.status == 409 ? ResourceExistsError :
        resp.status == 412 ? ResourceModifiedError :
        resp.status == 304 ? ResourceNotModifiedError : HttpResponseError
    throw(T(msg; status_code=resp.status, reason=status_text(resp), response_body=body))
end

"""
    proxy_config(http_proxy) -> proxy setting for HTTP.jl (or `nothing`)

Normalise a proxy setting: a URL string, or a dict / named tuple with
`proxy_hostname`, `proxy_port` (3128), `username` and `password` (the
Python SDKs' `http_proxy` format).
"""
function proxy_config(p)
    p === nothing && return nothing
    p isa AbstractString && return HTTP.ProxyURL(String(p))
    get_(k) = p isa AbstractDict ? get(p, k, get(p, String(k), nothing)) : get(p, k, nothing)
    host = get_(:proxy_hostname)
    host === nothing && throw(ArgumentError("http_proxy requires proxy_hostname"))
    port = something(get_(:proxy_port), 3128)
    user = get_(:username)
    pass = get_(:password)
    auth = user === nothing ? "" : string(URIs.escapeuri(String(user)), ":", URIs.escapeuri(String(something(pass, ""))), "@")
    return HTTP.ProxyURL("http://$(auth)$(host):$(port)")
end

"""
    http_client(connection_verify, http_proxy) -> Union{Nothing,HTTP.Client}

An `HTTP.Client` honouring `connection_verify` (`true`, `false` or the path
of a CA bundle) and `http_proxy`, or `nothing` when the defaults apply.
"""
function http_client(connection_verify, http_proxy)
    (connection_verify === true && http_proxy === nothing) && return nothing
    tls = if connection_verify isa AbstractString
        Reseau.TLS.Config(ca_file=String(connection_verify))
    elseif connection_verify === false
        Reseau.TLS.Config(verify_peer=false)
    else
        nothing
    end
    return HTTP.Client(transport=HTTP.Transport(tls_config=tls, proxy=proxy_config(http_proxy)))
end

"""
    is_local_host(host) -> Bool

True only for loopback hosts: `localhost`, `*.localhost`, an IPv4 address in
127.0.0.0/8 or `::1`. These are the only hosts that may receive credentials
over plain (unencrypted) connections.
"""
function is_local_host(host::AbstractString)
    h = lowercase(strip(host, ['[', ']']))
    (h == "localhost" || endswith(h, ".localhost") || h in ("::1", "0:0:0:0:0:0:0:1")) && return true
    m = match(r"^127\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})$", h)
    return m !== nothing && all(x -> parse(Int, x) <= 255, m.captures)
end

"""
    is_dev_host(host) -> Bool

True for hosts where a local emulator may run: loopback hosts (see
[`is_local_host`](@ref)) and single-label names such as docker-compose service
names (`azurite`). Such names may resolve through the DNS search domain to
any machine, so over plain connections they only get the emulators' public
keys (or no credential); real keys, SAS tokens and Entra ID tokens go
unencrypted only to hosts accepted by `is_local_host`.
"""
is_dev_host(host::AbstractString) = is_local_host(host) || occursin(r"^[a-z0-9]([a-z0-9-]*[a-z0-9])?$", lowercase(host))
