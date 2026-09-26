# Shared identity infrastructure: errors, authority hosts, input validation,
# token caching and the Entra ID token endpoint client.

"""
    CredentialUnavailableError(message)

The credential cannot attempt authentication in this environment (e.g. the
required environment variables are missing, or the Azure CLI is not
installed). [`ChainedTokenCredential`](@ref) moves on to the next credential
when it sees this error.
"""
struct CredentialUnavailableError <: AzureError
    message::String
    error::Any
end
CredentialUnavailableError(message::AbstractString; error=nothing) = CredentialUnavailableError(String(message), error)

"""
    AzureAuthorityHosts

Well-known Entra ID authority hosts (`AzureAuthorityHosts.AZURE_PUBLIC_CLOUD`,
`AZURE_CHINA`, `AZURE_GOVERNMENT`).
"""
module AzureAuthorityHosts
const AZURE_PUBLIC_CLOUD = "login.microsoftonline.com"
const AZURE_CHINA = "login.chinacloudapi.cn"
const AZURE_GOVERNMENT = "login.microsoftonline.us"
end

const DEFAULT_REFRESH_OFFSET = 300      # refresh tokens 5 minutes before expiry
const DEFAULT_TOKEN_REFRESH_RETRY_DELAY = 30

"Normalize an authority (host or https URL) to `https://host`."
function normalize_authority(authority::Union{Nothing,AbstractString})
    a = authority === nothing || isempty(authority) ? get(ENV, "AZURE_AUTHORITY_HOST", AzureAuthorityHosts.AZURE_PUBLIC_CLOUD) :
        String(authority)
    a = rstrip(a, '/')
    if startswith(lowercase(a), "http://")
        # plain http is only accepted for loopback test endpoints
        h = URIs.URI(a).host
        h in ("localhost", "127.0.0.1", "::1") || throw(ArgumentError("authority must use https"))
        return a
    end
    startswith(lowercase(a), "https://") || (a = "https://" * a)
    u = URIs.URI(a)
    isempty(u.host) && throw(ArgumentError("invalid authority: $(authority)"))
    return a
end

const _VALID_TENANT = r"^[0-9A-Za-z\-.]+$"
const _VALID_SCOPE = r"^[0-9A-Za-z\-_.:/]+$"
const _VALID_ID = r"^[0-9A-Za-z\-_.]+$"

function validate_tenant_id(tenant::AbstractString)
    occursin(_VALID_TENANT, tenant) ||
        throw(ArgumentError("Invalid tenant ID provided. You can locate your tenant ID by following the instructions listed here: https://learn.microsoft.com/partner-center/find-ids-and-domain-names"))
    return String(tenant)
end

function validate_scope(scope::AbstractString)
    occursin(_VALID_SCOPE, scope) || throw(ArgumentError("Invalid scope: only alphanumeric characters, '.', '-', '_', ':' and '/' are allowed"))
    return String(scope)
end

function validate_client_id(id::AbstractString)
    occursin(_VALID_ID, id) || throw(ArgumentError("Invalid client/object ID"))
    return String(id)
end

"Resource URI for a single `.default` scope (managed identity and CLI take resources)."
function scope_to_resource(scope::AbstractString)
    validate_scope(scope)
    return endswith(scope, "/.default") ? scope[1:end-length("/.default")] : String(scope)
end

function _require_scopes(scopes)
    isempty(scopes) && throw(ArgumentError("get_token requires at least one scope"))
    return String[String(s) for s in scopes]
end

"""
Resolve the tenant to request a token from, honouring
`additionally_allowed_tenants` (and `AZURE_IDENTITY_DISABLE_MULTITENANTAUTH`).
"""
function resolve_tenant(default_tenant::AbstractString, requested::Union{Nothing,AbstractString},
    allowed::Vector{String})
    (requested === nothing || isempty(requested) || requested == default_tenant) && return String(default_tenant)
    if lowercase(get(ENV, "AZURE_IDENTITY_DISABLE_MULTITENANTAUTH", "")) in ("1", "true")
        return String(default_tenant)
    end
    validate_tenant_id(requested)
    default_tenant == "adfs" && throw(ClientAuthenticationError("A tenant_id cannot be specified for ADFS authentication."))
    ("*" in allowed || requested in allowed) && return String(requested)
    throw(ClientAuthenticationError(
        "The current credential is not configured to acquire tokens for tenant $(requested). To enable acquiring tokens for this tenant add it to the additionally_allowed_tenants when creating the credential, or add \"*\" to additionally_allowed_tenants to allow acquiring tokens for any tenant."))
end

const _CAE_CLAIMS = Dict("access_token" => Dict("xms_cc" => Dict("values" => ["CP1"])))

function _merge_claims(claims::Union{Nothing,AbstractString}, enable_cae::Bool)
    enable_cae || return claims
    (claims === nothing || isempty(claims)) && return JSON.json(_CAE_CLAIMS)
    parsed = try
        JSON.parse(claims; dicttype=Dict{String,Any})
    catch
        throw(ArgumentError("claims must be a JSON object"))
    end
    at = get!(parsed, "access_token", Dict{String,Any}())
    at["xms_cc"] = Dict("values" => ["CP1"])
    return JSON.json(parsed)
end

# ---------------------------------------------------------------------------
# Token cache
# ---------------------------------------------------------------------------

"""
Per-credential in-memory token cache. Tokens are served from the cache until
their refresh time (explicit `refresh_on`, or 5 minutes before expiry); a
failed refresh falls back to the cached token while it is still valid.
"""
mutable struct TokenCache
    lock::ReentrantLock
    fetch_lock::ReentrantLock
    entries::Dict{Any,AccessTokenInfo}
    last_attempt::Dict{Any,Float64}
end
TokenCache() = TokenCache(ReentrantLock(), ReentrantLock(), Dict{Any,AccessTokenInfo}(), Dict{Any,Float64}())

_needs_refresh(t::AccessTokenInfo, now::Real) =
    t.refresh_on !== nothing ? now >= t.refresh_on : now >= t.expires_on - DEFAULT_REFRESH_OFFSET

function cached_or_fetch(fetch::Function, cache::TokenCache, key; bypass::Bool=false)
    now = time()
    if !bypass
        tok = @lock cache.lock get(cache.entries, key, nothing)
        tok !== nothing && !_needs_refresh(tok, now) && return tok
    end
    lock(cache.fetch_lock)
    try
        now = time()
        tok = @lock cache.lock get(cache.entries, key, nothing)
        if !bypass && tok !== nothing
            !_needs_refresh(tok, now) && return tok
            # avoid hammering the endpoint while a still-valid token is cached
            last = @lock cache.lock get(cache.last_attempt, key, 0.0)
            tok.expires_on > now && now - last < DEFAULT_TOKEN_REFRESH_RETRY_DELAY && return tok
        end
        @lock cache.lock cache.last_attempt[key] = now
        new = try
            fetch()
        catch err
            if !bypass && tok !== nothing && tok.expires_on > time()
                @warn "token refresh failed; using cached token until it expires" exception = (err, catch_backtrace())
                return tok
            end
            rethrow()
        end
        @lock cache.lock cache.entries[key] = new
        return new
    finally
        unlock(cache.fetch_lock)
    end
end

Base.empty!(cache::TokenCache) = (@lock cache.lock (empty!(cache.entries); empty!(cache.last_attempt)); cache)

# ---------------------------------------------------------------------------
# Token responses
# ---------------------------------------------------------------------------

_json_get(obj, key) = obj isa AbstractDict ? get(obj, key, nothing) : nothing

function _as_int(v)
    v === nothing && return nothing
    v isa Integer && return Int(v)
    v isa AbstractFloat && return floor(Int, v)
    v isa AbstractString && return tryparse(Int, strip(v))
    return nothing
end

"""
Build an `AccessTokenInfo` from an OAuth2 token response body. Handles
`expires_in`/`expires_on`/`refresh_in` as numbers or numeric strings.
"""
function parse_token_response(body::AbstractString; now::Real=time(), source::AbstractString="token endpoint")
    obj = try
        JSON.parse(body; dicttype=Dict{String,Any})
    catch
        throw(ClientAuthenticationError("Unexpected response from $source (not JSON)"))
    end
    token = _json_get(obj, "access_token")
    (token isa AbstractString && !isempty(token)) ||
        throw(ClientAuthenticationError("Unexpected response from $source: no access token"))
    expires_on = _as_int(_json_get(obj, "expires_on"))
    if expires_on === nothing
        s = _json_get(obj, "expires_on")
        if s isa AbstractString
            expires_on = _parse_datetime_string(s)
        end
    end
    expires_in = _as_int(_json_get(obj, "expires_in"))
    if expires_on === nothing
        expires_in === nothing && throw(ClientAuthenticationError("Unexpected response from $source: no expiry"))
        expires_on = floor(Int, now) + expires_in
    end
    refresh_in = _as_int(_json_get(obj, "refresh_in"))
    refresh_on = if refresh_in !== nothing
        floor(Int, now) + refresh_in
    else
        lifetime = something(expires_in, expires_on - floor(Int, now))
        lifetime >= 7200 ? floor(Int, now) + lifetime ÷ 2 : nothing
    end
    token_type = something(_json_get(obj, "token_type"), "Bearer")
    return AccessTokenInfo(String(token), expires_on; token_type=string(token_type), refresh_on=refresh_on)
end

# App Service / Cloud Shell sometimes return "11/8/2023 3:04:05 PM +00:00"
function _parse_datetime_string(s::AbstractString)
    for fmt in (Dates.dateformat"m/d/y I:M:S p", Dates.dateformat"y-m-dTH:M:S", Dates.dateformat"y-m-d H:M:S")
        str = replace(strip(s), r"\s*[+-]\d\d:\d\d$" => "", r"Z$" => "", r"\.\d+$" => "")
        dt = tryparse(Dates.DateTime, str, fmt)
        dt === nothing || return floor(Int, Dates.datetime2unix(dt))
    end
    return nothing
end

function _oauth_error_message(resp)
    body = String(copy(resp.body))
    obj = try
        JSON.parse(body; dicttype=Dict{String,Any})
    catch
        nothing
    end
    desc = _json_get(obj, "error_description")
    err = _json_get(obj, "error")
    if desc isa AbstractString
        return string(something(err, "error"), ": ", first(split(desc, "\r\n")))
    end
    return "HTTP $(resp.status)"
end

"""
Entra ID token endpoint client shared by the confidential-client credentials.
"""
struct EntraClient
    tenant_id::String
    client_id::String
    authority::String
    additionally_allowed_tenants::Vector{String}
    retry::RetryOptions
    timeout::Float64
end

function EntraClient(tenant_id::AbstractString, client_id::AbstractString; authority=nothing,
    additionally_allowed_tenants=String[], retry_total::Integer=3, timeout::Real=30.0)
    isempty(client_id) && throw(ArgumentError("client_id should be the id of a Microsoft Entra application"))
    EntraClient(validate_tenant_id(tenant_id), String(client_id), normalize_authority(authority),
        String[String(t) for t in additionally_allowed_tenants],
        RetryOptions(retry_total=retry_total), Float64(timeout))
end

token_endpoint(c::EntraClient, tenant::AbstractString) = string(c.authority, "/", tenant, "/oauth2/v2.0/token")

function _form_encode(pairs)
    join((string(URIs.escapeuri(k), "=", URIs.escapeuri(v)) for (k, v) in pairs), "&")
end

"""
    request_token(client, scopes, tenant, grant_params; claims=nothing) -> AccessTokenInfo

POST to the tenant's v2.0 token endpoint. `grant_params` supplies the grant
specific form fields (client secret, assertion, ...).
"""
function request_token(c::EntraClient, scopes::Vector{String}, tenant::AbstractString,
    grant_params::Vector{Pair{String,String}}; claims::Union{Nothing,AbstractString}=nothing)
    form = Pair{String,String}["client_id" => c.client_id, "scope" => join(scopes, " ")]
    append!(form, grant_params)
    claims === nothing || isempty(claims) || push!(form, "claims" => String(claims))
    url = token_endpoint(c, tenant)
    resp = AzureCore.send_request("POST", url;
        headers=["Content-Type" => "application/x-www-form-urlencoded", "Accept" => "application/json",
            "User-Agent" => AzureCore.user_agent("identity")],
        body=_form_encode(form), retry=c.retry, timeout=c.timeout,
        retry_status_codes=(429, 500, 502, 503, 504))
    if resp.status != 200
        throw(ClientAuthenticationError("Authentication failed: $(_oauth_error_message(resp))";
            status_code=resp.status))
    end
    return parse_token_response(String(copy(resp.body)); source="Microsoft Entra ID")
end
