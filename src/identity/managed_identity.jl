# Managed identity: IMDS (VMs, VM scale sets, AKS node identity, Container
# Instances), App Service / Functions / Container Apps, Cloud Shell, Azure Arc
# and the AKS token exchange (workload identity) fallback.

const IMDS_DEFAULT_HOST = "http://169.254.169.254"
const IMDS_PATH = "/metadata/identity/oauth2/token"

"""
    ManagedIdentityCredential(; client_id=nothing, object_id=nothing, resource_id=nothing)

Authenticate with an Azure managed identity. Pass one of `client_id`,
`object_id` or `resource_id` for a user-assigned identity; omit all three for
the system-assigned identity. The hosting environment is detected from
environment variables, like the Python SDK.
"""
struct ManagedIdentityCredential <: TokenCredential
    kind::Symbol
    id_kind::Union{Nothing,Symbol}     # :client_id, :object_id, :resource_id
    id_value::Union{Nothing,String}
    endpoint::String
    secret::String                     # App Service identity header
    cache::TokenCache
    within_dac::Bool
    probed::Base.RefValue{Bool}
    retry::RetryOptions
    token_exchange::Union{Nothing,WorkloadIdentityCredential}
end

function ManagedIdentityCredential(; client_id=nothing, object_id=nothing, resource_id=nothing,
    identity_config=nothing, _within_dac::Bool=false, retry_total::Integer=5)
    if identity_config !== nothing
        for (k, v) in identity_config
            k = string(k)
            k == "client_id" ? (client_id = v) : k == "object_id" ? (object_id = v) :
            k in ("resource_id", "mi_res_id", "msi_res_id") ? (resource_id = v) : nothing
        end
    end
    given = [(k, v) for (k, v) in ((:client_id, client_id), (:object_id, object_id), (:resource_id, resource_id))
             if v !== nothing && !isempty(v)]
    length(given) > 1 && throw(ArgumentError("client_id, object_id and resource_id are mutually exclusive"))
    id_kind, id_value = isempty(given) ? (nothing, nothing) : (given[1][1], String(given[1][2]))
    id_kind in (:client_id, :object_id) && validate_client_id(id_value)
    env(k) = get(ENV, k, "")
    kind, endpoint, secret = if !isempty(env("IDENTITY_ENDPOINT")) && !isempty(env("IDENTITY_HEADER"))
        if !isempty(env("IDENTITY_SERVER_THUMBPRINT"))
            (:service_fabric, env("IDENTITY_ENDPOINT"), env("IDENTITY_HEADER"))
        else
            (:app_service, env("IDENTITY_ENDPOINT"), env("IDENTITY_HEADER"))
        end
    elseif !isempty(env("MSI_ENDPOINT")) && isempty(env("MSI_SECRET"))
        (:cloud_shell, env("MSI_ENDPOINT"), "")
    elseif !isempty(env("IDENTITY_ENDPOINT")) && !isempty(env("IMDS_ENDPOINT"))
        (:azure_arc, env("IDENTITY_ENDPOINT"), "")
    elseif !isempty(env("AZURE_FEDERATED_TOKEN_FILE")) && !isempty(env("AZURE_TENANT_ID")) &&
           (id_kind === :client_id || !isempty(env("AZURE_CLIENT_ID")))
        (:token_exchange, "", "")
    else
        host = rstrip(isempty(env("AZURE_POD_IDENTITY_AUTHORITY_HOST")) ? IMDS_DEFAULT_HOST :
                      env("AZURE_POD_IDENTITY_AUTHORITY_HOST"), '/')
        (:imds, host * IMDS_PATH, "")
    end
    tx = kind === :token_exchange ?
         WorkloadIdentityCredential(client_id=id_kind === :client_id ? id_value : env("AZURE_CLIENT_ID")) : nothing
    return ManagedIdentityCredential(kind, id_kind, id_value, endpoint, secret, TokenCache(), _within_dac,
        Ref(false), RetryOptions(retry_total=retry_total, retry_backoff_factor=0.8, retry_backoff_max=60.0), tx)
end

function Base.show(io::IO, c::ManagedIdentityCredential)
    print(io, "ManagedIdentityCredential(", c.kind)
    c.id_kind === nothing || print(io, ", ", c.id_kind, "=", repr(c.id_value))
    print(io, ")")
end

function AzureCore.get_token_info(c::ManagedIdentityCredential, scopes::AbstractString...; claims=nothing,
    tenant_id=nothing, enable_cae::Bool=false)
    scopes_v = _require_scopes(scopes)
    if c.kind === :token_exchange
        return get_token_info(c.token_exchange::WorkloadIdentityCredential, scopes...; claims, tenant_id, enable_cae)
    end
    length(scopes_v) == 1 || throw(ArgumentError("ManagedIdentityCredential requires exactly one scope per token request"))
    resource = scope_to_resource(scopes_v[1])
    return cached_or_fetch(c.cache, (resource,); bypass=claims !== nothing && !isempty(claims)) do
        _request_mi_token(c, resource)
    end
end

function _id_query(c::ManagedIdentityCredential, names::NTuple{3,String})
    c.id_kind === nothing && return Pair{String,String}[]
    name = c.id_kind === :client_id ? names[1] : c.id_kind === :object_id ? names[2] : names[3]
    return [name => c.id_value::String]
end

function _mi_url(base::AbstractString, query::Vector{Pair{String,String}})
    q = join((string(k, "=", URIs.escapeuri(v)) for (k, v) in query), "&")
    return string(base, occursin('?', base) ? "&" : "?", q)
end

function _mi_error(resp, what)
    msg = _oauth_error_message(resp)
    if resp.status == 400 && c_identity_not_found(resp)
        return CredentialUnavailableError("$what: no managed identity is assigned or the requested identity was not found ($msg)")
    end
    return ClientAuthenticationError("$what: token request failed with $msg"; status_code=resp.status)
end

c_identity_not_found(resp) = occursin(r"identity not found|Identity not found|no managed identity"i, String(copy(resp.body)))

function _request_mi_token(c::ManagedIdentityCredential, resource::String)
    if c.kind === :imds
        return _imds_token(c, resource)
    elseif c.kind === :app_service
        url = _mi_url(c.endpoint, vcat(["api-version" => "2019-08-01", "resource" => resource],
            _id_query(c, ("client_id", "principal_id", "mi_res_id"))))
        resp = AzureCore.send_request("GET", url; headers=["X-IDENTITY-HEADER" => c.secret, "Metadata" => "true"],
            retry=c.retry, timeout=30.0)
        resp.status == 200 || throw(_mi_error(resp, "App Service managed identity"))
        return parse_token_response(String(copy(resp.body)); source="App Service managed identity")
    elseif c.kind === :cloud_shell
        c.id_kind === nothing || throw(CredentialUnavailableError("Cloud Shell only supports the system-assigned identity"))
        resp = AzureCore.send_request("POST", c.endpoint;
            headers=["Metadata" => "true", "Content-Type" => "application/x-www-form-urlencoded"],
            body=_form_encode(["resource" => resource]), retry=c.retry, timeout=30.0)
        resp.status == 200 || throw(_mi_error(resp, "Cloud Shell managed identity"))
        return parse_token_response(String(copy(resp.body)); source="Cloud Shell managed identity")
    elseif c.kind === :azure_arc
        return _arc_token(c, resource)
    elseif c.kind === :service_fabric
        throw(CredentialUnavailableError("Service Fabric managed identity is not supported by this package"))
    end
    throw(CredentialUnavailableError("unsupported managed identity environment $(c.kind)"))
end

function _imds_token(c::ManagedIdentityCredential, resource::String)
    url = _mi_url(c.endpoint, vcat(["api-version" => "2018-02-01", "resource" => resource],
        _id_query(c, ("client_id", "object_id", "msi_res_id"))))
    headers = ["Metadata" => "true", "User-Agent" => AzureCore.user_agent("identity")]
    if c.within_dac && !c.probed[]
        # Inside DefaultAzureCredential, fail fast when there is no IMDS endpoint.
        resp = try
            AzureCore.send_request("GET", url; headers=headers, retry=RetryOptions(retry_total=0),
                timeout=2.0, connect_timeout=1.0)
        catch err
            throw(CredentialUnavailableError("ManagedIdentityCredential authentication unavailable, no response from the IMDS endpoint."; error=err))
        end
        c.probed[] = true
        resp.status == 200 && return parse_token_response(String(copy(resp.body)); source="IMDS")
        resp.status in (400, 403) && throw(CredentialUnavailableError(
            "ManagedIdentityCredential authentication unavailable. The requested identity has not been assigned to this resource. ($(_oauth_error_message(resp)))"))
    end
    resp = AzureCore.send_request("GET", url; headers=headers, retry=c.retry, timeout=30.0,
        retry_status_codes=(404, 410, 429, 500, 502, 503, 504))
    c.probed[] = true
    resp.status == 200 || throw(_mi_error(resp, "IMDS managed identity"))
    return parse_token_response(String(copy(resp.body)); source="IMDS")
end

function _arc_key_dir()
    Sys.iswindows() ? joinpath(get(ENV, "ProgramData", "C:\\ProgramData"), "AzureConnectedMachineAgent", "Tokens") :
    Sys.islinux() ? "/var/opt/azcmagent/tokens" : nothing
end

function _arc_token(c::ManagedIdentityCredential, resource::String)
    c.id_kind === nothing || throw(ArgumentError("Azure Arc managed identity does not support user-assigned identities"))
    url = _mi_url(c.endpoint, ["api-version" => "2020-06-01", "resource" => resource])
    resp = AzureCore.send_request("GET", url; headers=["Metadata" => "true"], retry=c.retry, timeout=30.0,
        retry_status_codes=(429, 500, 502, 503, 504))
    resp.status == 401 || throw(ClientAuthenticationError("Azure Arc: expected a challenge response"; status_code=resp.status))
    challenge = HTTP.header(resp, "WWW-Authenticate", "")
    m = match(r"^Basic realm=(.+)$", strip(challenge))
    m === nothing && throw(ClientAuthenticationError("Azure Arc: malformed challenge"))
    keyfile = String(m.captures[1])
    # The key file path comes from the network response: validate it strictly.
    expected = _arc_key_dir()
    expected === nothing && throw(CredentialUnavailableError("Azure Arc is not supported on this platform"))
    realpath(dirname(keyfile)) == realpath(expected) ||
        throw(ClientAuthenticationError("Azure Arc: unexpected key file location"))
    endswith(keyfile, ".key") || throw(ClientAuthenticationError("Azure Arc: key file must have a .key extension"))
    filesize(keyfile) <= 4096 || throw(ClientAuthenticationError("Azure Arc: key file is too large"))
    secret = strip(read(keyfile, String))
    resp = AzureCore.send_request("GET", url; headers=["Metadata" => "true", "Authorization" => "Basic $secret"],
        retry=c.retry, timeout=30.0)
    resp.status == 200 || throw(_mi_error(resp, "Azure Arc managed identity"))
    return parse_token_response(String(copy(resp.body)); source="Azure Arc managed identity")
end
