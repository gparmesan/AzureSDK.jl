# Confidential client credentials: client secret, client assertion, workload
# identity (federated token file) and certificate.

abstract type EntraTokenCredential <: TokenCredential end

function AzureCore.get_token_info(cred::EntraTokenCredential, scopes::AbstractString...; claims=nothing,
    tenant_id=nothing, enable_cae::Bool=false)
    scopes_v = _require_scopes(scopes)
    c = cred.client::EntraClient
    tenant = resolve_tenant(c.tenant_id, tenant_id, c.additionally_allowed_tenants)
    claims_eff = _merge_claims(claims, enable_cae)
    key = (sort(scopes_v), tenant, enable_cae)
    bypass = claims !== nothing && !isempty(claims)
    return cached_or_fetch(cred.cache, key; bypass=bypass) do
        request_token(c, scopes_v, tenant, grant_params(cred, tenant); claims=claims_eff)
    end
end

# --- client secret -------------------------------------------------------------

"""
    ClientSecretCredential(tenant_id, client_id, client_secret; authority=nothing,
                           additionally_allowed_tenants=String[])

Authenticate a service principal with a client secret.
"""
struct ClientSecretCredential <: EntraTokenCredential
    client::EntraClient
    client_secret::String
    cache::TokenCache
end
function ClientSecretCredential(tenant_id::AbstractString, client_id::AbstractString, client_secret::AbstractString;
    authority=nothing, additionally_allowed_tenants=String[], kwargs...)
    isempty(client_secret) && throw(ArgumentError("secret should be a Microsoft Entra application's client secret"))
    ClientSecretCredential(EntraClient(tenant_id, client_id; authority, additionally_allowed_tenants, kwargs...),
        String(client_secret), TokenCache())
end
grant_params(c::ClientSecretCredential, ::AbstractString) =
    Pair{String,String}["grant_type" => "client_credentials", "client_secret" => c.client_secret]
Base.show(io::IO, c::ClientSecretCredential) =
    print(io, "ClientSecretCredential(tenant_id=", repr(c.client.tenant_id), ", client_id=", repr(c.client.client_id), ")")

# --- client assertion ------------------------------------------------------------

const JWT_BEARER = "urn:ietf:params:oauth:client-assertion-type:jwt-bearer"

"""
    ClientAssertionCredential(tenant_id, client_id, func; authority=nothing,
                              additionally_allowed_tenants=String[])

Authenticate with a client assertion (a signed JWT, e.g. a federated token)
returned by calling `func()`.
"""
struct ClientAssertionCredential <: EntraTokenCredential
    client::EntraClient
    func::Function
    cache::TokenCache
end
function ClientAssertionCredential(tenant_id::AbstractString, client_id::AbstractString, func::Function;
    authority=nothing, additionally_allowed_tenants=String[], kwargs...)
    ClientAssertionCredential(EntraClient(tenant_id, client_id; authority, additionally_allowed_tenants, kwargs...),
        func, TokenCache())
end
function grant_params(c::ClientAssertionCredential, ::AbstractString)
    assertion = c.func()
    (assertion isa AbstractString && !isempty(assertion)) ||
        throw(ClientAuthenticationError("client assertion function returned no assertion"))
    return Pair{String,String}["grant_type" => "client_credentials", "client_assertion_type" => JWT_BEARER,
        "client_assertion" => String(assertion)]
end
Base.show(io::IO, c::ClientAssertionCredential) =
    print(io, "ClientAssertionCredential(tenant_id=", repr(c.client.tenant_id), ", client_id=", repr(c.client.client_id), ")")

# --- workload identity ---------------------------------------------------------------

"""
    WorkloadIdentityCredential(; tenant_id=ENV["AZURE_TENANT_ID"], client_id=ENV["AZURE_CLIENT_ID"],
                               token_file_path=ENV["AZURE_FEDERATED_TOKEN_FILE"], authority=nothing,
                               additionally_allowed_tenants=String[])

Microsoft Entra Workload ID (e.g. on Azure Kubernetes Service): exchanges the
projected service account token in `token_file_path` for an access token. The
file is re-read at most every 5 minutes so rotated tokens are picked up.
"""
struct WorkloadIdentityCredential <: EntraTokenCredential
    client::EntraClient
    token_file_path::String
    cache::TokenCache
    assertion::Base.RefValue{Tuple{String,Float64}}
    lock::ReentrantLock
end
function WorkloadIdentityCredential(; tenant_id=get(ENV, "AZURE_TENANT_ID", ""),
    client_id=get(ENV, "AZURE_CLIENT_ID", ""), token_file_path=get(ENV, "AZURE_FEDERATED_TOKEN_FILE", ""),
    authority=nothing, additionally_allowed_tenants=nothing, kwargs...)
    isempty(something(tenant_id, "")) && throw(ArgumentError(
        "'tenant_id' is required. Please pass it in or set the AZURE_TENANT_ID environment variable."))
    isempty(something(client_id, "")) && throw(ArgumentError(
        "'client_id' is required. Please pass it in or set the AZURE_CLIENT_ID environment variable."))
    isempty(something(token_file_path, "")) && throw(ArgumentError(
        "'token_file_path' is required. Please pass it in or set the AZURE_FEDERATED_TOKEN_FILE environment variable."))
    allowed = additionally_allowed_tenants === nothing ?
              filter(!isempty, split(get(ENV, "AZURE_ADDITIONALLY_ALLOWED_TENANTS", ""), ';')) :
              additionally_allowed_tenants
    WorkloadIdentityCredential(EntraClient(tenant_id, client_id; authority, additionally_allowed_tenants=allowed, kwargs...),
        String(token_file_path), TokenCache(), Ref(("", 0.0)), ReentrantLock())
end
function grant_params(c::WorkloadIdentityCredential, ::AbstractString)
    assertion = @lock c.lock begin
        tok, read_at = c.assertion[]
        if isempty(tok) || time() - read_at > 300
            tok = try
                strip(read(c.token_file_path, String))
            catch err
                throw(CredentialUnavailableError("Failed to read the federated token file"; error=err))
            end
            isempty(tok) && throw(CredentialUnavailableError("The federated token file is empty"))
            c.assertion[] = (String(tok), time())
        end
        c.assertion[][1]
    end
    return Pair{String,String}["grant_type" => "client_credentials", "client_assertion_type" => JWT_BEARER,
        "client_assertion" => assertion]
end
Base.show(io::IO, c::WorkloadIdentityCredential) =
    print(io, "WorkloadIdentityCredential(tenant_id=", repr(c.client.tenant_id), ", client_id=", repr(c.client.client_id), ")")

# --- certificate -------------------------------------------------------------------

"""
    ClientCertificateCredential(tenant_id, client_id, certificate_path=nothing; certificate_data=nothing,
                                password=nothing, send_certificate_chain=false, authority=nothing,
                                additionally_allowed_tenants=String[])

Authenticate a service principal with a certificate. The certificate can be a
PEM file containing the certificate and its (optionally encrypted) private key,
or a PKCS#12 (`.pfx`) file. Client assertions are signed with RS256.
"""
struct ClientCertificateCredential <: EntraTokenCredential
    client::EntraClient
    cert::CertificateKey
    send_certificate_chain::Bool
    cache::TokenCache
end
function ClientCertificateCredential(tenant_id::AbstractString, client_id::AbstractString,
    certificate_path::Union{Nothing,AbstractString}=nothing; certificate_data=nothing, password=nothing,
    send_certificate_chain::Bool=false, authority=nothing, additionally_allowed_tenants=String[], kwargs...)
    data = if certificate_data !== nothing
        Vector{UInt8}(certificate_data)
    elseif certificate_path !== nothing
        read(certificate_path)
    else
        throw(ArgumentError("'certificate_path' or 'certificate_data' must be provided"))
    end
    cert = load_certificate(data, password)
    fill!(data, 0x00)
    ClientCertificateCredential(EntraClient(tenant_id, client_id; authority, additionally_allowed_tenants, kwargs...),
        cert, send_certificate_chain, TokenCache())
end
function grant_params(c::ClientCertificateCredential, tenant::AbstractString)
    aud = token_endpoint(c.client, tenant)
    assertion = client_assertion_jwt(c.cert, c.client.client_id, aud; send_chain=c.send_certificate_chain)
    return Pair{String,String}["grant_type" => "client_credentials", "client_assertion_type" => JWT_BEARER,
        "client_assertion" => assertion]
end
Base.show(io::IO, c::ClientCertificateCredential) =
    print(io, "ClientCertificateCredential(tenant_id=", repr(c.client.tenant_id), ", client_id=", repr(c.client.client_id),
        ", thumbprint=", bytes2hex(c.cert.thumbprint_sha1), ")")
