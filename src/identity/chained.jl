# Credential chains: EnvironmentCredential, ChainedTokenCredential,
# DefaultAzureCredential and the bearer token provider helper.

"""
    EnvironmentCredential()

Configured from environment variables:

- service principal with secret: `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET`
- service principal with certificate: `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`,
  `AZURE_CLIENT_CERTIFICATE_PATH`, optionally `AZURE_CLIENT_CERTIFICATE_PASSWORD`
  and `AZURE_CLIENT_SEND_CERTIFICATE_CHAIN`
- optional: `AZURE_AUTHORITY_HOST`, `AZURE_ADDITIONALLY_ALLOWED_TENANTS` (`;`-separated)

Username/password authentication is not supported (Microsoft has deprecated
it; it does not work with multi-factor authentication).
"""
struct EnvironmentCredential <: TokenCredential
    credential::Union{Nothing, TokenCredential}
    unavailable_message::String
end
function EnvironmentCredential(; authority = nothing, kwargs...)
    env(k) = get(ENV, k, "")
    tenant, client = env("AZURE_TENANT_ID"), env("AZURE_CLIENT_ID")
    allowed = filter(!isempty, split(env("AZURE_ADDITIONALLY_ALLOWED_TENANTS"), ';'))
    if !isempty(tenant) && !isempty(client) && !isempty(env("AZURE_CLIENT_SECRET"))
        return EnvironmentCredential(
            ClientSecretCredential(
                tenant, client, env("AZURE_CLIENT_SECRET");
                authority = authority, additionally_allowed_tenants = allowed
            ), ""
        )
    elseif !isempty(tenant) && !isempty(client) && !isempty(env("AZURE_CLIENT_CERTIFICATE_PATH"))
        pw = env("AZURE_CLIENT_CERTIFICATE_PASSWORD")
        chain = lowercase(env("AZURE_CLIENT_SEND_CERTIFICATE_CHAIN")) in ("1", "true")
        cred = try
            ClientCertificateCredential(
                tenant, client, env("AZURE_CLIENT_CERTIFICATE_PATH");
                password = isempty(pw) ? nothing : pw, send_certificate_chain = chain, authority = authority,
                additionally_allowed_tenants = allowed
            )
        catch err
            return EnvironmentCredential(
                nothing,
                "EnvironmentCredential: the certificate in AZURE_CLIENT_CERTIFICATE_PATH could not be loaded ($(sprint(showerror, err)))"
            )
        end
        return EnvironmentCredential(cred, "")
    elseif !isempty(env("AZURE_USERNAME")) && !isempty(env("AZURE_PASSWORD"))
        return EnvironmentCredential(
            nothing,
            "EnvironmentCredential: username/password authentication is not supported by this package"
        )
    end
    return EnvironmentCredential(
        nothing,
        "EnvironmentCredential authentication unavailable. Environment variables are not fully configured."
    )
end
function Base.show(io::IO, c::EnvironmentCredential)
    print(io, "EnvironmentCredential(")
    c.credential === nothing ? print(io, "unavailable") : show(io, c.credential)
    return print(io, ")")
end

function AzureCore.get_token_info(c::EnvironmentCredential, scopes::AbstractString...; kwargs...)
    c.credential === nothing && throw(CredentialUnavailableError(c.unavailable_message))
    return get_token_info(c.credential, scopes...; kwargs...)
end

"""
    ChainedTokenCredential(credentials...)

Try each credential in order. A [`CredentialUnavailableError`](@ref) moves on
to the next credential; any other failure stops the chain and raises a
`ClientAuthenticationError`.
"""
mutable struct ChainedTokenCredential <: TokenCredential
    credentials::Vector{TokenCredential}
    successful::Union{Nothing, TokenCredential}
    remember_successful::Bool
end
function ChainedTokenCredential(credentials::TokenCredential...)
    isempty(credentials) && throw(ArgumentError("at least one credential is required"))
    return ChainedTokenCredential(collect(TokenCredential, credentials), nothing, false)
end
Base.show(io::IO, c::ChainedTokenCredential) =
    print(io, "ChainedTokenCredential(", join((nameof(typeof(x)) for x in c.credentials), ", "), ")")

function AzureCore.get_token_info(c::ChainedTokenCredential, scopes::AbstractString...; kwargs...)
    if c.remember_successful && c.successful !== nothing
        return get_token_info(c.successful, scopes...; kwargs...)
    end
    history = String[]
    for cred in c.credentials
        try
            tok = get_token_info(cred, scopes...; kwargs...)
            c.successful = cred
            @debug "$(nameof(typeof(c))) acquired a token from $(nameof(typeof(cred)))"
            return tok
        catch err
            if err isa CredentialUnavailableError
                push!(history, "$(nameof(typeof(cred))): $(err.message)")
                continue
            end
            err isa InterruptException && rethrow()
            msg = err isa AzureError ? err.message : sprint(showerror, err)
            push!(history, "$(nameof(typeof(cred))): $msg")
            throw(
                ClientAuthenticationError(
                    "$(nameof(typeof(c))) failed to retrieve a token from the included credentials.\nAttempted credentials:\n\t" *
                        join(history, "\n\t"); error = err
                )
            )
        end
    end
    throw(
        ClientAuthenticationError(
            "$(nameof(typeof(c))) failed to retrieve a token from the included credentials.\nAttempted credentials:\n\t" *
                join(history, "\n\t") *
                "\nSee https://aka.ms/azsdk/python/identity/defaultazurecredential/troubleshoot for troubleshooting guidance (it applies to this package as well)."
        )
    )
end

"""
    DefaultAzureCredential(; exclude_environment_credential=false,
                           exclude_workload_identity_credential=false,
                           exclude_managed_identity_credential=false,
                           exclude_cli_credential=false,
                           exclude_developer_cli_credential=false,
                           managed_identity_client_id=ENV["AZURE_CLIENT_ID"],
                           workload_identity_client_id=ENV["AZURE_CLIENT_ID"],
                           workload_identity_tenant_id=ENV["AZURE_TENANT_ID"],
                           authority=nothing, process_timeout=10,
                           additionally_allowed_tenants=String[])

The default credential chain, in the same order as the Python SDK:

1. [`EnvironmentCredential`](@ref)
2. [`WorkloadIdentityCredential`](@ref) (if its environment is configured)
3. [`ManagedIdentityCredential`](@ref)
4. [`AzureCliCredential`](@ref)
5. [`AzureDeveloperCliCredential`](@ref)

Set `AZURE_TOKEN_CREDENTIALS=dev` (developer tools only), `prod` (environment,
workload identity, managed identity only), or the name of one credential
(e.g. `ManagedIdentityCredential`) to restrict the chain. Once a credential
succeeds it is used for all later requests.
"""
function DefaultAzureCredential(;
        exclude_environment_credential::Bool = false,
        exclude_workload_identity_credential::Bool = false,
        exclude_managed_identity_credential::Bool = false,
        exclude_cli_credential::Bool = false,
        exclude_developer_cli_credential::Bool = false,
        exclude_powershell_credential::Bool = true,
        exclude_shared_token_cache_credential::Bool = true,
        exclude_visual_studio_code_credential::Bool = true,
        exclude_interactive_browser_credential::Bool = true,
        managed_identity_client_id = get(ENV, "AZURE_CLIENT_ID", nothing),
        workload_identity_client_id = get(ENV, "AZURE_CLIENT_ID", nothing),
        workload_identity_tenant_id = get(ENV, "AZURE_TENANT_ID", nothing),
        authority = nothing, process_timeout::Real = 10, additionally_allowed_tenants = String[]
    )
    selection = lowercase(strip(get(ENV, "AZURE_TOKEN_CREDENTIALS", "")))
    dev_ok = selection in ("", "dev")
    prod_ok = selection in ("", "prod")
    only = selection in ("", "dev", "prod") ? nothing : selection
    use(name, is_prod, excluded) = only === nothing ? (!excluded && (is_prod ? prod_ok : dev_ok)) : only == lowercase(name)
    if only !== nothing && !(
            only in (
                "environmentcredential", "workloadidentitycredential", "managedidentitycredential",
                "azureclicredential", "azuredeveloperclicredential",
            )
        )
        throw(ArgumentError("Invalid value for AZURE_TOKEN_CREDENTIALS: $(get(ENV, "AZURE_TOKEN_CREDENTIALS", ""))"))
    end
    creds = TokenCredential[]
    use("EnvironmentCredential", true, exclude_environment_credential) && push!(creds, EnvironmentCredential(; authority))
    if use("WorkloadIdentityCredential", true, exclude_workload_identity_credential)
        file = get(ENV, "AZURE_FEDERATED_TOKEN_FILE", "")
        if !isempty(file) && workload_identity_client_id !== nothing && workload_identity_tenant_id !== nothing
            push!(
                creds, WorkloadIdentityCredential(
                    tenant_id = workload_identity_tenant_id,
                    client_id = workload_identity_client_id, token_file_path = file, authority = authority,
                    additionally_allowed_tenants = additionally_allowed_tenants
                )
            )
        end
    end
    if use("ManagedIdentityCredential", true, exclude_managed_identity_credential)
        push!(creds, ManagedIdentityCredential(client_id = managed_identity_client_id, _within_dac = only === nothing))
    end
    use("AzureCliCredential", false, exclude_cli_credential) &&
        push!(creds, AzureCliCredential(; process_timeout, additionally_allowed_tenants))
    use("AzureDeveloperCliCredential", false, exclude_developer_cli_credential) &&
        push!(creds, AzureDeveloperCliCredential(; process_timeout, additionally_allowed_tenants))
    isempty(creds) && throw(ArgumentError("DefaultAzureCredential: all credentials are excluded"))
    chain = ChainedTokenCredential(creds...)
    chain.remember_successful = true
    return DefaultAzureCredentialType(chain)
end

"The type returned by [`DefaultAzureCredential`](@ref)."
struct DefaultAzureCredentialType <: TokenCredential
    chain::ChainedTokenCredential
end
Base.show(io::IO, c::DefaultAzureCredentialType) =
    print(io, "DefaultAzureCredential(", join((nameof(typeof(x)) for x in c.chain.credentials), ", "), ")")

function AzureCore.get_token_info(c::DefaultAzureCredentialType, scopes::AbstractString...; kwargs...)
    try
        return get_token_info(c.chain, scopes...; kwargs...)
    catch err
        if err isa ClientAuthenticationError
            throw(
                ClientAuthenticationError(
                    replace(err.message, "ChainedTokenCredential" => "DefaultAzureCredential");
                    error = err.error
                )
            )
        end
        rethrow()
    end
end

"""
    get_bearer_token_provider(credential, scopes...) -> Function

Return a zero-argument function that yields a bearer token string for
`scopes`, refreshing it as needed.
"""
function get_bearer_token_provider(credential::TokenCredential, scopes::AbstractString...)
    isempty(scopes) && throw(ArgumentError("at least one scope is required"))
    s = String[String(x) for x in scopes]
    return () -> get_token(credential, s...).token
end
