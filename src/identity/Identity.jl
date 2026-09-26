"""
    AzureSDK.Identity

Microsoft Entra ID credentials modelled on Python's `azure.identity`:
[`DefaultAzureCredential`](@ref), [`ChainedTokenCredential`](@ref),
[`EnvironmentCredential`](@ref), [`WorkloadIdentityCredential`](@ref),
[`ManagedIdentityCredential`](@ref), [`ClientSecretCredential`](@ref),
[`ClientCertificateCredential`](@ref), [`ClientAssertionCredential`](@ref),
[`AzureCliCredential`](@ref) and [`AzureDeveloperCliCredential`](@ref).

All credentials are thread-safe, cache tokens in memory and refresh them
ahead of expiry. Secrets are never included in `show` output, logs or error
messages.
"""
module Identity

using Base64: base64encode, base64decode
using Dates
using UUIDs: uuid4
import HTTP
import JSON
import OpenSSL_jll
import SHA
import URIs

using ..AzureCore
using ..AzureCore: AbstractHttpResponseError
import ..AzureCore: get_token_info, get_token

export DefaultAzureCredential, ChainedTokenCredential, EnvironmentCredential, ClientSecretCredential,
    ClientCertificateCredential, ClientAssertionCredential, WorkloadIdentityCredential,
    ManagedIdentityCredential, AzureCliCredential, AzureDeveloperCliCredential,
    CredentialUnavailableError, AzureAuthorityHosts, get_bearer_token_provider,
    get_token, get_token_info, AccessToken, AccessTokenInfo, TokenCredential

include("common.jl")
include("certificate.jl")
include("client_credentials.jl")
include("managed_identity.jl")
include("developer.jl")
include("chained.jl")

end # module
