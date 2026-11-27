# Credential types (azure.core.credentials).

"""
    AccessToken(token, expires_on)

An access token and its expiry as a Unix timestamp (seconds). The token value
is never printed.
"""
struct AccessToken
    token::String
    expires_on::Int
end
Base.show(io::IO, t::AccessToken) = print(io, "AccessToken(token=<redacted>, expires_on=", t.expires_on, ")")

"""
    AccessTokenInfo(token, expires_on; token_type="Bearer", refresh_on=nothing)

Richer token information (`azure.core.credentials.AccessTokenInfo`).
`refresh_on` is the Unix time after which the token should be refreshed.
"""
struct AccessTokenInfo
    token::String
    expires_on::Int
    token_type::String
    refresh_on::Union{Nothing, Int}
end
AccessTokenInfo(
    token::AbstractString, expires_on::Integer; token_type::AbstractString = "Bearer",
    refresh_on::Union{Nothing, Integer} = nothing
) =
    AccessTokenInfo(String(token), Int(expires_on), String(token_type), refresh_on === nothing ? nothing : Int(refresh_on))
function Base.show(io::IO, t::AccessTokenInfo)
    return print(
        io, "AccessTokenInfo(token=<redacted>, expires_on=", t.expires_on, ", token_type=", repr(t.token_type),
        ", refresh_on=", t.refresh_on, ")"
    )
end

"""
    TokenCredential

Abstract supertype of credentials that can issue Entra ID access tokens. A
subtype must implement

    get_token_info(cred, scopes::AbstractString...; claims=nothing, tenant_id=nothing, enable_cae=false)

returning an [`AccessTokenInfo`](@ref). [`get_token`](@ref) is derived from it.
"""
abstract type TokenCredential end

"Alias kept for parity with Python's `SupportsTokenInfo` protocol."
const SupportsTokenInfo = TokenCredential

"""
    get_token_info(credential, scopes...; claims=nothing, tenant_id=nothing, enable_cae=false) -> AccessTokenInfo

Request an access token for `scopes`.
"""
function get_token_info end

"""
    get_token(credential, scopes...; claims=nothing, tenant_id=nothing, enable_cae=false) -> AccessToken

Request an access token for `scopes` (`azure.core.credentials.TokenCredential.get_token`).
"""
function get_token(
        cred::TokenCredential, scopes::AbstractString...; claims = nothing, tenant_id = nothing,
        enable_cae::Bool = false
    )
    info = get_token_info(cred, scopes...; claims = claims, tenant_id = tenant_id, enable_cae = enable_cae)
    return AccessToken(info.token, info.expires_on)
end

"""
    AzureSasCredential(signature)

A shared access signature. Use [`update!`](@ref) to rotate it; clients pick up
the new value the next time they authorize.
"""
mutable struct AzureSasCredential
    signature::String
    lock::ReentrantLock
end
function AzureSasCredential(signature::AbstractString)
    isempty(signature) && throw(ArgumentError("signature must be a non-empty string"))
    return AzureSasCredential(String(signature), ReentrantLock())
end
Base.show(io::IO, ::AzureSasCredential) = print(io, "AzureSasCredential(<redacted>)")
signature(c::AzureSasCredential) = @lock c.lock c.signature

"""
    AzureNamedKeyCredential(name, key)

A shared access key name and key (e.g. a Service Bus / Event Hubs shared
access policy). Use [`update!`](@ref) to rotate and [`named_key`](@ref) to read.
"""
mutable struct AzureNamedKeyCredential
    name::String
    key::String
    lock::ReentrantLock
end
function AzureNamedKeyCredential(name::AbstractString, key::AbstractString)
    (isempty(name) || isempty(key)) && throw(ArgumentError("name and key must be non-empty strings"))
    return AzureNamedKeyCredential(String(name), String(key), ReentrantLock())
end
Base.show(io::IO, c::AzureNamedKeyCredential) = print(io, "AzureNamedKeyCredential(name=", repr(c.name), ", key=<redacted>)")

"Return `(name = ..., key = ...)` of a named key credential."
named_key(c::AzureNamedKeyCredential) = @lock c.lock (name = c.name, key = c.key)

"""
    AzureKeyCredential(key)

An API key credential.
"""
mutable struct AzureKeyCredential
    key::String
    lock::ReentrantLock
end
AzureKeyCredential(key::AbstractString) = isempty(key) ? throw(ArgumentError("key must be a non-empty string")) :
    AzureKeyCredential(String(key), ReentrantLock())
Base.show(io::IO, ::AzureKeyCredential) = print(io, "AzureKeyCredential(<redacted>)")

"""
    update!(cred::AzureSasCredential, signature)
    update!(cred::AzureNamedKeyCredential, name, key)
    update!(cred::AzureKeyCredential, key)

Rotate the secret held by a credential.
"""
function update!(c::AzureSasCredential, signature::AbstractString)
    isempty(signature) && throw(ArgumentError("signature must be a non-empty string"))
    @lock c.lock c.signature = String(signature)
    return c
end
function update!(c::AzureNamedKeyCredential, name::AbstractString, key::AbstractString)
    (isempty(name) || isempty(key)) && throw(ArgumentError("name and key must be non-empty strings"))
    @lock c.lock begin
        c.name = String(name)
        c.key = String(key)
    end
    return c
end
function update!(c::AzureKeyCredential, key::AbstractString)
    isempty(key) && throw(ArgumentError("key must be a non-empty string"))
    @lock c.lock c.key = String(key)
    return c
end
