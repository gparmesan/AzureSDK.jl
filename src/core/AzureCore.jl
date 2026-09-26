"""
    AzureSDK.AzureCore

Shared building blocks, modelled on Python's `azure.core`: credential types,
the exception hierarchy, retry options, a small hardened HTTP helper,
connection-string parsing and shared access signature generation.
"""
module AzureCore

using Base64: base64encode
using Dates
using SHA: hmac_sha256
import HTTP
import Reseau
import URIs

export AccessToken, AccessTokenInfo, TokenCredential, SupportsTokenInfo, get_token, get_token_info,
    AzureSasCredential, AzureNamedKeyCredential, AzureKeyCredential, update!, named_key,
    AzureError, ServiceRequestError, ServiceResponseError, HttpResponseError, ClientAuthenticationError,
    ResourceNotFoundError, ResourceExistsError, ResourceNotModifiedError, IncompleteReadError,
    RetryMode, RetryOptions, generate_sas_token, parse_connection_string_pairs,
    ResourceModifiedError, ItemPaged, PageIterator, by_page, MatchConditions, from_connection_string,
    CaseInsensitiveDict

include("credentials.jl")
include("exceptions.jl")
include("retry.jl")
include("http.jl")
include("sas.jl")
include("paging.jl")
include("utils.jl")

end # module
