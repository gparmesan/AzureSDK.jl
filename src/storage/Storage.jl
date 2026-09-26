"""
    AzureSDK.Storage

Azure Storage clients, mirroring Python's `azure.storage.*` packages:

- `AzureSDK.Storage.Blob` — `azure.storage.blob`

Shared pieces (Shared Key signing, the retrying HTTP pipeline, error
mapping, XML handling) live in this module.
"""
module Storage

using Base64: base64encode, base64decode
using Dates
using UUIDs: uuid4
import HTTP
import OpenSSL_jll
import SHA
import URIs

using ..AzureCore
using ..AzureCore: TokenCredential, named_key, parse_connection_string_pairs

export Blob, StorageErrorCode

include("xml.jl")
include("shared.jl")
include("blob/Blob.jl")

end # module
