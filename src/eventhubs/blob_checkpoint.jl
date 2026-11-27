# BlobCheckpointStore: checkpoints and partition ownership in Azure Blob
# Storage, compatible with the layout used by the Python/.NET/Java SDKs
# (azure-eventhub-checkpointstoreblob), so consumers written in different
# languages can share a consumer group. Built on AzureSDK.Storage.Blob, as the
# Python package is built on azure-storage-blob.

"""
    BlobCheckpointStore(blob_account_url, container_name; credential=nothing, kwargs...)
    BlobCheckpointStore(container_client::ContainerClient)
    from_connection_string(BlobCheckpointStore, conn_str, container_name; kwargs...)

A [`CheckpointStore`](@ref) backed by an Azure Storage blob container (which
must exist). `credential` accepts everything a Blob `ContainerClient`
accepts: a `TokenCredential` (Entra ID; needs the *Storage Blob Data
Contributor* role), an `AzureNamedKeyCredential` or account key, an
`AzureSasCredential` or SAS string, or `nothing` when `blob_account_url`
carries a SAS. Other keyword arguments (e.g. `retry_total`, `api_version`,
`connection_verify`) are passed to the `ContainerClient`.

Blob layout (shared with the other Azure SDKs):
`<namespace>/<eventhub>/<consumer group>/ownership/<partition>` and
`.../checkpoint/<partition>`, with the data in blob metadata.
"""
struct BlobCheckpointStore <: CheckpointStore
    container::ContainerClient
end

BlobCheckpointStore(blob_account_url::AbstractString, container_name::AbstractString; credential = nothing, kwargs...) =
    BlobCheckpointStore(ContainerClient(blob_account_url, container_name; credential = credential, kwargs...))

from_connection_string(::Type{BlobCheckpointStore}, conn_str::AbstractString, container_name::AbstractString; kwargs...) =
    BlobCheckpointStore(from_connection_string(ContainerClient, conn_str, container_name; kwargs...))

Base.show(io::IO, s::BlobCheckpointStore) = print(
    io, "BlobCheckpointStore(", s.container.primary_endpoint, "/",
    s.container.container_name, ")"
)

_prefix(fqns, eh, cg, kind) = string(lowercase(fqns), "/", lowercase(eh), "/", lowercase(cg), "/", kind, "/")

_unix(t::Union{Nothing, DateTime}) = t === nothing ? time() : Dates.datetime2unix(t)

"All blobs under `prefix`, with metadata."
_list_blobs(s::BlobCheckpointStore, prefix::String) = collect(list_blobs(s.container; name_starts_with = prefix, include = "metadata"))

function list_ownership(s::BlobCheckpointStore, fully_qualified_namespace, eventhub_name, consumer_group)
    prefix = _prefix(fully_qualified_namespace, eventhub_name, consumer_group, "ownership")
    return [
        Dict{String, Any}(
            "fully_qualified_namespace" => fully_qualified_namespace, "eventhub_name" => eventhub_name,
            "consumer_group" => consumer_group, "partition_id" => b.name[(length(prefix) + 1):end],
            "owner_id" => get(b.metadata, "ownerid", ""), "etag" => b.etag, "last_modified_time" => _unix(b.last_modified)
        )
            for b in _list_blobs(s, prefix)
    ]
end

function claim_ownership(s::BlobCheckpointStore, ownerships::AbstractVector)
    claimed = Dict{String, Any}[]
    for o in ownerships
        name = _prefix(o["fully_qualified_namespace"], o["eventhub_name"], o["consumer_group"], "ownership") *
            string(o["partition_id"])
        etag = get(o, "etag", nothing)
        # Optimistic concurrency: only succeed if nobody claimed the partition since we listed it.
        cond = etag === nothing ? (; match_condition = MatchConditions.IfMissing) :
            (; etag = string(etag), match_condition = MatchConditions.IfNotModified)
        result = try
            upload_blob(
                get_blob_client(s.container, name), UInt8[]; overwrite = true,
                metadata = Dict("ownerid" => string(o["owner_id"])), cond...
            )
        catch err
            if err isa Union{ResourceExistsError, ResourceModifiedError}
                @debug "partition already claimed by another consumer" partition = o["partition_id"]
            else
                @debug "claiming partition ownership failed" partition = o["partition_id"] exception = err
            end
            continue
        end
        new = Dict{String, Any}(o)
        new["etag"] = get(result, "etag", nothing)
        new["last_modified_time"] = _unix(get(result, "last_modified", nothing))
        push!(claimed, new)
    end
    return claimed
end

function update_checkpoint(s::BlobCheckpointStore, checkpoint::AbstractDict)
    name = _prefix(
        checkpoint["fully_qualified_namespace"], checkpoint["eventhub_name"], checkpoint["consumer_group"],
        "checkpoint"
    ) * string(checkpoint["partition_id"])
    md = Dict{String, String}()
    off = get(checkpoint, "offset", nothing)
    off === nothing || (md["offset"] = string(off))
    sn = get(checkpoint, "sequence_number", nothing)
    sn === nothing || (md["sequencenumber"] = string(sn))
    upload_blob(get_blob_client(s.container, name), UInt8[]; overwrite = true, metadata = md)
    return nothing
end

function list_checkpoints(s::BlobCheckpointStore, fully_qualified_namespace, eventhub_name, consumer_group)
    prefix = _prefix(fully_qualified_namespace, eventhub_name, consumer_group, "checkpoint")
    out = Dict{String, Any}[]
    for b in _list_blobs(s, prefix)
        sn = tryparse(Int64, get(b.metadata, "sequencenumber", ""))
        push!(
            out, Dict{String, Any}(
                "fully_qualified_namespace" => fully_qualified_namespace,
                "eventhub_name" => eventhub_name, "consumer_group" => consumer_group,
                "partition_id" => b.name[(length(prefix) + 1):end], "offset" => get(b.metadata, "offset", nothing),
                "sequence_number" => sn
            )
        )
    end
    return out
end
