"""
    AzureSDK.Storage.Blob

Azure Blob Storage client modelled on Python's `azure.storage.blob`:
[`BlobServiceClient`](@ref), [`ContainerClient`](@ref), [`BlobClient`](@ref)
and [`BlobLeaseClient`](@ref); block, page and append blobs; parallel
chunked uploads and downloads ([`StorageStreamDownloader`](@ref));
snapshots, versions, tags, tiers, copies, leases, batch operations and
shared access signatures. Authentication: Shared Key (account key), SAS,
Microsoft Entra ID (`TokenCredential`, e.g. workload identity) or anonymous.
"""
module Blob

using Base64: base64encode, base64decode
using Dates
using UUIDs: uuid4
import HTTP
import SHA
import URIs

using ...AzureCore
import ...AzureCore: from_connection_string
using ..Storage: StorageConfig, storage_request, storage_error, response_dict, build_url, parse_storage_connection_string,
    quote_url, unquote_url, encode_query, query_pairs, rfc1123, iso8601, to_utc, parse_datetime, md5, _authorize!,
    XElement, RawXML, xml_parse, xml_element, xml_document, xfind, xfindall, xtext, xchildtext, xattr, StorageErrorCode,
    DEFAULT_API_VERSION, ContentValidationError

export BlobServiceClient, ContainerClient, BlobClient, BlobLeaseClient, StorageStreamDownloader, from_connection_string,
    from_container_url, from_blob_url, get_container_client, get_blob_client,
    # service
    get_account_information, list_containers, create_container, delete_container, undelete_container,
    get_service_properties, set_service_properties, get_service_stats, get_user_delegation_key, find_blobs_by_tags,
    # container
    exists, get_container_properties, set_container_metadata, get_container_access_policy,
    set_container_access_policy, list_blobs, list_blob_names, walk_blobs, upload_blob, download_blob, delete_blob,
    delete_blobs, set_standard_blob_tier_blobs, set_premium_page_blob_tier_blobs, acquire_lease,
    # blob
    get_blob_properties, set_http_headers, set_blob_metadata, undelete_blob, create_snapshot, start_copy_from_url,
    abort_copy, upload_blob_from_url, set_standard_blob_tier, set_premium_page_blob_tier, set_blob_tags, get_blob_tags,
    set_immutability_policy, delete_immutability_policy, set_legal_hold, stage_block, stage_block_from_url,
    get_block_list, commit_block_list, create_page_blob, upload_page, upload_pages_from_url, clear_page,
    get_page_ranges, list_page_ranges, resize_blob, set_sequence_number, create_append_blob, append_block,
    append_block_from_url, seal_append_blob,
    # downloads
    readall, readinto, chunks, content_as_text, content_as_bytes,
    # leases
    acquire, renew, release, change, break_lease,
    # SAS
    generate_account_sas, generate_container_sas, generate_blob_sas,
    # models
    BlobType, StandardBlobTier, PremiumPageBlobTier, RehydratePriority, PublicAccess, SequenceNumberAction, BlockState,
    BlobImmutabilityPolicyMode, ContentSettings, LeaseProperties, CopyProperties, ImmutabilityPolicy, BlobProperties,
    ContainerProperties, BlobPrefix, BlobBlock, PageRange, AccessPolicy, SignedIdentifier, CustomerProvidedEncryptionKey,
    UserDelegationKey, FilteredBlob, RetentionPolicy, BlobAnalyticsLogging, Metrics, CorsRule, StaticWebsite,
    BlobSasPermissions, ContainerSasPermissions, AccountSasPermissions, ResourceTypes, Services, BatchSubResponse,
    PartialBatchErrorException, StorageErrorCode, ContentValidationError,
    # re-exported from AzureCore
    MatchConditions, ItemPaged, by_page, CaseInsensitiveDict, AzureNamedKeyCredential, AzureSasCredential, AzureError, HttpResponseError,
    ResourceNotFoundError, ResourceExistsError, ResourceModifiedError, ResourceNotModifiedError,
    ClientAuthenticationError, ServiceRequestError

include("models.jl")
include("clients.jl")
include("service.jl")
include("container.jl")
include("blob.jl")
include("transfer.jl")
include("lease.jl")
include("sas.jl")
include("batch.jl")

end # module
