# Blob Storage

```julia
using AzureSDK.Storage.Blob, AzureSDK.Identity, Dates

svc = BlobServiceClient("https://myaccount.blob.core.windows.net"; credential=DefaultAzureCredential())
# or: from_connection_string(BlobServiceClient, ENV["AZURE_STORAGE_CONNECTION_STRING"])
container = create_container(svc, "reports")   # get_container_client(svc, "reports") if it exists

# upload: bytes, strings or any IO (files, pipes); large data is sent in parallel blocks
upload_blob(container, "2026/summary.json", """{"ok": true}""";
    content_settings=ContentSettings(content_type="application/json"), metadata=Dict("owner" => "etl"))
open("big.parquet") do io
    upload_blob(container, "data/big.parquet", io; overwrite=true, max_concurrency=8)
end

# download: everything, into an IO, chunk by chunk, or as a Julia IO stream
bytes = readall(download_blob(container, "data/big.parquet"; max_concurrency=8))
open("copy.parquet", "w") do io
    readinto(download_blob(container, "data/big.parquet"; max_concurrency=8), io)
end
text = content_as_text(download_blob(container, "2026/summary.json"))
for line in eachline(download_blob(container, "logs/app.log"))   # streams 4 MiB at a time
    occursin("ERROR", line) && println(line)
end

# list (lazily paged), hierarchically, by tag
for b in list_blobs(container; name_starts_with="2026/", include=["metadata"])
    println(b.name, " ", b.size, " ", b.last_modified)
end
walk_blobs(container; delimiter="/")            # BlobPrefix "directories" + BlobProperties
find_blobs_by_tags(svc, "\"project\"='alpha'")

# SAS tokens
sas = generate_blob_sas("myaccount", "reports", "2026/summary.json"; account_key=key,
    permission=BlobSasPermissions(read=true), expiry=Hour(1))
udk = get_user_delegation_key(svc, now(UTC), Hour(1))   # Entra ID: user delegation SAS
sas = generate_container_sas("myaccount", "reports"; user_delegation_key=udk, permission="rl", expiry=Hour(1))
```

## Clients

`BlobServiceClient`, `ContainerClient` and `BlobClient` can be constructed
directly, with `from_connection_string`, `from_container_url` or
`from_blob_url`, or with `get_container_client` / `get_blob_client`.
`BlobLeaseClient` provides `acquire_lease`, `renew`, `release`, `change` and
`break_lease`.

## Features

* **Containers:** create, delete, exists, properties, metadata, access
  policies, paged listing, undelete.
* **Block blobs:** single-shot and parallel chunked upload, `stage_block`,
  `stage_block_from_url`, `commit_block_list`, `get_block_list`.
* **Page blobs:** `create_page_blob`, `upload_page`, `upload_pages_from_url`,
  `clear_page`, `get_page_ranges`, `list_page_ranges`, `resize_blob`,
  `set_sequence_number`.
* **Append blobs:** `create_append_blob`, `append_block`,
  `append_block_from_url`, `seal_append_blob`.
* **Downloads:** ranged and parallel, with MD5 validation.
* **Blob management:** properties, HTTP headers, metadata, index tags, access
  tiers, snapshots, versions, soft delete and undelete, copy
  (`start_copy_from_url`, `abort_copy`, `upload_blob_from_url`),
  immutability policies and legal holds, leases.
* **Conditions and encryption:** `etag` + `MatchConditions`,
  `if_modified_since`, `if_tags_match_condition`, customer-provided keys and
  encryption scopes.
* **Batches:** `delete_blobs`, `set_standard_blob_tier_blobs`.
* **Account:** service properties (logging, metrics, CORS, soft delete, static
  website), `get_service_stats`, `get_account_information`.
* **SAS generation:** account, container and blob SAS, signed with an account
  key or a user delegation key.

## Transfer tuning

The Python keyword arguments work on any client: `max_single_put_size`
(64 MiB), `max_block_size` (4 MiB), `max_page_size`, `max_single_get_size`
(32 MiB) and `max_chunk_get_size` (4 MiB). Pass `max_concurrency` to
`upload_blob` and `download_blob`. Downloads pin the blob's ETag, so a blob
that is modified mid-download raises `ResourceModifiedError` instead of
returning mixed content.

## Differences from Python

* Metadata is returned as a `CaseInsensitiveDict`: HTTP.jl normalises the case
  of response header names, and Azure treats metadata names
  case-insensitively.
* ETags from listings are quoted like ETags from response headers.
* `PageRange` uses `start`/`stop` (both inclusive), since `end` is reserved in Julia.
* `readall` returns bytes unless an `encoding` was given.

See the [Blob Storage API reference](@ref "Blob Storage API") for all functions.
