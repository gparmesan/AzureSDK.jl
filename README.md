# AzureSDK.jl

A pure-Julia client library for **Azure Event Hubs**, **Azure Service Bus**,
**Azure Blob Storage** and **Microsoft Entra ID authentication**, modelled
closely on the official Azure SDK for Python (`azure-eventhub`,
`azure-servicebus`, `azure-storage-blob`, `azure-identity`, `azure-core`).

* **AMQP 1.0** over TLS (port 5671), **AMQP over WebSockets** (port 443, proxy
  support) and **HTTP/REST** transports
* **Shared access keys / signatures** and **Entra ID tokens** (workload identity,
  managed identity, service principals, Azure CLI, …) via claims-based security
  with automatic token refresh
* Automatic reconnection and retries with exponential back-off
* Written from scratch in Julia (no Python or C SDK underneath); TLS via
  [Reseau.jl](https://github.com/JuliaServices/Reseau.jl), HTTP/WebSockets via
  [HTTP.jl](https://github.com/JuliaWeb/HTTP.jl)

> This is an independent community project. It is not an official Microsoft
> product and is not affiliated with or endorsed by Microsoft.

## Installation

```julia
using Pkg
Pkg.add(url="<repository url>")      # or Pkg.develop(path="path/to/azure-sdk-julia")
```

Julia 1.10 or newer is required.

## Package layout

| Julia module          | Python package         |
|-----------------------|------------------------|
| `AzureSDK.Identity`   | `azure.identity`       |
| `AzureSDK.EventHubs`  | `azure.eventhub` (+ `azure.eventhub.extensions.checkpointstoreblob`) |
| `AzureSDK.ServiceBus` | `azure.servicebus`     |
| `AzureSDK.Storage.Blob` | `azure.storage.blob` |
| `AzureSDK.AzureCore`  | `azure.core` (credentials, errors, retry options) |
| `AzureSDK.AMQP`       | (`pyamqp`) the AMQP 1.0 engine |

Python methods become Julia functions that take the client as their first
argument, with the same names and keyword arguments:

| Python                                              | Julia                                                   |
|-----------------------------------------------------|---------------------------------------------------------|
| `EventHubProducerClient.from_connection_string(s, eventhub_name="h")` | `from_connection_string(EventHubProducerClient, s; eventhub_name="h")` |
| `producer.create_batch(partition_key="k")`          | `create_batch(producer; partition_key="k")`              |
| `batch.add(EventData("x"))`                         | `add!(batch, EventData("x"))`                            |
| `producer.send_batch(batch)`                        | `send_batch(producer, batch)`                            |
| `consumer.receive(on_event=f, starting_position="-1")` | `receive(consumer, f; starting_position="-1")`        |
| `client.get_queue_receiver("q", max_wait_time=5)`   | `get_queue_receiver(client, "q"; max_wait_time=5)`       |
| `receiver.complete_message(msg)`                    | `complete_message(receiver, msg)`                        |
| `receiver.session.set_state(b"x")`                  | `set_state(receiver.session, "x")`                       |
| `with client: ...`                                  | `ServiceBusClient(...) do client ... end` or `close(client)` |
| `event.sequence_number`, `msg.lock_token`           | the same (`event.sequence_number`, `msg.lock_token`)    |
| `TransportType.AmqpOverWebsocket`                   | `TransportType.AmqpOverWebsocket`                        |
| `ServiceBusReceiveMode.PEEK_LOCK`                   | `ServiceBusReceiveMode.PEEK_LOCK`                        |
| `container.upload_blob("a.txt", data, overwrite=True)` | `upload_blob(container, "a.txt", data; overwrite=true)` |
| `blob.download_blob().readall()`                    | `readall(download_blob(blob))`                           |
| `for b in container.list_blobs(): ...`              | `for b in list_blobs(container) ... end`                 |

## Authentication

```julia
using AzureSDK.Identity

cred = DefaultAzureCredential()        # environment → workload identity → managed identity → az CLI → azd
token = get_token(cred, "https://eventhubs.azure.net/.default")
```

Available credentials: `DefaultAzureCredential`, `ChainedTokenCredential`,
`EnvironmentCredential`, `WorkloadIdentityCredential`,
`ManagedIdentityCredential` (IMDS, App Service/Functions/Container Apps,
Cloud Shell, Azure Arc, AKS token exchange), `ClientSecretCredential`,
`ClientCertificateCredential` (PEM or PKCS#12), `ClientAssertionCredential`,
`AzureCliCredential`, `AzureDeveloperCliCredential`, plus
`get_bearer_token_provider`. `AZURE_TOKEN_CREDENTIALS=dev|prod|<CredentialName>`
narrows the default chain as in Python.

The messaging clients also accept `AzureNamedKeyCredential(key_name, key)` and
`AzureSasCredential(signature)`, or a connection string.

## Event Hubs

```julia
using AzureSDK.EventHubs, AzureSDK.Identity

# publish
EventHubProducerClient("myns.servicebus.windows.net", "myhub", DefaultAzureCredential()) do producer
    batch = create_batch(producer; partition_key="device-42")
    add!(batch, EventData("""{"temperature": 21.5}"""; properties=Dict("type" => "reading")))
    add!(batch, EventData(rand(UInt8, 16)))
    send_batch(producer, batch)
end

# consume (blocks until close(consumer) is called, e.g. from a callback or another task)
consumer = from_connection_string(EventHubConsumerClient, ENV["EVENTHUB_CONNECTION_STRING"];
    consumer_group="\$Default", eventhub_name="myhub")
receive(consumer; starting_position="-1") do ctx, event
    println(ctx.partition_id, ": ", body_as_str(event), " seq=", event.sequence_number)
    update_checkpoint(ctx, event)
end
```

The Python-style call `receive(consumer, on_event; ...)` works as well.

* Load balancing across processes: pass
  `checkpoint_store=BlobCheckpointStore(account_url, container; credential=...)`
  (or `from_connection_string(BlobCheckpointStore, conn, container)`; it is
  built on `AzureSDK.Storage.Blob`). The blob layout is the same as in the
  other Azure SDKs, so Julia and Python consumers can share a consumer group.
* `receive_batch(consumer, on_event_batch; max_batch_size=300, max_wait_time=...)`
* `get_eventhub_properties`, `get_partition_ids`, `get_partition_properties`
* `owner_level`, `track_last_enqueued_event_properties`, `prefetch`,
  `load_balancing_strategy = LoadBalancingStrategy.GREEDY | BALANCED`

## Service Bus

```julia
using AzureSDK.ServiceBus

ServiceBusClient("myns.servicebus.windows.net", DefaultAzureCredential()) do client
    sender = get_queue_sender(client, "orders")
    send_messages(sender, ServiceBusMessage("order 1"; application_properties=Dict("priority" => 1)))
    send_messages(sender, [ServiceBusMessage("order $i") for i in 2:10])

    receiver = get_queue_receiver(client, "orders"; max_wait_time=5)
    for msg in receiver                          # stops after 5 s without messages
        try
            process(body_as_str(msg))
            complete_message(receiver, msg)
        catch
            abandon_message(receiver, msg)
        end
    end
end
```

Supported: queues, topics and subscriptions, peek-lock and receive-and-delete,
`complete/abandon/defer/dead_letter_message`, `renew_message_lock`,
`AutoLockRenewer`, `receive_deferred_messages`, `peek_messages`,
`schedule_messages` / `cancel_scheduled_messages`, dead-letter and transfer
dead-letter sub-queues, message batches, sessions
(`session_id="…"` or `NEXT_AVAILABLE_SESSION`, `get_state`, `set_state`,
`renew_lock`) and prefetching.

## Blob Storage

```julia
using AzureSDK.Storage.Blob, AzureSDK.Identity

svc = BlobServiceClient("https://myaccount.blob.core.windows.net"; credential=DefaultAzureCredential())
# or: from_connection_string(BlobServiceClient, ENV["AZURE_STORAGE_CONNECTION_STRING"])
container = create_container(svc, "reports")          # get_container_client(svc, "reports") if it exists

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
walk_blobs(container; delimiter="/")                   # BlobPrefix "directories" + BlobProperties
find_blobs_by_tags(svc, "\"project\"='alpha'")

# SAS tokens
sas = generate_blob_sas("myaccount", "reports", "2026/summary.json"; account_key=key,
    permission=BlobSasPermissions(read=true), expiry=Dates.Hour(1))
udk = get_user_delegation_key(svc, Dates.now(Dates.UTC), Dates.Hour(1))   # Entra ID: user delegation SAS
sas = generate_container_sas("myaccount", "reports"; user_delegation_key=udk, permission="rl", expiry=Dates.Hour(1))
```

Clients: `BlobServiceClient`, `ContainerClient`, `BlobClient` (construct
directly, `from_connection_string`, `from_container_url`, `from_blob_url`, or
`get_container_client` / `get_blob_client`) and `BlobLeaseClient`
(`acquire_lease`, `renew`, `release`, `change`, `break_lease`). Credentials:
`TokenCredential` (with `audience` and tenant discovery), account key
(`AzureNamedKeyCredential` or the key string), SAS (`AzureSasCredential`, a
SAS string or a SAS URL) or anonymous.

Supported: containers (create, delete, exists, properties, metadata, access
policies, listing with paging, undelete); block blobs (single-shot and
parallel chunked upload, `stage_block`, `stage_block_from_url`,
`commit_block_list`, `get_block_list`); page blobs (`create_page_blob`,
`upload_page`, `upload_pages_from_url`, `clear_page`, `get_page_ranges`,
`list_page_ranges`, `resize_blob`, `set_sequence_number`); append blobs
(`create_append_blob`, `append_block`, `append_block_from_url`,
`seal_append_blob`); ranged and parallel downloads with MD5 validation;
properties, HTTP headers, metadata, index tags, access tiers, snapshots,
versions, soft delete/undelete, copy (`start_copy_from_url`, `abort_copy`,
`upload_blob_from_url`), immutability policies and legal holds, leases,
conditional requests (`etag` + `MatchConditions`, `if_modified_since`,
`if_tags_match_condition`), customer-provided keys and encryption scopes,
batch deletes and tier changes (`delete_blobs`,
`set_standard_blob_tier_blobs`), service properties (logging, metrics, CORS,
soft delete, static website), `get_service_stats`,
`get_account_information`, and account / container / blob SAS generation
(account key or user delegation key).

Transfer tuning uses the Python keyword arguments on any client:
`max_single_put_size` (64 MiB), `max_block_size` (4 MiB), `max_page_size`,
`max_single_get_size` (32 MiB) and `max_chunk_get_size` (4 MiB); pass
`max_concurrency` to `upload_blob` / `download_blob`. Downloads pin the
blob's ETag, so a blob modified mid-download raises `ResourceModifiedError`
instead of returning mixed content.

Differences from Python: metadata is returned as a `CaseInsensitiveDict`
(HTTP.jl normalises header name case; Azure treats metadata names
case-insensitively); listing ETags are quoted like header ETags;
`PageRange` uses `start`/`stop` (inclusive); `readall` returns bytes unless
an `encoding` was given. Not implemented: client-side encryption,
`query_blob` (quick query), CRC64 content validation and reads from the
secondary endpoint.

## Transports, proxies and custom endpoints

```julia
client = ServiceBusClient(ns, cred;
    transport_type = TransportType.AmqpOverWebsocket,        # port 443
    http_proxy = (proxy_hostname="proxy.corp", proxy_port=3128, username="u", password="p"),
    connection_verify = "/etc/ssl/corp-ca.pem",              # custom CA bundle
    custom_endpoint_address = "https://gateway.corp:443")    # e.g. an application gateway
```

`TransportType.Http` uses the services' REST APIs. It supports Event Hubs
publishing, and for Service Bus sending, receiving (peek-lock and
receive-and-delete), complete, abandon and lock renewal. Everything else needs
AMQP. Over HTTP, application property names are case-insensitive (they travel
as HTTP headers).

## Local development with the emulators

Connection strings with `UseDevelopmentEmulator=true` connect to the local
[Event Hubs](https://github.com/Azure/azure-event-hubs-emulator-installer) and
[Service Bus](https://github.com/Azure/azure-service-bus-emulator-installer)
emulators without TLS. The flag is refused for non-local hosts.
`from_connection_string(BlobServiceClient, "UseDevelopmentStorage=true")`
connects to [Azurite](https://github.com/Azure/Azurite); plain `http://`
storage URLs are only accepted for local hosts.

```julia
cs = "Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;"
producer = from_connection_string(EventHubProducerClient, cs; eventhub_name="eh1")
```

## Errors and retries

Errors mirror the Python hierarchies (`EventHubError` and `ServiceBusError`
subtypes such as `ConnectionLostError`, `AuthenticationError`,
`MessageLockLostError`, `SessionLockLostError`, `MessageSizeExceededError`,
`ServiceBusServerBusyError`, …, all subtypes of `AzureCore.AzureError`).
Blob Storage raises the `azure.core` types (`ResourceNotFoundError`,
`ResourceExistsError`, `ResourceModifiedError`, `ClientAuthenticationError`,
`HttpResponseError`, `ServiceRequestError`) with the service's `error_code`
(compare with `StorageErrorCode.blob_not_found` etc.).
Transient failures (connection loss, timeouts, server busy) are retried
according to `retry_total`, `retry_backoff_factor`, `retry_backoff_max` and
`retry_mode`, and connections and links are re-established transparently.
`OperationTimeoutError` exists in both modules and is not exported; use
`EventHubs.OperationTimeoutError` / `ServiceBus.OperationTimeoutError`.

## Security

* TLS certificate and host name verification are always on unless you pass
  `connection_verify=false` (which logs a warning). TLS 1.2 is the minimum.
* Secrets never appear in `show` output, log messages or error messages;
  URLs in errors are stripped of their query strings (SAS signatures).
* HTTP requests never follow redirects or store cookies, so credentials are
  never forwarded to another host; Service Bus REST lock URLs must point back
  to the namespace host. Bearer tokens, account keys and SAS tokens are only
  sent over HTTPS (plain HTTP is limited to local emulators).
* The Storage XML reader rejects DTDs/entity declarations (no XXE or entity
  expansion), bounds nesting depth and requires valid UTF-8.
* The AMQP decoder validates every length and count against the received
  bytes, bounds nesting depth, frame and message sizes, so malformed or
  hostile input raises an error instead of exhausting memory.
* Event position filters are validated (no filter-expression injection);
  tenant IDs, scopes and client IDs are validated before being used in URLs or
  command lines. The Azure CLI and `azd` are executed directly, never through a
  shell, and with a timeout.
* The Azure Arc key file path returned by the local endpoint is validated
  against the agent's key directory before it is read.

## Performance

The AMQP engine pipelines deliveries (many sends in flight on one link),
coalesces outgoing frames into large socket writes, never holds a lock across
socket I/O, and decodes directly from receive buffers. On a laptop against the
in-process test broker it sustains well over 10,000 small messages per second
per link and tens of MB/s for large messages; real throughput is bounded by the
service tier. Batches (`create_batch` / `create_message_batch`) are much more
efficient than sending messages one at a time. A precompile workload keeps the
latency of the first operation low.

## Not implemented (yet)

Service Bus and Event Hubs **administration** clients (creating queues,
topics, hubs), the Event Hubs buffered producer and idempotent publishing, AMQP
transactions, Kafka, and the interactive / device code / PowerShell / VS Code
credentials. Service Fabric managed identity is not supported.

## Testing

```julia
julia --project -e 'using Pkg; Pkg.test()'
```

The suite uses an in-process mock AMQP broker (Service Bus semantics, TLS,
WebSockets), local HTTP stand-ins for Entra ID, IMDS and the REST APIs, and
generates throw-away certificates at run time. Integration tests against
Microsoft's local emulators run when enabled:

```sh
# Azurite (Blob Storage; MIT licensed, no EULA)
docker compose -f test/emulator/azurite-compose.yml up -d
AZURESDK_AZURITE=1 julia --project -e 'using Pkg; Pkg.test()'

# Event Hubs emulator + Azurite
ACCEPT_EULA=Y docker compose -f test/emulator/docker-compose.yml up -d
# Service Bus emulator + SQL Server (~2 GB RAM)
export MSSQL_SA_PASSWORD='<any strong password>'
ACCEPT_EULA=Y docker compose -f test/emulator/servicebus-compose.yml up -d

AZURESDK_EVENTHUBS_EMULATOR=1 AZURESDK_SERVICEBUS_EMULATOR=1 julia --project -e 'using Pkg; Pkg.test()'
```

Starting the Event Hubs and Service Bus emulators means accepting Microsoft's
emulator (and SQL Server) license terms. Note that those emulators do not
validate SAS keys and do not support TLS or WebSockets, so those paths are
covered by the mock broker. Azurite does validate Shared Key and SAS
signatures.

## Contributing

After cloning, run `sh scripts/configure-private-git.sh` to use a neutral
commit identity, get a `git ci` alias that records UTC timestamps, and install
a pre-commit hook that refuses changes containing your home directory path,
e-mail address or host name.

## License

MIT, see [LICENSE](LICENSE).
