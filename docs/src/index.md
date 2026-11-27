# AzureSDK.jl

A pure-Julia client library for **Azure Event Hubs**, **Azure Service Bus**,
**Azure Blob Storage** and **Microsoft Entra ID authentication**. Its structure
and API follow the official Azure SDK for Python (`azure-eventhub`,
`azure-servicebus`, `azure-storage-blob`, `azure-identity`, `azure-core`).

- **AMQP 1.0** over TLS (port 5671), **AMQP over WebSockets** (port 443, proxy
  support) and **HTTP/REST** transports
- **Shared access keys / signatures** and **Entra ID tokens** (workload identity,
  managed identity, service principals, Azure CLI, …) with automatic token refresh
- Automatic reconnection and retries with exponential back-off
- Written in Julia (no Python or C SDK underneath); TLS via
  [Reseau.jl](https://github.com/JuliaServices/Reseau.jl), HTTP and WebSockets via
  [HTTP.jl](https://github.com/JuliaWeb/HTTP.jl)

!!! note
    This is an independent community project. It is not an official Microsoft
    product and is not affiliated with or endorsed by Microsoft.

## Installation

```julia
using Pkg
Pkg.add("AzureSDK")        # before registration: Pkg.add(url="<repository url>")
```

Julia 1.10 or newer is required.

## Package layout

| Julia module            | Python package                                                       |
| :---------------------- | :------------------------------------------------------------------- |
| `AzureSDK.Identity`     | `azure.identity`                                                     |
| `AzureSDK.EventHubs`    | `azure.eventhub` (+ `azure.eventhub.extensions.checkpointstoreblob`) |
| `AzureSDK.ServiceBus`   | `azure.servicebus`                                                   |
| `AzureSDK.Storage.Blob` | `azure.storage.blob`                                                 |
| `AzureSDK.AzureCore`    | `azure.core` (credentials, errors, retry options, paging)            |
| `AzureSDK.AMQP`         | (`pyamqp`) the AMQP 1.0 engine                                       |

## Coming from Python

Python methods become Julia functions that take the client as their first
argument, with the same names and keyword arguments:

| Python                                                                | Julia                                                                  |
| :-------------------------------------------------------------------- | :--------------------------------------------------------------------- |
| `EventHubProducerClient.from_connection_string(s, eventhub_name="h")` | `from_connection_string(EventHubProducerClient, s; eventhub_name="h")` |
| `producer.create_batch(partition_key="k")`                            | `create_batch(producer; partition_key="k")`                            |
| `batch.add(EventData("x"))`                                           | `add!(batch, EventData("x"))`                                          |
| `producer.send_batch(batch)`                                          | `send_batch(producer, batch)`                                          |
| `consumer.receive(on_event=f, starting_position="-1")`                | `receive(consumer, f; starting_position="-1")`                         |
| `client.get_queue_receiver("q", max_wait_time=5)`                     | `get_queue_receiver(client, "q"; max_wait_time=5)`                     |
| `receiver.complete_message(msg)`                                      | `complete_message(receiver, msg)`                                      |
| `receiver.session.set_state(b"x")`                                    | `set_state(receiver.session, "x")`                                     |
| `with client: ...`                                                    | `ServiceBusClient(...) do client ... end` or `close(client)`           |
| `event.sequence_number`, `msg.lock_token`                             | the same                                                               |
| `TransportType.AmqpOverWebsocket`                                     | `TransportType.AmqpOverWebsocket`                                      |
| `container.upload_blob("a.txt", data, overwrite=True)`                | `upload_blob(container, "a.txt", data; overwrite=true)`                |
| `blob.download_blob().readall()`                                      | `readall(download_blob(blob))`                                         |
| `for b in container.list_blobs(): ...`                                | `for b in list_blobs(container) ... end`                               |

## Not implemented (yet)

Service Bus and Event Hubs **administration** clients (creating queues,
topics, hubs), the Event Hubs buffered producer and idempotent publishing, AMQP
transactions, Kafka, the interactive / device code / PowerShell / VS Code
credentials and Service Fabric managed identity. For Blob Storage:
client-side encryption, `query_blob`, CRC64 content validation and reads from
the secondary endpoint.

## Acknowledgements

The API design, names, defaults and documented behaviour follow the
[Azure SDK for Python](https://github.com/Azure/azure-sdk-for-python)
(MIT License, Copyright (c) Microsoft Corporation), so that code and
knowledge carry over between the two.
