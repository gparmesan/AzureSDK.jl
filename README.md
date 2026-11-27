# AzureSDK.jl

A pure-Julia client library for **Azure Event Hubs**, **Azure Service Bus**,
**Azure Blob Storage** and **Microsoft Entra ID authentication**. Its structure
and API follow the official
[Azure SDK for Python](https://github.com/Azure/azure-sdk-for-python).

- AMQP 1.0, AMQP over WebSockets and HTTP transports
- Shared access keys and signatures, or Entra ID tokens (workload identity,
  managed identity, service principals, Azure CLI, …)
- Automatic reconnection, retries and token refresh

> This is an independent community project. It is not an official Microsoft
> product and is not affiliated with or endorsed by Microsoft.

## Installation

```julia
using Pkg
Pkg.add(url="<repository url>")
```

Julia 1.10 or newer is required.

## Example

```julia
using AzureSDK.EventHubs, AzureSDK.ServiceBus, AzureSDK.Storage.Blob, AzureSDK.Identity

cred = DefaultAzureCredential()

EventHubProducerClient("myns.servicebus.windows.net", "myhub", cred) do producer
    batch = create_batch(producer)
    add!(batch, EventData("hello"))
    send_batch(producer, batch)
end

ServiceBusClient("myns.servicebus.windows.net", cred) do client
    send_messages(get_queue_sender(client, "orders"), ServiceBusMessage("order 1"))
    receiver = get_queue_receiver(client, "orders"; max_wait_time=5)
    for msg in receiver
        println(body_as_str(msg))
        complete_message(receiver, msg)
    end
end

container = ContainerClient("https://myaccount.blob.core.windows.net", "reports"; credential=cred)
upload_blob(container, "hello.txt", "Hello, world"; overwrite=true)
content_as_text(download_blob(container, "hello.txt"))
```

Python methods become Julia functions that take the client as their first
argument, with the same names and keyword arguments:
`producer.send_batch(batch)` becomes `send_batch(producer, batch)`.

## Documentation

The guide covers each service, authentication, transports, errors and
security, and includes the API reference:
[Authentication](docs/src/identity.md) ·
[Event Hubs](docs/src/eventhubs.md) ·
[Service Bus](docs/src/servicebus.md) ·
[Blob Storage](docs/src/blob.md) ·
[Transports and emulators](docs/src/transports.md) ·
[Errors, retries and security](docs/src/errors.md) ·
[Testing and contributing](docs/src/testing.md).
More examples are in [`examples/`](examples).

To report a security problem, see [SECURITY.md](SECURITY.md).

## Acknowledgements

The API design, names, defaults and documented behaviour follow the
[Azure SDK for Python](https://github.com/Azure/azure-sdk-for-python)
(MIT License, Copyright (c) Microsoft Corporation).

## License

MIT, see [LICENSE](LICENSE).
