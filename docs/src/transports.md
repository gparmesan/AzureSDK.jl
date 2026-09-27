# Transports and emulators

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
AMQP. Over HTTP, application properties travel as HTTP headers, whose names
lose their original case. Messages received over HTTP therefore return
`application_properties` as a `CaseInsensitiveDict`, so `props["myProp"]` works
whatever case the name arrives in.

## Performance

The AMQP engine pipelines deliveries (many sends in flight on one link),
coalesces outgoing frames into large socket writes, never holds a lock across
socket I/O and decodes directly from receive buffers. Batches
(`create_batch` / `create_message_batch`) are much more efficient than sending
messages one at a time. A precompile workload keeps the latency of the first
operation low.

## Local emulators

Connection strings with `UseDevelopmentEmulator=true` connect to the local
[Event Hubs](https://github.com/Azure/azure-event-hubs-emulator-installer) and
[Service Bus](https://github.com/Azure/azure-service-bus-emulator-installer)
emulators without TLS. The flag is refused for non-local hosts.

```julia
cs = "Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;"
producer = from_connection_string(EventHubProducerClient, cs; eventhub_name="eh1")
```

`from_connection_string(BlobServiceClient, "UseDevelopmentStorage=true")`
connects to [Azurite](https://github.com/Azure/Azurite). Plain `http://`
storage URLs are only accepted for local hosts.
