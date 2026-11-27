# Event Hubs

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

- **Load balancing across processes:** pass
  `checkpoint_store=BlobCheckpointStore(account_url, container; credential=...)`
  (or `from_connection_string(BlobCheckpointStore, conn, container)`). The blob
  layout is the same as in the other Azure SDKs, so Julia and Python consumers
  can share a consumer group.
- `receive_batch(consumer, on_event_batch; max_batch_size=300, max_wait_time=...)`
- `get_eventhub_properties`, `get_partition_ids`, `get_partition_properties`
- `owner_level`, `track_last_enqueued_event_properties`, `prefetch`,
  `load_balancing_strategy = LoadBalancingStrategy.GREEDY | BALANCED`

See the [Event Hubs API reference](@ref "Event Hubs API") for all functions.
