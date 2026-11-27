# Send and receive events.
#
# Uses EVENTHUB_CONNECTION_STRING and EVENTHUB_NAME, or (without them) the local
# Event Hubs emulator started with test/emulator/docker-compose.yml.
#
#   julia --project examples/eventhubs_quickstart.jl

using AzureSDK.EventHubs

const CONN = get(
    ENV, "EVENTHUB_CONNECTION_STRING",
    "Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;"
)
const HUB = get(ENV, "EVENTHUB_NAME", "eh1")

from_connection_string(EventHubProducerClient, CONN; eventhub_name = HUB) do producer
    println("partitions: ", get_partition_ids(producer))
    batch = create_batch(producer; partition_key = "sensor-1")
    for i in 1:5
        add!(batch, EventData("""{"reading": $i}"""; properties = Dict("unit" => "C")))
    end
    send_batch(producer, batch)
    println("sent ", length(batch), " events (", size_in_bytes(batch), " bytes)")
end

consumer = from_connection_string(EventHubConsumerClient, CONN; eventhub_name = HUB, consumer_group = "\$default")
received = Ref(0)
# stop after 10 s without new events
receive(consumer; starting_position = "-1", max_wait_time = 10) do ctx, event
    if event === nothing
        close(consumer)
        return
    end
    received[] += 1
    println("partition ", ctx.partition_id, " seq ", event.sequence_number, ": ", body_as_str(event))
end
println("received ", received[], " events")
