# Load-balanced consumption with checkpoints in Azure Blob Storage. Start this
# script several times: the partitions are shared between the processes.
#
#   STORAGE_CONNECTION_STRING  (default: the local Azurite emulator)
#   EVENTHUB_CONNECTION_STRING, EVENTHUB_NAME (default: the local Event Hubs emulator)

using AzureSDK.EventHubs

conn = get(
    ENV, "EVENTHUB_CONNECTION_STRING",
    "Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;"
)
store = from_connection_string(
    BlobCheckpointStore, get(ENV, "STORAGE_CONNECTION_STRING", "UseDevelopmentStorage=true"),
    "eventhub-checkpoints"
)      # the container must exist

consumer = from_connection_string(
    EventHubConsumerClient, conn; eventhub_name = get(ENV, "EVENTHUB_NAME", "eh2"),
    consumer_group = "\$default", checkpoint_store = store
)

Base.exit_on_sigint(false)
try
    receive_batch(
        consumer; max_batch_size = 100, max_wait_time = 5,
        on_partition_initialize = ctx -> println("claimed partition ", ctx.partition_id),
        on_partition_close = (ctx, reason) -> println("released partition ", ctx.partition_id, " (", reason, ")"),
        on_error = (ctx, err) -> @warn("receive error", partition = ctx === nothing ? nothing : ctx.partition_id, err)
    ) do ctx, events
        isempty(events) && return
        println("partition ", ctx.partition_id, ": ", length(events), " events")
        update_checkpoint(ctx, events[end])
    end
catch err
    err isa InterruptException || rethrow()
finally
    close(consumer)
end
