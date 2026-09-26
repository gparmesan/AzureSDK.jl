# Service Bus

```julia
using AzureSDK.ServiceBus, AzureSDK.Identity

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

See the [Service Bus API reference](@ref "Service Bus API") for all functions.
