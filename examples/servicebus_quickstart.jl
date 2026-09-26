# Queue basics: send, receive, settle, dead-letter.
#
#   SERVICEBUS_NAMESPACE=myns.servicebus.windows.net SERVICEBUS_QUEUE=orders julia --project examples/servicebus_quickstart.jl
#
# Authenticates with DefaultAzureCredential (az login, managed identity,
# workload identity, environment variables ...). Alternatively set
# SERVICEBUS_CONNECTION_STRING.

using AzureSDK.ServiceBus, AzureSDK.Identity

queue = get(ENV, "SERVICEBUS_QUEUE", "orders")
client = if haskey(ENV, "SERVICEBUS_CONNECTION_STRING")
    from_connection_string(ServiceBusClient, ENV["SERVICEBUS_CONNECTION_STRING"])
else
    ServiceBusClient(ENV["SERVICEBUS_NAMESPACE"], DefaultAzureCredential())
end

try
    sender = get_queue_sender(client, queue)
    send_messages(sender, [ServiceBusMessage("order $i"; application_properties=Dict("n" => i)) for i in 1:5])
    send_messages(sender, ServiceBusMessage("poison"; subject="bad"))

    receiver = get_queue_receiver(client, queue; max_wait_time=5)
    for msg in receiver
        if msg.subject == "bad"
            dead_letter_message(receiver, msg; reason="invalid", error_description="example")
        else
            println("processing ", body_as_str(msg), " (delivery ", msg.delivery_count, ")")
            complete_message(receiver, msg)
        end
    end

    dlq = get_queue_receiver(client, queue; sub_queue=ServiceBusSubQueue.DEAD_LETTER, max_wait_time=5)
    for msg in receive_messages(dlq; max_message_count=10)
        println("dead-lettered: ", body_as_str(msg), " reason=", msg.dead_letter_reason)
        complete_message(dlq, msg)
    end
finally
    close(client)
end
