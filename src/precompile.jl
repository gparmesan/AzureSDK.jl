# Precompile workload: run the real client code paths against the in-memory
# loopback peer so the first send/receive in user code does not pay for JIT
# compilation. No network access happens here.

using PrecompileTools: @setup_workload, @compile_workload

@setup_workload begin
    cred = AzureCore.AzureNamedKeyCredential("precompile", "key")
    @compile_workload begin
        try
            # codec
            m = AMQP.AMQPMessage(Vector{UInt8}("x"); application_properties=Dict{String,Any}("a" => 1),
                message_annotations=Dict{Any,Any}(:k => "v"))
            AMQP.decode_message(AMQP.encode_message(m))
            AzureCore.generate_sas_token("sb://precompile.localhost/q", "n", "k")

            # Service Bus
            ServiceBus.ServiceBusClient("precompile.localhost", cred; _scheme=:loopback, auth_timeout=10) do client
                sender = ServiceBus.get_queue_sender(client, "q")
                ServiceBus.send_messages(sender, ServiceBus.ServiceBusMessage("one"; application_properties=Dict("n" => 1)))
                ServiceBus.send_messages(sender, [ServiceBus.ServiceBusMessage("two"), ServiceBus.ServiceBusMessage("three")])
                receiver = ServiceBus.get_queue_receiver(client, "q"; max_wait_time=1)
                msgs = ServiceBus.receive_messages(receiver; max_message_count=3, max_wait_time=1)
                for msg in msgs
                    ServiceBus.body_as_str(msg)
                    msg.sequence_number
                    ServiceBus.complete_message(receiver, msg)
                end
            end

            # Event Hubs
            EventHubs.EventHubProducerClient("precompile.localhost", "hub", cred; _scheme=:loopback, auth_timeout=10) do producer
                batch = EventHubs.create_batch(producer; partition_id="0")
                EventHubs.add!(batch, EventHubs.EventData("event"; properties=Dict("i" => 1)))
                EventHubs.send_batch(producer, batch)
                EventHubs.get_eventhub_properties(producer)
            end
            consumer = EventHubs.EventHubConsumerClient("precompile.localhost", "hub", "\$Default", cred;
                _scheme=:loopback, auth_timeout=10)
            task = Threads.@spawn EventHubs.receive(consumer, (ctx, ev) -> close(consumer);
                partition_id="0", starting_position="-1", max_wait_time=0.2)
            timedwait(() -> istaskdone(task), 10.0)
            close(consumer)
            timedwait(() -> istaskdone(task), 5.0)
        catch err
            @debug "precompile workload failed" exception = err
        end
    end
end
