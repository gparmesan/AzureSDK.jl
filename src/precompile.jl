# Precompile workload: run the real client code paths against the in-memory
# loopback peer so the first send/receive in user code does not pay for JIT
# compilation. No network access happens here.

using PrecompileTools: @setup_workload, @compile_workload
import Dates
import HTTP

@setup_workload begin
    cred = AzureCore.AzureNamedKeyCredential("precompile", "key")
    @compile_workload begin
        try
            # codec
            m = AMQP.AMQPMessage(
                Vector{UInt8}("x"); application_properties = Dict{String, Any}("a" => 1),
                message_annotations = Dict{Any, Any}(:k => "v")
            )
            AMQP.decode_message(AMQP.encode_message(m))
            AzureCore.generate_sas_token("sb://precompile.localhost/q", "n", "k")

            # Service Bus
            ServiceBus.ServiceBusClient("precompile.localhost", cred; _scheme = :loopback, auth_timeout = 10) do client
                sender = ServiceBus.get_queue_sender(client, "q")
                ServiceBus.send_messages(sender, ServiceBus.ServiceBusMessage("one"; application_properties = Dict("n" => 1)))
                ServiceBus.send_messages(sender, [ServiceBus.ServiceBusMessage("two"), ServiceBus.ServiceBusMessage("three")])
                receiver = ServiceBus.get_queue_receiver(client, "q"; max_wait_time = 1)
                msgs = ServiceBus.receive_messages(receiver; max_message_count = 3, max_wait_time = 1)
                for msg in msgs
                    ServiceBus.body_as_str(msg)
                    msg.sequence_number
                    ServiceBus.complete_message(receiver, msg)
                end
            end

            # Event Hubs
            EventHubs.EventHubProducerClient("precompile.localhost", "hub", cred; _scheme = :loopback, auth_timeout = 10) do producer
                batch = EventHubs.create_batch(producer; partition_id = "0")
                EventHubs.add!(batch, EventHubs.EventData("event"; properties = Dict("i" => 1)))
                EventHubs.send_batch(producer, batch)
                EventHubs.get_eventhub_properties(producer)
            end
            consumer = EventHubs.EventHubConsumerClient(
                "precompile.localhost", "hub", "\$Default", cred;
                _scheme = :loopback, auth_timeout = 10
            )
            task = Threads.@spawn EventHubs.receive(
                consumer, (ctx, ev) -> close(consumer);
                partition_id = "0", starting_position = "-1", max_wait_time = 0.2
            )
            timedwait(() -> istaskdone(task), 10.0)
            close(consumer)
            timedwait(() -> istaskdone(task), 5.0)
        catch err
            @debug "precompile workload failed" exception = err
        end
        try
            # Blob Storage: clients, signing, SAS and response parsing (no requests are sent)
            B = Storage.Blob
            svc = B.from_connection_string(B.BlobServiceClient, "UseDevelopmentStorage=true")
            bc = B.get_blob_client(B.get_container_client(svc, "c"), "dir/b.txt")
            bc.url
            key = Storage.DEV_ACCOUNT_KEY
            Storage.shared_key_signature(
                "devstoreaccount1", key, "GET", bc.url * "?comp=list",
                ["x-ms-date" => "Mon, 05 Jan 2026 10:11:12 GMT", "x-ms-version" => Storage.DEFAULT_API_VERSION]
            )
            B.generate_blob_sas("a", "c", "b"; account_key = key, permission = "r", expiry = Dates.DateTime(2030))
            B.generate_account_sas("a", key, "sco", "rl", Dates.DateTime(2030))
            xml = "\ufeff<?xml version=\"1.0\" encoding=\"utf-8\"?><EnumerationResults><Blobs><Blob><Name>b</Name>" *
                "<Properties><Last-Modified>Mon, 05 Jan 2026 10:11:12 GMT</Last-Modified><Etag>0x1</Etag>" *
                "<Content-Length>1</Content-Length><BlobType>BlockBlob</BlobType></Properties><Metadata><k>v</k>" *
                "</Metadata></Blob><BlobPrefix><Name>d/</Name></BlobPrefix></Blobs><NextMarker/></EnumerationResults>"
            x = Storage.xml_parse(xml)
            [B._blob_properties_from_xml(e, "c") for e in Storage.xfindall(Storage.xfind(x, "Blobs"), "Blob")]
            resp = HTTP.Response(
                200, [
                    "ETag" => "\"0x1\"", "Content-Length" => "1", "x-ms-meta-k" => "v",
                    "Last-Modified" => "Mon, 05 Jan 2026 10:11:12 GMT", "x-ms-blob-type" => "BlockBlob",
                ]
            )
            B._blob_properties_from_headers(resp, bc)
            Storage.response_dict(resp)
            Storage.storage_error(HTTP.Response(404, ["x-ms-error-code" => "BlobNotFound"]))
        catch err
            @debug "precompile workload (storage) failed" exception = err
        end
    end
end
