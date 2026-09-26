using AzureSDK
using AzureSDK.EventHubs
using Test, Dates, HTTP, JSON

const A = AzureSDK.AMQP

@testset "Event Hubs (unit)" begin
    @testset "EventData" begin
        e = EventData("hello"; properties=Dict("a" => 1), content_type="text/plain", message_id="m", correlation_id="c")
        @test body_as_str(e) == "hello"
        @test e.sequence_number === nothing && e.offset === nothing
        m = EventHubs.to_amqp_message(e; partition_key="pk")
        @test m.message_annotations[Symbol("x-opt-partition-key")] == "pk"
        @test m.properties.content_type == Symbol("text/plain") && m.properties.message_id == "m"
        # received event
        raw = A.decode_message(A.encode_message(A.AMQPMessage(message_annotations=Dict{Any,Any}(
                Symbol("x-opt-sequence-number") => Int64(5), Symbol("x-opt-offset") => "100",
                Symbol("x-opt-enqueued-time") => DateTime(2024), Symbol("x-opt-partition-key") => "k"),
            application_properties=Dict{String,Any}("x" => 1), body_type=:data, body=[Vector{UInt8}("{\"v\":1}")])))
        r = EventData(raw)
        @test r.sequence_number == 5 && r.offset == "100" && r.enqueued_time == DateTime(2024) && r.partition_key == "k"
        @test body_as_json(r)["v"] == 1
        @test r.system_properties["x-opt-sequence-number"] == 5
        @test :sequence_number in propertynames(r)
        # re-sending a received event drops broker annotations
        resent = EventHubs.to_amqp_message(r)
        @test resent.message_annotations === nothing
        @test_throws ArgumentError EventData(42)
    end

    @testset "EventDataBatch" begin
        b = EventDataBatch(max_size_in_bytes=1000, partition_key="pk")
        n = 0
        while try_add!(b, EventData("x"^50))
            n += 1
        end
        @test n > 5 && length(b) == n && size_in_bytes(b) <= 1000
        @test_throws ArgumentError add!(b, EventData("x"^50))
        payload = EventHubs.batch_payload(b)
        @test length(payload) == size_in_bytes(b)
        env = A.decode_message(payload)
        @test env.message_annotations[Symbol("x-opt-partition-key")] == "pk"
        @test length(env.body) == n
        inner = A.decode_message(env.body[1])
        @test String(A.body_bytes(inner)) == "x"^50
        @test_throws ArgumentError EventDataBatch(partition_id="0", partition_key="k")
    end

    @testset "starting positions are validated" begin
        @test EventHubs.selector_expression("-1", false) == "amqp.annotation.x-opt-offset > '-1'"
        @test EventHubs.selector_expression("@latest", true) == "amqp.annotation.x-opt-offset > '@latest'"
        @test EventHubs.selector_expression("123", true) == "amqp.annotation.x-opt-offset >= '123'"
        @test EventHubs.selector_expression(42, false) == "amqp.annotation.x-opt-sequence-number > 42"
        @test EventHubs.selector_expression(DateTime(1970, 1, 1, 0, 0, 1), false) ==
              "amqp.annotation.x-opt-enqueued-time > 1000"
        # filter injection attempts are refused
        @test_throws ArgumentError EventHubs.selector_expression("1' OR 1=1 --", false)
        @test_throws ArgumentError EventHubs.selector_expression(1.5, false)
    end

    @testset "client construction" begin
        cs = "Endpoint=sb://ns.servicebus.windows.net/;SharedAccessKeyName=n;SharedAccessKey=k;EntityPath=hub"
        p = from_connection_string(EventHubProducerClient, cs)
        @test p.eventhub_name == "hub" && p.config.fully_qualified_namespace == "ns.servicebus.windows.net"
        @test !occursin("SharedAccessKey=k", sprint(show, p))
        close(p)
        @test_throws ClientClosedError create_batch(p)
        @test_throws ArgumentError from_connection_string(EventHubProducerClient, cs; eventhub_name="other")
        @test_throws ArgumentError EventHubConsumerClient("ns.servicebus.windows.net", "hub", "\$Default",
            AzureNamedKeyCredential("n", "k"); transport_type=TransportType.Http)
        @test_throws ArgumentError EventHubProducerClient("ns.servicebus.windows.net", "hub", AzureNamedKeyCredential("n", "k");
            http_proxy=(proxy_hostname="p", proxy_port=3128))
        ws = EventHubProducerClient("ns.servicebus.windows.net", "hub", AzureNamedKeyCredential("n", "k");
            transport_type=TransportType.AmqpOverWebsocket, http_proxy=(proxy_hostname="p", proxy_port=3128))
        @test ws.conn.transport_config.scheme === :wss
        close(ws)
    end

    @testset "InMemoryCheckpointStore and load balancing" begin
        store = InMemoryCheckpointStore()
        fq, eh, cg = "ns", "hub", "cg"
        o(pid, owner, etag=nothing) = Dict{String,Any}("fully_qualified_namespace" => fq, "eventhub_name" => eh,
            "consumer_group" => cg, "partition_id" => pid, "owner_id" => owner, "etag" => etag)
        c = claim_ownership(store, [o("0", "a")])
        @test length(c) == 1
        @test isempty(claim_ownership(store, [o("0", "b")]))              # stale etag
        @test length(claim_ownership(store, [o("0", "b", c[1]["etag"])])) == 1
        update_checkpoint(store, Dict{String,Any}("fully_qualified_namespace" => fq, "eventhub_name" => eh,
            "consumer_group" => cg, "partition_id" => "0", "offset" => "10", "sequence_number" => 3))
        @test list_checkpoints(store, fq, eh, cg)[1]["sequence_number"] == 3

        # three consumers balancing eight partitions end up with 3/3/2
        store = InMemoryCheckpointStore()
        pids = string.(0:7)
        consumers = [EventHubConsumerClient("ns", "hub", "cg", AzureNamedKeyCredential("n", "k"); checkpoint_store=store)
                     for _ in 1:3]
        owned = Dict{String,Set{String}}()
        for round in 1:6, c in consumers
            owned[c.owner_id] = EventHubs._balance!(c, pids)
        end
        counts = sort([length(owned[c.owner_id]) for c in consumers])
        @test counts == [2, 3, 3]
        @test length(union(values(owned)...)) == 8
        foreach(close, consumers)
    end
end

@testset "Event Hubs over HTTP" begin
    requests = Any[]
    server = HTTP.serve!("127.0.0.1", 0; listenany=true) do req
        push!(requests, (target=req.target, headers=Dict(lowercase(k) => v for (k, v) in req.headers),
            body=String(copy(req.body))))
        return HTTP.Response(201)
    end
    try
        port = HTTP.port(server)
        cs = "Endpoint=sb://localhost:$port;SharedAccessKeyName=key;SharedAccessKey=secret;UseDevelopmentEmulator=true"
        p = from_connection_string(EventHubProducerClient, cs; eventhub_name="hub", transport_type=TransportType.Http)
        send_batch(p, [EventData("a"; properties=Dict("x" => 1)), EventData("b")]; partition_key="pk")
        r = requests[end]
        @test startswith(r.target, "/hub/messages?")
        @test r.headers["content-type"] == "application/vnd.microsoft.servicebus.json"
        items = JSON.parse(r.body)
        @test [i["Body"] for i in items] == ["a", "b"] && items[1]["UserProperties"]["x"] == 1
        @test items[1]["BrokerProperties"]["PartitionKey"] == "pk"
        @test startswith(r.headers["authorization"], "SharedAccessSignature sr=")
        send_event(p, AmqpAnnotatedMessage(Vector{UInt8}("raw")); partition_key="k2")
        @test requests[end].body == "raw" && occursin("k2", requests[end].headers["brokerproperties"])
        send_event(p, EventData(UInt8[0xff, 0xfe]); partition_id="3")
        @test startswith(requests[end].target, "/hub/partitions/3/messages?")
        @test_throws ArgumentError get_eventhub_properties(p)
        close(p)
    finally
        HTTP.forceclose(server)
    end
end

# ---------------------------------------------------------------------------
# Integration tests against the local Event Hubs emulator (and Azurite for the
# blob checkpoint store). Enable with AZURESDK_EVENTHUBS_EMULATOR=1; see
# test/emulator/docker-compose.yml.
# ---------------------------------------------------------------------------

const EMULATOR_CS = "Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;"

if get(ENV, "AZURESDK_EVENTHUBS_EMULATOR", "") == "1"
    @testset "Event Hubs (emulator)" begin
        producer = from_connection_string(EventHubProducerClient, EMULATOR_CS; eventhub_name="eh1")
        props = get_eventhub_properties(producer)
        @test props["eventhub_name"] == "eh1" && props["partition_ids"] == ["0", "1"]
        pp = get_partition_properties(producer, "1")
        @test pp["id"] == "1" && pp["last_enqueued_sequence_number"] isa Integer
        start_seq = pp["last_enqueued_sequence_number"]

        marker = string(rand(UInt32))
        batch = create_batch(producer; partition_id="1")
        for i in 1:10
            add!(batch, EventData("$marker-$i"; properties=Dict("i" => i)))
        end
        send_batch(producer, batch)
        send_event(producer, EventData("$marker-11"); partition_id="1")

        @testset "receive from a sequence number" begin
            consumer = from_connection_string(EventHubConsumerClient, EMULATOR_CS; eventhub_name="eh1",
                consumer_group="\$default")
            got = EventData[]
            ctxs = Any[]
            task = Threads.@spawn receive(consumer, (ctx, ev) -> begin
                    ev === nothing && return
                    push!(got, ev)
                    push!(ctxs, ctx)
                    length(got) >= 11 && close(consumer)
                end; partition_id="1", starting_position=start_seq, starting_position_inclusive=false,
                max_wait_time=5, on_partition_initialize=ctx -> push!(ctxs, :init))
            @test timedwait(() -> istaskdone(task), 60.0) === :ok
            @test [body_as_str(e) for e in got] == ["$marker-$i" for i in 1:11]
            @test got[1].properties["i"] == 1 && got[1].sequence_number == start_seq + 1
            @test ctxs[1] === :init && ctxs[2].partition_id == "1"
        end

        @testset "receive_batch with checkpoints" begin
            store = InMemoryCheckpointStore()
            consumer = from_connection_string(EventHubConsumerClient, EMULATOR_CS; eventhub_name="eh1",
                consumer_group="cg1", checkpoint_store=store, load_balancing_interval=1)
            seen = String[]
            task = Threads.@spawn receive_batch(consumer, (ctx, evs) -> begin
                    for e in evs
                        startswith(body_as_str(e), marker) && push!(seen, body_as_str(e))
                    end
                    isempty(evs) || update_checkpoint(ctx, evs[end])
                    length(seen) >= 11 && close(consumer)
                end; starting_position="-1", max_batch_size=4, max_wait_time=2)
            @test timedwait(() -> istaskdone(task), 90.0) === :ok
            @test sort(seen) == sort(["$marker-$i" for i in 1:11])
            cps = list_checkpoints(store, "localhost", "eh1", "cg1")
            @test any(cp -> cp["partition_id"] == "1", cps)
            own = list_ownership(store, "localhost", "eh1", "cg1")
            @test all(o -> o["owner_id"] == "", own)      # relinquished on close
        end

        @testset "partition keys and concurrency" begin
            p2 = from_connection_string(EventHubProducerClient, EMULATOR_CS; eventhub_name="eh2")
            @sync for i in 1:50
                Threads.@spawn send_event(p2, EventData("c$i"); partition_key="key-$(i % 3)")
            end
            close(p2)
        end

        close(producer)

        if get(ENV, "AZURESDK_AZURITE", "1") == "1"
            @testset "BlobCheckpointStore (Azurite)" begin
                store = from_connection_string(BlobCheckpointStore, "UseDevelopmentStorage=true", "ehcheckpoints")
                EventHubs._blob_request(store, "PUT", ""; query=["restype" => "container"])
                fq, eh, cg = "localhost", "eh1", "\$Default"
                o = Dict{String,Any}("fully_qualified_namespace" => fq, "eventhub_name" => eh, "consumer_group" => cg,
                    "partition_id" => "p$(rand(UInt32))", "owner_id" => "me", "etag" => nothing)
                c = claim_ownership(store, [o])
                @test length(c) == 1 && startswith(c[1]["etag"], "\"")
                @test isempty(claim_ownership(store, [o]))
                @test length(claim_ownership(store, [merge(c[1], Dict("owner_id" => "other"))])) == 1
                listed = [x for x in list_ownership(store, fq, eh, cg) if x["partition_id"] == o["partition_id"]]
                @test listed[1]["owner_id"] == "other"
                update_checkpoint(store, Dict{String,Any}("fully_qualified_namespace" => fq, "eventhub_name" => eh,
                    "consumer_group" => cg, "partition_id" => o["partition_id"], "offset" => "42", "sequence_number" => 7))
                cp = [x for x in list_checkpoints(store, fq, eh, cg) if x["partition_id"] == o["partition_id"]]
                @test cp[1]["offset"] == "42" && cp[1]["sequence_number"] == 7

                # two consumers share eh2's four partitions through the blob store
                c1 = from_connection_string(EventHubConsumerClient, EMULATOR_CS; eventhub_name="eh2",
                    consumer_group="\$default", checkpoint_store=store, load_balancing_interval=1)
                c2 = from_connection_string(EventHubConsumerClient, EMULATOR_CS; eventhub_name="eh2",
                    consumer_group="\$default", checkpoint_store=store, load_balancing_interval=1)
                t1 = Threads.@spawn receive(c1, (ctx, ev) -> nothing; max_wait_time=1)
                t2 = Threads.@spawn receive(c2, (ctx, ev) -> nothing; max_wait_time=1)
                ok = timedwait(() -> length(c1.pumps) == 2 && length(c2.pumps) == 2, 60.0)
                owned = union(Set(keys(c1.pumps)), Set(keys(c2.pumps)))
                t = @elapsed (close(c1); close(c2); wait(t1); wait(t2))
                @test ok === :ok
                @test owned == Set(["0", "1", "2", "3"])
                @test t < 15
            end
        end
    end
else
    @info "Skipping Event Hubs emulator tests (set AZURESDK_EVENTHUBS_EMULATOR=1 to run them)"
end
