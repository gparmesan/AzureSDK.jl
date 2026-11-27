using AzureSDK
using AzureSDK.ServiceBus
using Test, Dates, HTTP, JSON, UUIDs

isdefined(@__MODULE__, :MockBroker) || include("mock_broker.jl")

bodies(msgs) = [body_as_str(m) for m in msgs]

@testset "Service Bus" begin
    b = MockBroker.Broker(lock_duration=5.0)
    for q in ("q", "q2", "q3", "q4", "topic", "topic/Subscriptions/sub", "reject-q")
        MockBroker.entity!(b, q)
    end
    MockBroker.entity!(b, "sessions"; requires_session=true)
    MockBroker.start!(b)
    cs = MockBroker.connection_string(b)
    client = from_connection_string(ServiceBusClient, cs; retry_total=2, retry_backoff_factor=0.05)
    try
        @testset "messages" begin
            m = ServiceBusMessage("hi"; application_properties=Dict("n" => 1, "s" => "x"), session_id="sid",
                message_id="mid", time_to_live=Minute(1), content_type="text/plain", correlation_id="cid",
                subject="subj", to="to", reply_to="rt", reply_to_session_id="rts",
                scheduled_enqueue_time_utc=DateTime(2030))
            raw = ServiceBus.to_amqp_message(m)
            @test raw.properties.group_id == "sid" && raw.properties.message_id == "mid"
            @test raw.header.ttl == 60_000
            @test raw.message_annotations[Symbol("x-opt-scheduled-enqueue-time")] == DateTime(2030)
            @test_throws ArgumentError ServiceBusMessage("x"; session_id="a", partition_key="b")
            @test_throws ArgumentError ServiceBusMessage("x"; message_id="m"^200)
            @test_throws ArgumentError ServiceBusMessage(42)
            @test ServiceBus.guid_from_bytes_le(UInt8[0x33, 0x22, 0x11, 0x00, 0x55, 0x44, 0x77, 0x66, 0x88, 0x99,
                0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff]) == UUID("00112233-4455-6677-8899-aabbccddeeff")
        end

        @testset "send, receive and settle" begin
            s = get_queue_sender(client, "q")
            r = get_queue_receiver(client, "q"; max_wait_time=2)
            send_messages(s, ServiceBusMessage("one"; application_properties=Dict("k" => "v"), message_id="m1"))
            send_messages(s, [ServiceBusMessage("m$i") for i in 2:5])
            batch = create_message_batch(s)
            @test max_size_in_bytes(batch) == 256 * 1024
            for i in 6:8
                add_message!(batch, ServiceBusMessage("m$i"))
            end
            @test length(batch) == 3 && size_in_bytes(batch) > 0
            send_messages(s, batch)
            msgs = receive_messages(r; max_message_count=20)
            @test bodies(msgs) == ["one", "m2", "m3", "m4", "m5", "m6", "m7", "m8"]
            m1 = msgs[1]
            @test m1.message_id == "m1" && m1.application_properties["k"] == "v"
            @test m1.sequence_number isa Int64 && m1.lock_token isa UUID && m1.locked_until_utc > now(UTC)
            @test m1.delivery_count == 0 && m1.enqueued_time_utc isa DateTime
            complete_message(r, m1)
            @test_throws MessageAlreadySettled complete_message(r, m1)
            abandon_message(r, msgs[2])
            defer_message(r, msgs[3])
            dead_letter_message(r, msgs[4]; reason="bad input", error_description="details")
            old = msgs[5].locked_until_utc
            sleep(0.01)
            @test renew_message_lock(r, msgs[5]) >= old
            foreach(m -> complete_message(r, m), msgs[5:end])
            again = receive_messages(r; max_message_count=10)
            @test bodies(again) == ["m2"] && again[1].delivery_count == 1
            complete_message(r, again[1])
            deferred = receive_deferred_messages(r, [msgs[3].sequence_number])
            @test bodies(deferred) == ["m3"]
            complete_message(r, deferred[1])
            dlq = get_queue_receiver(client, "q"; sub_queue=ServiceBusSubQueue.DEAD_LETTER, max_wait_time=2)
            dm = receive_messages(dlq)
            @test bodies(dm) == ["m4"] && dm[1].dead_letter_reason == "bad input" &&
                  dm[1].dead_letter_error_description == "details" && dm[1].dead_letter_source == "q"
            complete_message(dlq, dm[1])
            @test isempty(receive_messages(r; max_wait_time=0.5))
            # a message from another receiver cannot be settled here
            send_messages(s, ServiceBusMessage("x"))
            x = receive_messages(r)[1]
            @test_throws ArgumentError complete_message(dlq, x)
            complete_message(r, x)
        end

        @testset "settlement after the link was lost uses the management link" begin
            s = get_queue_sender(client, "q2")
            r = get_queue_receiver(client, "q2"; max_wait_time=2)
            send_messages(s, [ServiceBusMessage("a"), ServiceBusMessage("b")])
            msgs = receive_messages(r; max_message_count=2)
            @test length(msgs) == 2
            # drop our receiving link on the client side; the locks stay valid
            AzureSDK.AMQP.detach(r.link[1])
            complete_message(r, msgs[1])
            complete_message(r, msgs[2])
            @test isempty(b.entities["q2"].messages)
        end

        @testset "expired lock" begin
            s = get_queue_sender(client, "q3")
            r = get_queue_receiver(client, "q3"; max_wait_time=2)
            send_messages(s, ServiceBusMessage("slow"))
            m = receive_messages(r)[1]
            b.entities["q3"].messages[1].locked_until = now(UTC) - Second(1)
            @test_throws MessageLockLostError complete_message(r, m)
        end

        @testset "peek, schedule and cancel" begin
            s = get_queue_sender(client, "q4")
            r = get_queue_receiver(client, "q4"; max_wait_time=1)
            send_messages(s, [ServiceBusMessage("p$i") for i in 1:3])
            peeked = peek_messages(r; max_message_count=10)
            @test bodies(peeked) == ["p1", "p2", "p3"]
            @test_throws ArgumentError complete_message(r, peeked[1])
            @test isempty(peek_messages(r; max_message_count=10))        # continues after the last peeked
            @test length(peek_messages(r; max_message_count=10, sequence_number=1)) == 3
            seqs = schedule_messages(s, [ServiceBusMessage("later")], now(UTC) + Minute(10))
            @test length(seqs) == 1
            @test "later" in bodies(peek_messages(r; max_message_count=10, sequence_number=1))
            cancel_scheduled_messages(s, seqs)
            @test !("later" in bodies(peek_messages(r; max_message_count=10, sequence_number=1)))
            # scheduled in the past -> immediately available
            schedule_messages(s, ServiceBusMessage("now"), now(UTC) - Second(1))
            got = receive_messages(r; max_message_count=10)
            @test "now" in bodies(got)
            foreach(m -> complete_message(r, m), got)
        end

        @testset "receive and delete, prefetch and iteration" begin
            s = get_queue_sender(client, "q4")
            send_messages(s, [ServiceBusMessage("d$i") for i in 1:5])
            rd = get_queue_receiver(client, "q4"; receive_mode=ServiceBusReceiveMode.RECEIVE_AND_DELETE,
                max_wait_time=1, prefetch_count=10)
            got = collect(rd)                 # iteration ends after max_wait_time without messages
            @test bodies(got) == ["d$i" for i in 1:5]
            @test_throws ArgumentError complete_message(rd, got[1])
            @test isempty(b.entities["q4"].messages)
        end

        @testset "topics and subscriptions" begin
            s = get_topic_sender(client, "topic")
            send_messages(s, ServiceBusMessage("event"))
            # the mock routes by address; publish directly to the subscription entity for the round trip
            MockBroker.enqueue!(b, b.entities["topic/Subscriptions/sub"], AzureSDK.AMQP.AMQPMessage(Vector{UInt8}("event")))
            r = get_subscription_receiver(client, "topic", "sub"; max_wait_time=2)
            m = receive_messages(r)
            @test bodies(m) == ["event"]
            complete_message(r, m[1])
            @test_throws ArgumentError get_subscription_receiver(client, "topic", "bad/sub")
        end

        @testset "sessions" begin
            s = get_queue_sender(client, "sessions")
            send_messages(s, [ServiceBusMessage("a$i"; session_id="A") for i in 1:3])
            send_messages(s, ServiceBusMessage("b1"; session_id="B"))
            ra = get_queue_receiver(client, "sessions"; session_id=NEXT_AVAILABLE_SESSION, max_wait_time=2)
            got = receive_messages(ra; max_message_count=10)
            @test ra.session.session_id == "A"
            @test bodies(got) == ["a1", "a2", "a3"] && all(m -> m.session_id == "A", got)
            @test ra.session.locked_until_utc isa DateTime
            @test get_state(ra.session) === nothing
            set_state(ra.session, "progress=3")
            @test String(get_state(ra.session)) == "progress=3"
            @test renew_lock(ra.session) isa DateTime
            foreach(m -> complete_message(ra, m), got)
            # a locked session cannot be taken by a second receiver
            rb = get_queue_receiver(client, "sessions"; session_id="A", max_wait_time=1, )
            @test_throws SessionCannotBeLockedError receive_messages(rb)
            rnext = get_queue_receiver(client, "sessions"; session_id=NEXT_AVAILABLE_SESSION, max_wait_time=1)
            gb = receive_messages(rnext)
            @test rnext.session.session_id == "B" && bodies(gb) == ["b1"]
            # every session is locked now: a next-available receiver times out without retries
            t = @elapsed @test_throws ServiceBus.OperationTimeoutError receive_messages(
                get_queue_receiver(client, "sessions"; session_id=NEXT_AVAILABLE_SESSION, max_wait_time=1))
            @test t < 5
            complete_message(rnext, gb[1])
            close(ra)
            close(rb)
            close(rnext)
            # session lost -> the receiver does not silently switch sessions
            send_messages(s, ServiceBusMessage("c1"; session_id="C"))
            rc = get_queue_receiver(client, "sessions"; session_id="C", max_wait_time=1)
            receive_messages(rc)
            MockBroker.kill_connections!(b)
            sleep(0.2)
            @test_throws SessionLockLostError receive_messages(rc)
        end

        @testset "auto lock renewal" begin
            s = get_queue_sender(client, "q3")
            renewer = AutoLockRenewer(max_lock_renewal_duration=30)
            r = get_queue_receiver(client, "q3"; max_wait_time=2, auto_lock_renewer=renewer)
            send_messages(s, ServiceBusMessage("long job"))
            m = receive_messages(r)[1]
            first_lock = m.locked_until_utc
            sleep(3)     # lock duration is 5 s; renewal starts 10 s before expiry
            @test m.locked_until_utc > first_lock
            complete_message(r, m)
            close(renewer)
        end

        @testset "errors" begin
            @test_throws MessagingEntityNotFoundError send_messages(get_queue_sender(client, "missing-q"), ServiceBusMessage("x"))
            # a busy service is retried, then reported
            err = try
                send_messages(get_queue_sender(client, "reject-q"), ServiceBusMessage("x")); nothing
            catch e
                e
            end
            @test err isa ServiceBusServerBusyError && err.retriable
            big = ServiceBusMessage(rand(UInt8, 300_000))
            @test_throws MessageSizeExceededError send_messages(get_queue_sender(client, "q"), big)
            @test_throws ArgumentError get_queue_sender(client, "bad name")
            b.reject_tokens = true
            try
                bad = from_connection_string(ServiceBusClient, cs; retry_total=0)
                @test_throws ServiceBusAuthenticationError send_messages(get_queue_sender(bad, "q"), ServiceBusMessage("x"))
                close(bad)
            finally
                b.reject_tokens = false
            end
        end

        @testset "raw AMQP messages (AmqpAnnotatedMessage)" begin
            s = get_queue_sender(client, "q")
            r = get_queue_receiver(client, "q"; max_wait_time=2)
            raw = AmqpAnnotatedMessage(body_type=:value, body=Dict{String,Any}("k" => 1),
                application_properties=Dict{String,Any}("raw" => true))
            send_messages(s, raw)
            m = receive_messages(r)
            @test m[1].body == Dict{Any,Any}("k" => 1) && m[1].application_properties["raw"] == true
            @test m[1].raw_amqp_message.body_type === :value
            complete_message(r, m[1])
        end

        @testset "reconnect after connection loss" begin
            s = get_queue_sender(client, "q")
            r = get_queue_receiver(client, "q"; max_wait_time=2)
            send_messages(s, ServiceBusMessage("before"))
            MockBroker.kill_connections!(b)
            send_messages(s, ServiceBusMessage("after"))
            got = receive_messages(r; max_message_count=5)
            @test Set(bodies(got)) == Set(["before", "after"])
            foreach(m -> complete_message(r, m), got)
        end

        @testset "slow drain replies" begin
            # The broker answers drain requests late (seen on the Service Bus emulator).
            # Only the receiver's connection stalls; the sender uses `client`.
            MockBroker.entity!(b, "slow")
            MockBroker.entity!(b, "slow-rd")
            rc = from_connection_string(ServiceBusClient, cs)
            s = get_queue_sender(client, "slow")
            srd = get_queue_sender(client, "slow-rd")
            try
                # peek-lock: the call keeps to its time budget; late messages are not lost
                r = get_queue_receiver(rc, "slow"; max_wait_time=0.5)
                send_messages(s, ServiceBusMessage("warm-up"))
                complete_message(r, only(receive_messages(r)))
                b.drain_stall = 2.0
                t = @elapsed got = receive_messages(r; max_message_count=10)
                b.drain_stall = 0.0
                @test isempty(got)
                @test t < 1.5
                send_messages(s, [ServiceBusMessage("late$i") for i in 1:3])   # the broker still has credit
                got = ServiceBusReceivedMessage[]
                for _ in 1:20
                    append!(got, receive_messages(r; max_message_count=10))
                    length(got) >= 3 && break
                end
                @test sort(bodies(got)) == ["late1", "late2", "late3"]
                foreach(m -> complete_message(r, m), got)

                # peek-lock: messages that arrived but were never handed out are released on close
                b.drain_stall = 2.0
                @test isempty(receive_messages(r; max_message_count=10))
                b.drain_stall = 0.0
                send_messages(s, [ServiceBusMessage("held$i") for i in 1:3])
                sleep(0.3)
                close(r)
                r2 = get_queue_receiver(client, "slow"; max_wait_time=3)
                got = receive_messages(r2; max_message_count=10)   # before their 5 s locks would expire
                @test sort(bodies(got)) == ["held1", "held2", "held3"]
                foreach(m -> complete_message(r2, m), got)
                close(r2)

                # receive-and-delete: no message may arrive after the call returned, since
                # it would be lost when the receiver closes (the drain takes longer than 10 s)
                rd = get_queue_receiver(rc, "slow-rd"; receive_mode=ServiceBusReceiveMode.RECEIVE_AND_DELETE,
                    max_wait_time=0.5)
                send_messages(srd, ServiceBusMessage("warm-up"))
                @test bodies(receive_messages(rd)) == ["warm-up"]
                b.drain_stall = 12.0
                @test isempty(receive_messages(rd; max_message_count=10))
                b.drain_stall = 0.0
                send_messages(srd, [ServiceBusMessage("rd$i") for i in 1:3])
                sleep(0.3)
                close(rd)
                @test length(b.entities["slow-rd"].messages) == 3
            finally
                b.drain_stall = 0.0
                close(rc)
            end
        end

        @testset "close" begin
            c2 = from_connection_string(ServiceBusClient, cs)
            s = get_queue_sender(c2, "q")
            close(c2)
            @test_throws ServiceBusClientClosedError send_messages(s, ServiceBusMessage("x"))
            @test_throws ServiceBusClientClosedError get_queue_sender(c2, "q")
            # do-block forms
            from_connection_string(ServiceBusClient, cs) do c
                get_queue_sender(c, "q") do snd
                    send_messages(snd, ServiceBusMessage("do-block"))
                end
            end
            m = get_queue_receiver(client, "q"; max_wait_time=2) do rr
                x = receive_messages(rr)
                complete_message(rr, x[1])
                x
            end
            @test bodies(m) == ["do-block"]
        end
    finally
        close(client)
        MockBroker.stop!(b)
    end
end

# ---------------------------------------------------------------------------
# REST transport against a local HTTP stand-in for the Service Bus REST API
# ---------------------------------------------------------------------------

@testset "Service Bus over HTTP" begin
    queue = Vector{Any}()
    locks = Dict{String,Any}()
    auths = String[]
    seq = Ref(0)
    port = Ref(0)
    server = HTTP.serve!("127.0.0.1", 0; listenany=true) do req
        push!(auths, HTTP.header(req, "Authorization", ""))
        path = HTTP.URI(req.target).path
        body = req.body isa HTTP.EmptyBody ? UInt8[] : Vector{UInt8}(copy(req.body))
        if req.method == "POST" && path == "/q/messages"
            ct = HTTP.header(req, "Content-Type", "")
            if ct == "application/vnd.microsoft.servicebus.json"
                for item in JSON.parse(String(body))
                    seq[] += 1
                    push!(queue, (seq[], Vector{UInt8}(item["Body"]), item["BrokerProperties"], item["UserProperties"]))
                end
            else
                seq[] += 1
                bp = JSON.parse(HTTP.header(req, "BrokerProperties", "{}"))
                up = Dict(k => JSON.parse(v) for (k, v) in req.headers if lowercase(k) == "prop")
                push!(queue, (seq[], body, bp, up))
            end
            return HTTP.Response(201)
        elseif path == "/q/messages/head"
            isempty(queue) && return HTTP.Response(204)
            item = popfirst!(queue)
            bp = merge(Dict{String,Any}(item[3]), Dict("SequenceNumber" => item[1], "DeliveryCount" => 1,
                "EnqueuedTimeUtc" => "Sun, 06 Nov 2029 08:49:37 GMT"))
            headers = ["Content-Type" => "text/plain"]
            for (k, v) in item[4]
                push!(headers, k => JSON.json(v))
            end
            if req.method == "POST"
                tok = string(uuid4())
                bp["LockToken"] = tok
                bp["LockedUntilUtc"] = "Sun, 06 Nov 2039 08:49:37 GMT"
                locks[tok] = item
                push!(headers, "Location" => "http://localhost:$(port[])/q/messages/$(item[1])/$tok")
            end
            push!(headers, "BrokerProperties" => JSON.json(bp))
            return HTTP.Response(req.method == "POST" ? 201 : 200, headers, item[2])
        elseif startswith(path, "/q/messages/")
            tok = split(path, '/')[end]
            haskey(locks, tok) || return HTTP.Response(410)
            if req.method == "DELETE"
                delete!(locks, tok)
            elseif req.method == "PUT"
                pushfirst!(queue, pop!(locks, tok))
            elseif req.method == "POST"
                return HTTP.Response(200, ["BrokerProperties" => JSON.json(Dict("LockedUntilUtc" => "Sun, 06 Nov 2039 09:00:00 GMT"))])
            end
            return HTTP.Response(200)
        end
        return HTTP.Response(404)
    end
    port[] = HTTP.port(server)
    try
        cs = "Endpoint=sb://localhost:$(port[]);SharedAccessKeyName=key;SharedAccessKey=secret;UseDevelopmentEmulator=true"
        client = from_connection_string(ServiceBusClient, cs; transport_type=TransportType.Http)
        s = get_queue_sender(client, "q")
        send_messages(s, ServiceBusMessage("single"; message_id="m1", application_properties=Dict("prop" => 5)))
        send_messages(s, [ServiceBusMessage("b1"), ServiceBusMessage("b2"; subject="lbl")])
        @test all(a -> startswith(a, "SharedAccessSignature sr="), auths)
        @test length(queue) == 3
        r = get_queue_receiver(client, "q"; max_wait_time=2)
        m = receive_messages(r)
        # HTTP header names are case-insensitive; HTTP.jl returns them in canonical form,
        # so received application properties are looked up case-insensitively
        @test bodies(m) == ["single"] && m[1].message_id == "m1" && m[1].application_properties["prop"] == 5
        @test m[1].application_properties["Prop"] == 5 && m[1].application_properties isa CaseInsensitiveDict
        @test m[1].sequence_number == 1 && m[1].lock_token isa UUID
        @test renew_message_lock(r, m[1]) == DateTime(2039, 11, 6, 9)
        complete_message(r, m[1])
        m2 = receive_messages(r)
        abandon_message(r, m2[1])
        m3 = receive_messages(r; max_message_count=5)
        @test bodies(m3) == ["b1", "b2"] && m3[2].subject == "lbl"
        foreach(x -> complete_message(r, x), m3)
        send_messages(s, ServiceBusMessage("dl"))
        dl = receive_messages(r)
        @test_throws ArgumentError dead_letter_message(r, dl[1])
        @test_throws ArgumentError defer_message(r, dl[1])
        complete_message(r, dl[1])
        rd = get_queue_receiver(client, "q"; max_wait_time=1, receive_mode=ServiceBusReceiveMode.RECEIVE_AND_DELETE)
        send_messages(s, ServiceBusMessage("rd"))
        @test bodies(receive_messages(rd)) == ["rd"]
        @test isempty(receive_messages(rd; max_wait_time=0.5))
        @test_throws ArgumentError peek_messages(r)
        # lock URLs must match the namespace endpoint's scheme, host and port
        @test ServiceBus._check_location(r, "http://localhost:$(port[])/q/messages/1/x") === nothing
        for bad in ("https://localhost:$(port[])/q/messages/1/x", "http://localhost:1/q/messages/1/x",
            "http://evil.example:$(port[])/q/messages/1/x", "http://localhost/q/messages/1/x")
            @test_throws ServiceBusServiceError ServiceBus._check_location(r, bad)
        end
        close(client)
        prod = ServiceBusClient("ns.servicebus.windows.net", AzureNamedKeyCredential("n", "k");
            transport_type=TransportType.Http)
        pr = get_queue_receiver(prod, "q")
        @test ServiceBus._check_location(pr, "https://ns.servicebus.windows.net/q/messages/1/x") === nothing
        @test ServiceBus._check_location(pr, "https://NS.servicebus.windows.net:443/q/messages/1/x") === nothing
        @test_throws ServiceBusServiceError ServiceBus._check_location(pr, "http://ns.servicebus.windows.net/q/messages/1/x")
        close(prod)
    finally
        HTTP.forceclose(server)
    end
end
