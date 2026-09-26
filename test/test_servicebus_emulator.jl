# Integration tests against the local Service Bus emulator
# (test/emulator/servicebus-compose.yml). Enable with AZURESDK_SERVICEBUS_EMULATOR=1.
using AzureSDK
using AzureSDK.ServiceBus
using Test, Dates, UUIDs

const SB_EMULATOR_CS = get(ENV, "AZURESDK_SERVICEBUS_CONNECTION_STRING",
    "Endpoint=sb://localhost:5673;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;")

sbodies(msgs) = [body_as_str(m) for m in msgs]

"Remove whatever earlier runs left in an entity."
function purge!(client, entity; subscription=nothing, sub_queue=nothing, session=false)
    while true
        r = if subscription === nothing
            get_queue_receiver(client, entity; receive_mode=ServiceBusReceiveMode.RECEIVE_AND_DELETE,
                max_wait_time=1, sub_queue=sub_queue, session_id=session ? NEXT_AVAILABLE_SESSION : nothing)
        else
            get_subscription_receiver(client, entity, subscription; receive_mode=ServiceBusReceiveMode.RECEIVE_AND_DELETE,
                max_wait_time=1, sub_queue=sub_queue)
        end
        got = try
            receive_messages(r; max_message_count=100)
        catch err
            err isa ServiceBus.OperationTimeoutError || rethrow()
            ServiceBusReceivedMessage[]          # no session with messages left
        finally
            close(r)
        end
        isempty(got) && return
    end
end

if get(ENV, "AZURESDK_SERVICEBUS_EMULATOR", "") == "1"
    @testset "Service Bus (emulator)" begin
        client = from_connection_string(ServiceBusClient, SB_EMULATOR_CS; retry_total=2, retry_backoff_factor=0.2)
        try
            purge!(client, "queue.1")
            purge!(client, "queue.1"; sub_queue=ServiceBusSubQueue.DEAD_LETTER)
            purge!(client, "queue.short-lock")
            purge!(client, "queue.sessions"; session=true)
            purge!(client, "topic.1"; subscription="subscription.1")
            purge!(client, "topic.1"; subscription="subscription.2")

            @testset "message properties round trip" begin
                s = get_queue_sender(client, "queue.1")
                r = get_queue_receiver(client, "queue.1"; max_wait_time=5)
                send_messages(s, ServiceBusMessage("props"; message_id="mid-1", correlation_id="cid", subject="subj",
                    content_type="application/json", to="dest", reply_to="rt", time_to_live=Minute(5),
                    application_properties=Dict("str" => "v", "int" => 42, "float" => 1.5, "bool" => true,
                        "time" => DateTime(2030, 1, 2, 3, 4, 5), "uuid" => UUID("12345678-1234-1234-1234-123456789abc"))))
                m = only(receive_messages(r))
                @test body_as_str(m) == "props"
                @test (m.message_id, m.correlation_id, m.subject, m.content_type, m.to, m.reply_to) ==
                      ("mid-1", "cid", "subj", "application/json", "dest", "rt")
                ap = m.application_properties
                @test ap["str"] == "v" && ap["int"] == 42 && ap["float"] == 1.5 && ap["bool"] === true
                @test ap["time"] == DateTime(2030, 1, 2, 3, 4, 5)
                @test ap["uuid"] == UUID("12345678-1234-1234-1234-123456789abc")
                @test m.time_to_live == 300.0 && m.expires_at_utc - m.enqueued_time_utc == Minute(5)
                @test m.sequence_number isa Int64 && m.lock_token isa UUID && m.delivery_count == 0
                @test m.locked_until_utc > now(UTC)
                @test m.state == ServiceBusMessageState.ACTIVE
                complete_message(r, m)
                binary = rand(UInt8, 1000)
                send_messages(s, ServiceBusMessage(binary))
                b = only(receive_messages(r))
                @test b.body == binary
                complete_message(r, b)
                close(r)
            end

            @testset "settlement" begin
                s = get_queue_sender(client, "queue.1")
                r = get_queue_receiver(client, "queue.1"; max_wait_time=5)
                send_messages(s, [ServiceBusMessage("m$i") for i in 1:5])
                msgs = ServiceBusReceivedMessage[]
                while length(msgs) < 5
                    got = receive_messages(r; max_message_count=5 - length(msgs))
                    isempty(got) && break
                    append!(msgs, got)
                end
                @test sbodies(msgs) == ["m$i" for i in 1:5]
                complete_message(r, msgs[1])
                @test_throws MessageAlreadySettled complete_message(r, msgs[1])
                abandon_message(r, msgs[2])
                defer_message(r, msgs[3])
                dead_letter_message(r, msgs[4]; reason="invalid", error_description="failed validation")
                old = msgs[5].locked_until_utc
                sleep(1)
                @test renew_message_lock(r, msgs[5]) > old
                complete_message(r, msgs[5])

                again = receive_messages(r)
                @test sbodies(again) == ["m2"] && again[1].delivery_count == 1
                complete_message(r, again[1])

                deferred = receive_deferred_messages(r, [msgs[3].sequence_number])
                @test sbodies(deferred) == ["m3"] && deferred[1].state == ServiceBusMessageState.DEFERRED
                complete_message(r, deferred[1])
                @test_throws MessageNotFoundError receive_deferred_messages(r, [msgs[3].sequence_number])

                dlq = get_queue_receiver(client, "queue.1"; sub_queue=ServiceBusSubQueue.DEAD_LETTER, max_wait_time=5)
                dm = only(receive_messages(dlq))
                @test body_as_str(dm) == "m4"
                @test dm.dead_letter_reason == "invalid" && dm.dead_letter_error_description == "failed validation"
                complete_message(dlq, dm)
                close(dlq)
                @test isempty(receive_messages(r; max_wait_time=1))
                close(r)
            end

            @testset "settling after the receiving link is gone" begin
                s = get_queue_sender(client, "queue.1")
                r = get_queue_receiver(client, "queue.1"; max_wait_time=5)
                send_messages(s, ServiceBusMessage("orphan"))
                m = only(receive_messages(r))
                AzureSDK.AMQP.detach(r.link[1])
                complete_message(r, m)          # falls back to the management link with the lock token
                @test isempty(receive_messages(r; max_wait_time=1))
                close(r)
            end

            @testset "lock expiry and automatic renewal" begin
                s = get_queue_sender(client, "queue.short-lock")      # 5 s lock duration
                r = get_queue_receiver(client, "queue.short-lock"; max_wait_time=5)
                send_messages(s, ServiceBusMessage("expires"))
                m = only(receive_messages(r))
                sleep(7)
                @test_throws MessageLockLostError complete_message(r, m)
                redelivered = only(receive_messages(r))
                @test body_as_str(redelivered) == "expires" && redelivered.delivery_count == 1
                complete_message(r, redelivered)
                close(r)

                failures = Any[]
                renewer = AutoLockRenewer(max_lock_renewal_duration=60, on_lock_renew_failure=(t, e) -> push!(failures, e))
                r2 = get_queue_receiver(client, "queue.short-lock"; max_wait_time=5, auto_lock_renewer=renewer)
                send_messages(s, ServiceBusMessage("long job"))
                m2 = only(receive_messages(r2))
                sleep(12)                   # well past the 5 s lock
                complete_message(r2, m2)    # still locked thanks to the renewer
                @test isempty(failures)
                close(renewer)
                close(r2)
            end

            @testset "peek, schedule and cancel" begin
                s = get_queue_sender(client, "queue.1")
                r = get_queue_receiver(client, "queue.1"; max_wait_time=5)
                send_messages(s, [ServiceBusMessage("peek$i") for i in 1:3])
                first_seq = peek_messages(r; max_message_count=1, sequence_number=0)[1].sequence_number
                # (scheduled messages left by other runs may show up in a peek as well)
                peeked = filter(m -> startswith(body_as_str(m), "peek"),
                    peek_messages(r; max_message_count=10, sequence_number=first_seq))
                @test sbodies(peeked) == ["peek1", "peek2", "peek3"]
                @test_throws ArgumentError complete_message(r, peeked[1])
                @test isempty(peek_messages(r; max_message_count=10))       # continues after the last peeked
                foreach(m -> complete_message(r, m), receive_messages(r; max_message_count=3))

                later = schedule_messages(s, ServiceBusMessage("later"), now(UTC) + Hour(1))
                @test length(later) == 1
                sched = peek_messages(r; max_message_count=10, sequence_number=later[1])
                @test sbodies(sched) == ["later"] && sched[1].state == ServiceBusMessageState.SCHEDULED
                cancel_scheduled_messages(s, later)
                @test isempty(peek_messages(r; max_message_count=10, sequence_number=later[1]))

                schedule_messages(s, [ServiceBusMessage("soon1"), ServiceBusMessage("soon2")], now(UTC) + Second(3))
                @test isempty(receive_messages(r; max_wait_time=1))
                got = ServiceBusReceivedMessage[]
                deadline = time() + 30
                while length(got) < 2 && time() < deadline
                    append!(got, receive_messages(r; max_message_count=2, max_wait_time=5))
                end
                @test sort(sbodies(got)) == ["soon1", "soon2"]
                foreach(m -> complete_message(r, m), got)
                close(r)
            end

            @testset "receive modes, prefetch and iteration" begin
                s = get_queue_sender(client, "queue.1")
                send_messages(s, [ServiceBusMessage("rd$i") for i in 1:5])
                rd = get_queue_receiver(client, "queue.1"; receive_mode=ServiceBusReceiveMode.RECEIVE_AND_DELETE,
                    max_wait_time=3)
                got = collect(rd)
                @test sbodies(got) == ["rd$i" for i in 1:5]
                @test_throws ArgumentError complete_message(rd, got[1])
                close(rd)

                send_messages(s, [ServiceBusMessage("pf$i") for i in 1:20])
                rp = get_queue_receiver(client, "queue.1"; prefetch_count=50, max_wait_time=3)
                seen = String[]
                for m in rp
                    push!(seen, body_as_str(m))
                    complete_message(rp, m)
                end
                @test seen == ["pf$i" for i in 1:20]
                close(rp)
            end

            @testset "batches, large messages and concurrency" begin
                s = get_queue_sender(client, "queue.1")
                r = get_queue_receiver(client, "queue.1"; receive_mode=ServiceBusReceiveMode.RECEIVE_AND_DELETE,
                    max_wait_time=5)
                batch = create_message_batch(s)
                n = 0
                while try_add_message!(batch, ServiceBusMessage("x"^1000))
                    n += 1
                end
                @test n > 100 && size_in_bytes(batch) <= max_size_in_bytes(batch)
                send_messages(s, batch)
                big = rand(UInt8, 200_000)                  # multi-frame transfer
                send_messages(s, ServiceBusMessage(big))
                @test_throws MessageSizeExceededError send_messages(s, ServiceBusMessage(rand(UInt8, 300_000)))
                @sync for i in 1:100
                    Threads.@spawn send_messages(s, ServiceBusMessage("c$i"))
                end
                total = ServiceBusReceivedMessage[]
                while true
                    got = receive_messages(r; max_message_count=200, max_wait_time=3)
                    isempty(got) && break
                    append!(total, got)
                end
                @test length(total) == n + 1 + 100
                @test any(m -> m.body == big, total)
                @test Set(filter(startswith("c"), sbodies(total))) == Set("c$i" for i in 1:100)
                close(r)
            end

            @testset "topics and subscriptions" begin
                s = get_topic_sender(client, "topic.1")
                send_messages(s, [ServiceBusMessage("event$i"; application_properties=Dict("i" => i)) for i in 1:3])
                for sub in ("subscription.1", "subscription.2")
                    r = get_subscription_receiver(client, "topic.1", sub; max_wait_time=5)
                    got = receive_messages(r; max_message_count=3)
                    length(got) < 3 && append!(got, receive_messages(r; max_message_count=3 - length(got)))
                    @test sbodies(got) == ["event$i" for i in 1:3]
                    foreach(m -> complete_message(r, m), got)
                    close(r)
                end
            end

            @testset "sessions" begin
                s = get_queue_sender(client, "queue.sessions")
                send_messages(s, [ServiceBusMessage("a$i"; session_id="A") for i in 1:3])
                send_messages(s, [ServiceBusMessage("b$i"; session_id="B") for i in 1:2])
                @test_throws ArgumentError send_messages(s, [ServiceBusMessage("x"; session_id="A"),
                    ServiceBusMessage("y"; session_id="B")])

                ra = get_queue_receiver(client, "queue.sessions"; session_id="A", max_wait_time=5)
                got = receive_messages(ra; max_message_count=10)
                @test ra.session.session_id == "A" && sbodies(got) == ["a1", "a2", "a3"]
                @test all(m -> m.session_id == "A", got)
                @test ra.session.locked_until_utc > now(UTC)
                @test get_state(ra.session) === nothing
                set_state(ra.session, "processed=3")
                @test String(get_state(ra.session)) == "processed=3"
                @test renew_lock(ra.session) > now(UTC)
                foreach(m -> complete_message(ra, m), got)
                # the session is locked by `ra`
                @test_throws Union{SessionCannotBeLockedError,ServiceBus.OperationTimeoutError} receive_messages(
                    get_queue_receiver(client, "queue.sessions"; session_id="A", max_wait_time=3))

                rnext = get_queue_receiver(client, "queue.sessions"; session_id=NEXT_AVAILABLE_SESSION, max_wait_time=5)
                gb = receive_messages(rnext; max_message_count=10)
                @test rnext.session.session_id == "B" && sbodies(gb) == ["b1", "b2"]
                peeked = peek_messages(rnext; max_message_count=10, sequence_number=0)
                @test isempty(peeked) || all(m -> m.session_id == "B", peeked)
                foreach(m -> complete_message(rnext, m), gb)

                t = @elapsed @test_throws ServiceBus.OperationTimeoutError receive_messages(
                    get_queue_receiver(client, "queue.sessions"; session_id=NEXT_AVAILABLE_SESSION, max_wait_time=3))
                @test t < 15
                set_state(ra.session, nothing)
                @test get_state(ra.session) === nothing
                close(ra)
                close(rnext)
            end

            @testset "errors" begin
                @test_throws MessagingEntityNotFoundError send_messages(get_queue_sender(client, "no-such-queue"),
                    ServiceBusMessage("x"))
                @test_throws MessagingEntityNotFoundError receive_messages(
                    get_queue_receiver(client, "no-such-queue"; max_wait_time=1))
                # (the emulator accepts any SAS key, so authentication failures are tested with the mock broker)
            end
        finally
            close(client)
        end
    end
else
    @info "Skipping Service Bus emulator tests (set AZURESDK_SERVICEBUS_EMULATOR=1 to run them)"
end
