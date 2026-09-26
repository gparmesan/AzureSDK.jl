using AzureSDK
const A = AzureSDK.AMQP
using Test, Dates

isdefined(@__MODULE__, :MockBroker) || include("mock_broker.jl")
isdefined(@__MODULE__, :TestCerts) || include("certs.jl")

function raw_connection(b; scheme=:amqp, port=b.port, kw...)
    tc = A.TransportConfig(scheme=scheme, host="localhost", port=port; kw...)
    return A.connect(tc, A.ConnectionConfig(hostname="localhost"))
end

@testset "AMQP engine" begin
    b = MockBroker.Broker()
    MockBroker.entity!(b, "q")
    dir = mktempdir()
    cert, key, _ = TestCerts.self_signed("localhost")
    write(joinpath(dir, "cert.pem"), cert)
    write(joinpath(dir, "key.pem"), key)
    MockBroker.start!(b; tls_cert=joinpath(dir, "cert.pem"), tls_key=joinpath(dir, "key.pem"), websocket=true)
    try
        @testset "connection, session and links" begin
            conn = raw_connection(b)
            @test isopen(conn)
            @test conn.max_frame_size == b.max_frame_size
            s = A.begin_session(conn)
            snd = A.attach_sender(s, "s", "q")
            @test timedwait(() -> A.link_credit(snd) == 1000, 5.0) === :ok
            @test A.send_transfer(snd, A.encode_message(A.AMQPMessage(UInt8[1]))) isa A.Accepted
            # presettled
            @test A.send_transfer(snd, A.encode_message(A.AMQPMessage(UInt8[2])); settled=true) === nothing
            rcv = A.attach_receiver(s, "r", "q"; snd_settle_mode=A.SND_SETTLED)
            @test A.receive_delivery(rcv; timeout=0.2) === nothing        # no credit yet
            A.flow!(rcv, 10)
            d1 = A.receive_delivery(rcv; timeout=5)
            d2 = A.receive_delivery(rcv; timeout=5)
            @test A.body_bytes(A.decode_delivery(d1)) == UInt8[1]
            @test A.body_bytes(A.decode_delivery(d2)) == UInt8[2]
            @test A.drain!(rcv; timeout=5)
            @test A.link_credit(rcv) == 0
            A.detach(rcv)
            @test !A.is_attached(rcv)
            A.end_session(s)
            close(conn)
            @test !isopen(conn)
        end

        @testset "attach to a missing entity reports the service error" begin
            conn = raw_connection(b)
            s = A.begin_session(conn)
            err = try
                A.attach_sender(s, "x", "missing-queue"); nothing
            catch e
                e
            end
            @test err isa A.AMQPException && err.condition == Symbol("amqp:not-found")
            @test occursin("missing-queue", err.description)
            # the session stays usable
            @test A.attach_sender(s, "y", "q") isa A.SenderLink
            close(conn)
        end

        @testset "multi-frame transfers in both directions" begin
            b.split_outgoing = 1000
            b.max_message_size = UInt64(10^7)
            conn = raw_connection(b)
            s = A.begin_session(conn)
            snd = A.attach_sender(s, "s", "q")
            payload = rand(UInt8, 300_000)       # > 64 KiB broker frames
            @test A.send_transfer(snd, A.encode_message(A.AMQPMessage(payload))) isa A.Accepted
            rcv = A.attach_receiver(s, "r", "q"; snd_settle_mode=A.SND_SETTLED, prefetch=5)
            d = A.receive_delivery(rcv; timeout=10)
            @test A.body_bytes(A.decode_delivery(d)) == payload
            close(conn)
            b.split_outgoing = 0
            b.max_message_size = UInt64(256 * 1024)
        end

        @testset "message size limit from the peer" begin
            conn = raw_connection(b)
            s = A.begin_session(conn)
            snd = A.attach_sender(s, "s", "q")
            err = try
                A.send_transfer(snd, rand(UInt8, 300_000)); nothing
            catch e
                e
            end
            @test err isa A.AMQPException && err.condition == A.COND_MSG_SIZE
            close(conn)
        end

        @testset "concurrent pipelined sends" begin
            conn = raw_connection(b)
            s = A.begin_session(conn)
            snd = A.attach_sender(s, "s", "q")
            results = Vector{Any}(undef, 500)
            @sync for i in 1:500
                Threads.@spawn results[i] = A.send_transfer(snd, A.encode_message(A.AMQPMessage(Vector{UInt8}(string(i)))))
            end
            @test all(r -> r isa A.Accepted, results)
            rcv = A.attach_receiver(s, "r", "q"; snd_settle_mode=A.SND_SETTLED, prefetch=100)
            got = Set{String}()
            while (d = A.receive_delivery(rcv; timeout=2)) !== nothing
                push!(got, String(A.body_bytes(A.decode_delivery(d))))
            end
            @test got == Set(string.(1:500))
            close(conn)
        end

        @testset "connection loss fails waiters promptly" begin
            conn = raw_connection(b)
            s = A.begin_session(conn)
            rcv = A.attach_receiver(s, "r", "q"; prefetch=1)
            t = Threads.@spawn try
                A.receive_delivery(rcv; timeout=30)
            catch e
                e
            end
            sleep(0.3)
            MockBroker.kill_connections!(b)
            res = fetch(t)
            @test res isa Exception
            @test !isopen(conn)
            @test_throws Exception A.begin_session(conn)
        end

        @testset "TLS" begin
            conn = raw_connection(b; scheme=:amqps, port=b.tls_port, ca_file=joinpath(dir, "cert.pem"))
            @test isopen(conn)
            s = A.begin_session(conn)
            @test A.send_transfer(A.attach_sender(s, "s", "q"), A.encode_message(A.AMQPMessage(UInt8[7]))) isa A.Accepted
            close(conn)
            # untrusted certificate is refused
            @test_throws Exception raw_connection(b; scheme=:amqps, port=b.tls_port)
            # the certificate must match the host name (dialled through a custom endpoint)
            @test_throws Exception A.connect(A.TransportConfig(scheme=:amqps, host="other.test",
                    custom_endpoint_address="127.0.0.1:$(b.tls_port)", ca_file=joinpath(dir, "cert.pem")),
                A.ConnectionConfig(hostname="other.test"))
            conn = A.connect(A.TransportConfig(scheme=:amqps, host="localhost",
                    custom_endpoint_address="127.0.0.1:$(b.tls_port)", ca_file=joinpath(dir, "cert.pem")),
                A.ConnectionConfig(hostname="localhost"))
            @test isopen(conn)
            close(conn)
        end

        @testset "WebSocket" begin
            conn = raw_connection(b; scheme=:ws, port=b.ws_port)
            s = A.begin_session(conn)
            @test A.send_transfer(A.attach_sender(s, "s", "q"), A.encode_message(A.AMQPMessage(UInt8[8]))) isa A.Accepted
            close(conn)
        end

        @testset "CBS put-token and management request/response" begin
            conn = raw_connection(b)
            s = A.begin_session(conn)
            cbs = A.open_management_link(s, "\$cbs"; reply_to="cbs")
            @test A.put_token(cbs, "sb://localhost/q", "tok"; token_type="jwt", expires_on=time() + 60) == 202
            b.reject_tokens = true
            @test_throws A.AMQPException A.put_token(cbs, "sb://localhost/q", "tok")
            b.reject_tokens = false
            mgmt = A.open_management_link(s, "q/\$management")
            resp = A.request(mgmt, A.AMQPMessage(body_type=:value, body=Dict{String,Any}("session-id" => "none"),
                application_properties=Dict{String,Any}("operation" => "com.microsoft:get-session-state")))
            @test A.response_status(resp)[1] == 200
            close(conn)
        end

        @testset "ClientConnection reconnects and re-authorizes" begin
            provider = aud -> ("token-for-$aud", "jwt", time() + 3600.0)
            cc = A.ClientConnection(A.TransportConfig(scheme=:amqp, host="localhost", port=b.port);
                hostname="localhost", token_provider=provider)
            A.authorize!(cc, "sb://localhost/q")
            _, _, g1 = A.acquire!(cc)
            n = length(b.put_tokens)
            MockBroker.kill_connections!(b)
            sleep(0.2)
            A.authorize!(cc, "sb://localhost/q")
            _, _, g2 = A.acquire!(cc)
            @test g2 == g1 + 1
            @test length(b.put_tokens) == n + 1
            close(cc)
            @test_throws A.ClientClosedError A.acquire!(cc)
        end
    finally
        MockBroker.stop!(b)
    end
end
