# Live tests against real Azure resources: Entra ID credentials, the
# WebSocket and HTTP transports, SAS token credentials and user delegation
# SAS. The emulator suites cover the functional surface; run them against the
# same resources as well (see docs/src/testing.md).
#
# Enable with AZURESDK_LIVE=1 and
#   AZURESDK_LIVE_EVENTHUBS_NAMESPACE    e.g. myns.servicebus.windows.net
#   AZURESDK_LIVE_EVENTHUB               an event hub only these tests use (default "live-hub")
#   AZURESDK_LIVE_SERVICEBUS_NAMESPACE   e.g. myns.servicebus.windows.net
#   AZURESDK_LIVE_QUEUE                  a queue only these tests use (default "live-queue")
#   AZURESDK_LIVE_STORAGE_URL            e.g. https://myaccount.blob.core.windows.net
# Entra ID access uses DefaultAzureCredential (e.g. `az login`); the identity
# needs the Event Hubs, Service Bus and Storage Blob "Data Owner" roles.
# Optional:
#   AZURESDK_LIVE_TENANT_ID, AZURESDK_LIVE_CLIENT_ID and
#   AZURESDK_LIVE_CLIENT_SECRET and/or AZURESDK_LIVE_CLIENT_CERTIFICATE_PATH
#       also test ClientSecretCredential / ClientCertificateCredential
#   AZURESDK_LIVE_EVENTHUBS_CONNECTION_STRING, AZURESDK_LIVE_SERVICEBUS_CONNECTION_STRING
#       also test AzureSasCredential with tokens made from the namespace keys
using AzureSDK
using AzureSDK.AzureCore
using AzureSDK.Identity
using AzureSDK.EventHubs
using AzureSDK.ServiceBus
using AzureSDK.Storage.Blob
using Dates
using Test
import UUIDs

if get(ENV, "AZURESDK_LIVE", "") == "1"
    const LIVE_EH_NS = ENV["AZURESDK_LIVE_EVENTHUBS_NAMESPACE"]
    const LIVE_EH = get(ENV, "AZURESDK_LIVE_EVENTHUB", "live-hub")
    const LIVE_SB_NS = ENV["AZURESDK_LIVE_SERVICEBUS_NAMESPACE"]
    const LIVE_QUEUE = get(ENV, "AZURESDK_LIVE_QUEUE", "live-queue")
    const LIVE_STORAGE = ENV["AZURESDK_LIVE_STORAGE_URL"]
    _live_env(name) = (v = get(ENV, name, ""); isempty(v) ? nothing : v)
    # UUIDs.uuid4 uses the OS random source; Test reseeds the default RNG in every testset
    _live_id() = "live-" * string(UUIDs.uuid4())

    "Token credentials to test, by name."
    function live_credentials()
        creds = Pair{String, Any}["DefaultAzureCredential" => DefaultAzureCredential()]
        tenant, client = _live_env("AZURESDK_LIVE_TENANT_ID"), _live_env("AZURESDK_LIVE_CLIENT_ID")
        if tenant !== nothing && client !== nothing
            secret = _live_env("AZURESDK_LIVE_CLIENT_SECRET")
            secret === nothing || push!(creds, "ClientSecretCredential" => ClientSecretCredential(tenant, client, secret))
            cert = _live_env("AZURESDK_LIVE_CLIENT_CERTIFICATE_PATH")
            cert === nothing || push!(creds, "ClientCertificateCredential" => ClientCertificateCredential(tenant, client, cert))
        end
        return creds
    end

    "A SAS token credential for `namespace/entity`, made from a namespace connection string's key."
    function live_sas_credential(conn_str, entity)
        d = AzureCore.parse_connection_string_pairs(conn_str)
        host = replace(d["endpoint"], r"^sb://" => "", r"/+$" => "")
        tok, _ = generate_sas_token("sb://$host/$entity", d["sharedaccesskeyname"], d["sharedaccesskey"]; expiry = 900)
        return AzureSasCredential(tok)
    end

    "Send one event to partition 0 and read it back over AMQP."
    function eh_roundtrip(cred, transport; read_cred = cred, read_transport = transport)
        marker = _live_id()
        start = EventHubProducerClient(LIVE_EH_NS, LIVE_EH, read_cred; transport_type = read_transport) do p
            get_partition_properties(p, "0")["last_enqueued_sequence_number"]
        end
        EventHubProducerClient(LIVE_EH_NS, LIVE_EH, cred; transport_type = transport) do p
            if transport == TransportType.Http
                send_event(p, EventData(marker; properties = Dict("n" => 1)); partition_id = "0")
            else
                batch = create_batch(p; partition_id = "0")
                add!(batch, EventData(marker; properties = Dict("n" => 1)))
                send_batch(p, batch)
            end
        end
        consumer = EventHubConsumerClient(LIVE_EH_NS, LIVE_EH, "\$Default", read_cred; transport_type = read_transport)
        got = Ref{Any}(nothing)
        task = Threads.@spawn receive(
            consumer, (ctx, ev) -> begin
                ev === nothing && return
                if body_as_str(ev) == marker
                    got[] = ev
                    close(consumer)
                end
            end; partition_id = "0", starting_position = start, starting_position_inclusive = false, max_wait_time = 5
        )
        timedwait(() -> istaskdone(task), 60.0)
        istaskdone(task) || close(consumer)
        return got[]
    end

    "Send one message to the live queue and receive it in receive-and-delete mode."
    function sb_roundtrip(cred, transport)
        marker = _live_id()
        return ServiceBusClient(LIVE_SB_NS, cred; transport_type = transport) do client
            send_messages(
                get_queue_sender(client, LIVE_QUEUE),
                ServiceBusMessage(marker; application_properties = Dict("n" => 1))
            )
            r = get_queue_receiver(
                client, LIVE_QUEUE; receive_mode = ServiceBusReceiveMode.RECEIVE_AND_DELETE,
                max_wait_time = 10
            )
            deadline = time() + 60
            while time() < deadline
                for m in receive_messages(r; max_message_count = 10)
                    body_as_str(m) == marker && return m
                end
            end
            return nothing
        end
    end

    @testset "Live Azure" begin
        creds = live_credentials()

        @testset "tokens: $name" for (name, cred) in creds
            for scope in (
                    "https://eventhubs.azure.net/.default", "https://servicebus.azure.net/.default",
                    "https://storage.azure.com/.default",
                )
                tok = get_token(cred, scope)
                @test !isempty(tok.token) && tok.expires_on > time() + 60
            end
            secret = _live_env("AZURESDK_LIVE_CLIENT_SECRET")
            @test secret === nothing || !occursin(secret, repr(cred))
        end

        @testset "Event Hubs: $name over $transport" for (name, cred) in creds,
                transport in (TransportType.Amqp, TransportType.AmqpOverWebsocket)
            ev = eh_roundtrip(cred, transport)
            @test ev !== nothing && ev.properties["n"] == 1 && ev.partition_key === nothing
        end

        @testset "Event Hubs: HTTP publishing" begin
            ev = eh_roundtrip(first(creds).second, TransportType.Http; read_transport = TransportType.Amqp)
            @test ev !== nothing && ev.properties["n"] == 1
        end

        @testset "Service Bus: $name over $transport" for (name, cred) in creds,
                transport in (TransportType.Amqp, TransportType.AmqpOverWebsocket, TransportType.Http)
            m = sb_roundtrip(cred, transport)
            @test m !== nothing && m.application_properties["n"] == 1
        end

        eh_cs = _live_env("AZURESDK_LIVE_EVENTHUBS_CONNECTION_STRING")
        sb_cs = _live_env("AZURESDK_LIVE_SERVICEBUS_CONNECTION_STRING")
        if eh_cs !== nothing
            @testset "Event Hubs: AzureSasCredential" begin
                @test eh_roundtrip(live_sas_credential(eh_cs, LIVE_EH), TransportType.AmqpOverWebsocket) !== nothing
            end
        end
        if sb_cs !== nothing
            @testset "Service Bus: AzureSasCredential" begin
                sas = live_sas_credential(sb_cs, LIVE_QUEUE)
                @test sb_roundtrip(sas, TransportType.Amqp) !== nothing
                @test sb_roundtrip(sas, TransportType.Http) !== nothing
                # a token for another entity is rejected
                other = live_sas_credential(sb_cs, "no-such-queue")
                @test_throws ServiceBusError ServiceBusClient(LIVE_SB_NS, other; retry_total = 0) do client
                    send_messages(get_queue_sender(client, LIVE_QUEUE), ServiceBusMessage("x"))
                end
            end
        end

        @testset "Blob Storage: $name" for (name, cred) in creds
            svc = BlobServiceClient(LIVE_STORAGE; credential = cred, max_single_put_size = 4 * 1024 * 1024)
            cc = create_container(svc, _live_id())
            try
                upload_blob(cc, "dir/a.txt", "hello from $name"; metadata = Dict("who" => "live"))
                @test content_as_text(download_blob(cc, "dir/a.txt")) == "hello from $name"
                @test [b.name for b in list_blobs(cc)] == ["dir/a.txt"]
                big = rand(UInt8, 9 * 1024 * 1024 + 7)
                upload_blob(cc, "big.bin", big; max_concurrency = 4)   # parallel blocks
                @test readall(download_blob(cc, "big.bin"; max_concurrency = 4)) == big

                # user delegation SAS (only possible with Entra ID)
                udk = get_user_delegation_key(svc, now(UTC) - Minute(5), now(UTC) + Hour(1))
                sas = generate_blob_sas(
                    svc.account_name, cc.container_name, "dir/a.txt"; user_delegation_key = udk,
                    permission = BlobSasPermissions(read = true), expiry = now(UTC) + Minute(30)
                )
                anon = from_blob_url(get_blob_client(cc, "dir/a.txt").url * "?" * sas)
                @test anon.credential === nothing
                @test content_as_text(download_blob(anon)) == "hello from $name"
                @test_throws ClientAuthenticationError upload_blob(anon, "overwrite"; overwrite = true)
                csas = generate_container_sas(
                    svc.account_name, cc.container_name; user_delegation_key = udk,
                    permission = "rl", expiry = now(UTC) + Minute(30)
                )
                @test length(collect(list_blobs(from_container_url(cc.url * "?" * csas)))) == 2
                @test !occursin("sig=", repr(anon))
            finally
                delete_container(cc)
            end
        end

        @testset "Blob Storage: anonymous access is refused" begin
            anon = BlobServiceClient(LIVE_STORAGE)
            @test_throws Union{ClientAuthenticationError, ResourceNotFoundError, HttpResponseError} collect(list_containers(anon))
        end
    end
else
    @info "Skipping live Azure tests (set AZURESDK_LIVE=1; see test/test_live.jl)"
end
