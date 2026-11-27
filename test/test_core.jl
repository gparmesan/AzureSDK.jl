using AzureSDK
using AzureSDK.AzureCore
using AzureSDK.EventHubs
using AzureSDK.ServiceBus
using Test, Dates
const Messaging = AzureSDK.Messaging

struct StaticTokenCredential <: TokenCredential end
AzureCore.get_token_info(::StaticTokenCredential, scopes...; kwargs...) =
    AccessTokenInfo("tok", floor(Int, time()) + 3600)

@testset "AzureCore" begin
    @testset "SAS tokens" begin
        # reference value computed with the algorithm used by the Python SDK
        tok, exp = generate_sas_token("sb://x.servicebus.windows.net/hub", "k", "v"; expiry = 3600, now = 1000)
        @test exp == 4600
        @test tok == "SharedAccessSignature sr=sb%3A%2F%2Fx.servicebus.windows.net%2Fhub&sig=ls4fKQBYhzbd863Cng%2B3xEciG7vvn6YFJGHvTFAfEBg%3D&se=4600&skn=k"
        @test AzureCore.sas_expiry(tok) == 4600
        @test AzureCore.sas_expiry("sr=x&sig=y") === nothing
    end

    @testset "connection strings" begin
        d = parse_connection_string_pairs("Endpoint=sb://ns.servicebus.windows.net/;SharedAccessKeyName=n;SharedAccessKey=abc==;")
        @test d["endpoint"] == "sb://ns.servicebus.windows.net/" && d["sharedaccesskey"] == "abc=="
        @test_throws ArgumentError parse_connection_string_pairs("")
        @test_throws ArgumentError parse_connection_string_pairs("Endpoint")
        @test_throws ArgumentError parse_connection_string_pairs("a=1;A=2")

        p = EventHubs.parse_connection_string("Endpoint=sb://ns.servicebus.windows.net/;SharedAccessKeyName=n;SharedAccessKey=k;EntityPath=hub")
        @test p.fully_qualified_namespace == "ns.servicebus.windows.net" && p.eventhub_name == "hub"
        @test !occursin("k;", sprint(show, p))
        q = ServiceBus.parse_connection_string("Endpoint=sb://ns.servicebus.windows.net/;SharedAccessSignature=SharedAccessSignature sr=a&sig=b&se=1&skn=c")
        @test q.shared_access_signature !== nothing && q.shared_access_key === nothing
        @test_throws ArgumentError ServiceBus.parse_connection_string("Endpoint=sb://ns/;SharedAccessKeyName=n")
        @test_throws ArgumentError ServiceBus.parse_connection_string(
            "Endpoint=sb://ns/;SharedAccessKeyName=n;SharedAccessKey=k;SharedAccessSignature=s"
        )
        # the emulator flag is refused for non-local hosts (it disables TLS)
        @test_throws ArgumentError ServiceBus.parse_connection_string(
            "Endpoint=sb://ns.servicebus.windows.net;SharedAccessKeyName=n;SharedAccessKey=k;UseDevelopmentEmulator=true"
        )
        for h in ("127.evil.example", "emulator.test")
            @test_throws ArgumentError ServiceBus.parse_connection_string(
                "Endpoint=sb://$h;SharedAccessKeyName=n;SharedAccessKey=k;UseDevelopmentEmulator=true"
            )
        end
        # docker-compose service names are allowed, but only with the emulator's public key: such
        # names may resolve through the DNS search domain to another machine, and there is no TLS
        @test ServiceBus.parse_connection_string(
            "Endpoint=sb://servicebus-emulator;SharedAccessKeyName=n;" *
                "SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true"
        ) !== nothing
        @test_throws ArgumentError ServiceBus.parse_connection_string(
            "Endpoint=sb://servicebus-emulator;SharedAccessKeyName=n;SharedAccessKey=k;UseDevelopmentEmulator=true"
        )
        @test_throws ArgumentError ServiceBus.parse_connection_string(
            "Endpoint=sb://servicebus-emulator;" *
                "SharedAccessSignature=SharedAccessSignature sr=a&sig=b&se=1&skn=c;UseDevelopmentEmulator=true"
        )
        # loopback hosts accept any key
        @test ServiceBus.parse_connection_string(
            "Endpoint=sb://localhost;SharedAccessKeyName=n;SharedAccessKey=k;UseDevelopmentEmulator=true"
        ) !== nothing
        # the keyword form is checked too, and Entra tokens only go unencrypted to loopback hosts
        @test_throws ArgumentError ServiceBusClient(
            "ns.servicebus.windows.net", AzureNamedKeyCredential("n", "k");
            use_development_emulator = true
        )
        @test_throws ArgumentError ServiceBusClient("servicebus-emulator", StaticTokenCredential(); use_development_emulator = true)
        close(ServiceBusClient("localhost", StaticTokenCredential(); use_development_emulator = true))
        close(
            ServiceBusClient(
                "servicebus-emulator", AzureNamedKeyCredential("n", "SAS_KEY_VALUE");
                use_development_emulator = true
            )
        )
        @test_throws ArgumentError ServiceBusClient(
            "servicebus-emulator", AzureNamedKeyCredential("n", "k");
            use_development_emulator = true
        )
        @test_throws ArgumentError ServiceBusClient(
            "servicebus-emulator",
            AzureSasCredential("SharedAccessSignature sr=a&sig=b&se=1"); use_development_emulator = true
        )
        close(ServiceBusClient("localhost", AzureNamedKeyCredential("n", "k"); use_development_emulator = true))
        # rotating to a real key after creation is caught when the key is used
        rotated = AzureNamedKeyCredential("n", "SAS_KEY_VALUE")
        cfg = Messaging.ClientConfig("servicebus-emulator", rotated, "scope", "test"; use_development_emulator = true)
        @test startswith(Messaging.http_authorization(cfg, "sb://servicebus-emulator/q"), "SharedAccessSignature ")
        AzureCore.update!(rotated, "n", "real-key")
        @test_throws ArgumentError Messaging.http_authorization(cfg, "sb://servicebus-emulator/q")
        @test_throws ArgumentError Messaging._checked_token_provider(cfg)("sb://servicebus-emulator/q")
        # the private _scheme hook only selects the in-process loopback peer
        for sch in (:amqp, :ws, :amqps, :wss, "amqp")
            @test_throws ArgumentError Messaging.ClientConfig(
                "ns.servicebus.windows.net", StaticTokenCredential(), "scope",
                "test"; _scheme = sch
            )
        end
        @test Messaging.ClientConfig(
            "ns.servicebus.windows.net", StaticTokenCredential(), "scope", "test";
            _scheme = :loopback
        ).scheme_override === :loopback
    end

    @testset "local and emulator hosts" begin
        for h in ("localhost", "LOCALHOST", "app.localhost", "127.0.0.1", "127.1.2.3", "::1", "[::1]")
            @test AzureCore.is_local_host(h)
        end
        for h in (
                "127.evil.example", "127.0.0.1.nip.io", "127.0.0.256", "localhost.evil.example", "fileserver",
                "x.test", "10.0.0.1", "",
            )
            @test !AzureCore.is_local_host(h)
        end
        @test AzureCore.is_dev_host("azurite") && AzureCore.is_dev_host("servicebus-emulator")
        @test AzureCore.is_dev_host("127.0.0.1")
        for h in ("127.evil.example", "x.test", "acct.blob.core.windows.net", "-bad", "")
            @test !AzureCore.is_dev_host(h)
        end
    end

    @testset "credentials never print secrets" begin
        @test !occursin("topsecret", sprint(show, AzureSasCredential("topsecret")))
        c = AzureNamedKeyCredential("name", "topsecret")
        @test !occursin("topsecret", sprint(show, c))
        update!(c, "n2", "k2")
        @test named_key(c) == (name = "n2", key = "k2")
        @test !occursin("topsecret", sprint(show, AccessToken("topsecret", 1)))
        @test !occursin("topsecret", sprint(show, AccessTokenInfo("topsecret", 1)))
        @test !occursin("topsecret", sprint(show, AzureKeyCredential("topsecret")))
        @test_throws ArgumentError AzureSasCredential("")
    end

    @testset "retry back-off" begin
        opts = RetryOptions(retry_total = 3, retry_backoff_factor = 1.0, retry_backoff_max = 5.0)
        @test 0.8 <= AzureCore.backoff_delay(opts, 1) <= 1.2
        @test 1.6 <= AzureCore.backoff_delay(opts, 2) <= 2.4
        @test AzureCore.backoff_delay(opts, 10) <= 5.0
        fixed = RetryOptions(retry_mode = RetryMode.Fixed, retry_backoff_factor = 2.0)
        @test 1.6 <= AzureCore.backoff_delay(fixed, 5) <= 2.4
    end

    @testset "URL redaction" begin
        @test AzureCore.redact_url("https://h:1/p/x?sig=secret&a=b") == "https://h:1/p/x"
    end
end
