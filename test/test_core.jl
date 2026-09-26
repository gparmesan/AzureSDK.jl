using AzureSDK
using AzureSDK.AzureCore
using AzureSDK.EventHubs
using AzureSDK.ServiceBus
using Test, Dates

@testset "AzureCore" begin
    @testset "SAS tokens" begin
        # reference value computed with the algorithm used by the Python SDK
        tok, exp = generate_sas_token("sb://x.servicebus.windows.net/hub", "k", "v"; expiry=3600, now=1000)
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
            "Endpoint=sb://ns/;SharedAccessKeyName=n;SharedAccessKey=k;SharedAccessSignature=s")
        # the emulator flag is refused for non-local hosts (it disables TLS)
        @test_throws ArgumentError ServiceBus.parse_connection_string(
            "Endpoint=sb://ns.servicebus.windows.net;SharedAccessKeyName=n;SharedAccessKey=k;UseDevelopmentEmulator=true")
    end

    @testset "credentials never print secrets" begin
        @test !occursin("topsecret", sprint(show, AzureSasCredential("topsecret")))
        c = AzureNamedKeyCredential("name", "topsecret")
        @test !occursin("topsecret", sprint(show, c))
        update!(c, "n2", "k2")
        @test named_key(c) == (name="n2", key="k2")
        @test !occursin("topsecret", sprint(show, AccessToken("topsecret", 1)))
        @test !occursin("topsecret", sprint(show, AccessTokenInfo("topsecret", 1)))
        @test !occursin("topsecret", sprint(show, AzureKeyCredential("topsecret")))
        @test_throws ArgumentError AzureSasCredential("")
    end

    @testset "retry back-off" begin
        opts = RetryOptions(retry_total=3, retry_backoff_factor=1.0, retry_backoff_max=5.0)
        @test 0.8 <= AzureCore.backoff_delay(opts, 1) <= 1.2
        @test 1.6 <= AzureCore.backoff_delay(opts, 2) <= 2.4
        @test AzureCore.backoff_delay(opts, 10) <= 5.0
        fixed = RetryOptions(retry_mode=RetryMode.Fixed, retry_backoff_factor=2.0)
        @test 1.6 <= AzureCore.backoff_delay(fixed, 5) <= 2.4
    end

    @testset "URL redaction" begin
        @test AzureCore.redact_url("https://h:1/p/x?sig=secret&a=b") == "https://h:1/p/x"
    end
end
