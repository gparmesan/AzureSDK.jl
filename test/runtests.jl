# Test entry point.
#
#   julia --project -e 'using Pkg; Pkg.test()'
#
# Integration tests against the local emulators run when
#   AZURESDK_EVENTHUBS_EMULATOR=1   (Event Hubs + Azurite, test/emulator/docker-compose.yml)
#   AZURESDK_SERVICEBUS_EMULATOR=1  (Service Bus, test/emulator/servicebus-compose.yml)
#   AZURESDK_AZURITE=1              (Blob Storage, test/emulator/azurite-compose.yml)
# and against real Azure resources when AZURESDK_LIVE=1 (see test/test_live.jl).
using Test

include("mock_broker.jl")
include("certs.jl")

@testset "AzureSDK.jl" begin
    include("test_aqua.jl")
    include("test_codec.jl")
    include("test_core.jl")
    include("test_amqp_engine.jl")
    include("test_identity.jl")
    include("test_servicebus.jl")
    include("test_eventhubs.jl")
    include("test_servicebus_emulator.jl")
    include("test_blob.jl")
    include("test_blob_azurite.jl")
    include("test_live.jl")
end
