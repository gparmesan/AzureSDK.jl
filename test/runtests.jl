# Test entry point.
#
#   julia --project -e 'using Pkg; Pkg.test()'
#
# Integration tests against the local Event Hubs emulator + Azurite run when
# AZURESDK_EVENTHUBS_EMULATOR=1 (see test/emulator/docker-compose.yml).
using Test

include("mock_broker.jl")
include("certs.jl")

@testset "AzureSDK.jl" begin
    include("test_codec.jl")
    include("test_core.jl")
    include("test_amqp_engine.jl")
    include("test_identity.jl")
    include("test_servicebus.jl")
    include("test_eventhubs.jl")
end
