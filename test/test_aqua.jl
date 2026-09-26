# Package quality checks: method ambiguities, unbound type parameters,
# undefined exports, stale dependencies, compat bounds, type piracy and
# tasks left running after loading.
using Aqua, AzureSDK

@testset "Aqua" begin
    Aqua.test_all(AzureSDK)
end
