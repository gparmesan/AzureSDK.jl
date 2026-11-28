# Guards against source-tree problems that only show up on macOS and Windows,
# whose default filesystems are case-insensitive: two paths differing only in
# case (e.g. `Blob.jl` and `blob.jl`) are the same file there.
@testset "Portable file names" begin
    root = pkgdir(AzureSDK)
    paths = String[]
    for dir in ("src", "test", "docs", "examples", "benchmark", "scripts")
        isdir(joinpath(root, dir)) || continue
        for (base, dirs, files) in walkdir(joinpath(root, dir))
            for name in Iterators.flatten((dirs, files))
                push!(paths, relpath(joinpath(base, name), root))
            end
        end
    end
    groups = Dict{String, Vector{String}}()
    for p in paths
        push!(get!(groups, lowercase(p), String[]), p)
    end
    collisions = sort!([sort(v) for v in values(groups) if length(v) > 1])
    @test isempty(collisions)
end
