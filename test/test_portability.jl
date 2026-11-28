# Guards against source-tree problems that only show up on macOS and Windows:
# - their default filesystems are case-insensitive, so two paths differing only
#   in case (e.g. `Blob.jl` and `blob.jl`) are the same file there;
# - Windows rejects reserved device names (`con`, `aux`, `nul`, `com1`, ...),
#   the characters `<>:"|?*`, and names ending in a dot or a space.
using AzureSDK

# Paths tracked by git, relative to `root`, with `/` separators. Falls back to
# walking the whole tree when this is not a git checkout (e.g. an installed
# package), skipping only the `.git` directory.
function tracked_paths(root)
    if isdir(joinpath(root, ".git")) && Sys.which("git") !== nothing
        out = read(`git -C $root ls-files -z`, String)
        return filter!(!isempty, split(out, '\0'))
    end
    paths = String[]
    for (base, dirs, files) in walkdir(root)
        filter!(!=(".git"), dirs)
        for name in files
            push!(paths, join(splitpath(relpath(joinpath(base, name), root)), '/'))
        end
    end
    return paths
end

# Every file and every directory that contains one.
function all_entries(files)
    entries = Set{String}()
    for f in files
        parts = split(f, '/')
        for i in eachindex(parts)
            push!(entries, join(parts[1:i], '/'))
        end
    end
    return sort!(collect(entries))
end

const WINDOWS_RESERVED = Set(
    vcat(
        ["con", "prn", "aux", "nul"],
        ["com$i" for i in 0:9],
        ["lpt$i" for i in 0:9],
    ),
)

function windows_problem(name)
    any(in("<>:\"|?*\\"), name) && return "forbidden character"
    any(c -> c < ' ', name) && return "control character"
    (endswith(name, '.') || endswith(name, ' ')) && return "ends in a dot or space"
    lowercase(first(split(name, '.'))) in WINDOWS_RESERVED && return "reserved name"
    return nothing
end

@testset "Portable file names" begin
    entries = all_entries(tracked_paths(pkgdir(AzureSDK)))
    @test !isempty(entries)

    @testset "no case-only collisions" begin
        groups = Dict{String, Vector{String}}()
        for p in entries
            push!(get!(groups, lowercase(p), String[]), p)
        end
        collisions = sort!([sort(v) for v in values(groups) if length(v) > 1])
        @test isempty(collisions)
    end

    @testset "valid on Windows" begin
        bad = [
            p => problem for p in entries
                for problem in (windows_problem(basename(p)),)
                if problem !== nothing
        ]
        @test isempty(bad)
    end
end
