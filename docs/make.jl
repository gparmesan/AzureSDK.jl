using Documenter, AzureSDK

# Source links point to the GitHub repository when built in GitHub Actions.
repo = get(ENV, "GITHUB_REPOSITORY", "")
# Local builds have no source links.
remote = isempty(repo) ? (; remotes = nothing) : (; repo = Documenter.Remotes.GitHub(repo))

makedocs(;
    sitename = "AzureSDK.jl",
    modules = [AzureSDK],
    remote...,
    format = Documenter.HTML(;
        prettyurls = get(ENV, "CI", "false") == "true",
        edit_link = "main",
        repolink = isempty(repo) ? nothing : "https://github.com/$repo",
        size_threshold_warn = 200 * 2^10,   # the Blob API reference page is large
    ),
    checkdocs = :exports,
    pages = [
        "Home" => "index.md",
        "Guide" => [
            "identity.md",
            "eventhubs.md",
            "servicebus.md",
            "blob.md",
            "transports.md",
            "errors.md",
            "testing.md",
        ],
        "API reference" => [
            "api/core.md",
            "api/identity.md",
            "api/eventhubs.md",
            "api/servicebus.md",
            "api/blob.md",
            "api/amqp.md",
        ],
    ],
)

isempty(repo) || deploydocs(; repo = "github.com/$repo.git", devbranch = "main", push_preview = false)
