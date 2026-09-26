using AzureSDK
using AzureSDK.Identity
using AzureSDK.AzureCore
using Test, HTTP, JSON, Dates, Base64
import OpenSSL_jll

isdefined(@__MODULE__, :TestCerts) || include("certs.jl")

const TENANT = "72f988bf-86f1-41af-91ab-2d7cd011db47"

"Local stand-in for Entra ID / IMDS / App Service token endpoints."
mutable struct TokenServer
    server::Any
    port::Int
    requests::Vector{Any}
    status::Int
    expires_in::Int
end

function start_token_server(; status=200, expires_in=3600)
    ts = TokenServer(nothing, 0, Any[], status, expires_in)
    ts.server = HTTP.serve!("127.0.0.1", 0; listenany=true) do req
        body = req.body isa HTTP.EmptyBody ? "" : String(copy(req.body))
        push!(ts.requests, (method=req.method, target=req.target, headers=Dict(lowercase(k) => v for (k, v) in req.headers),
            form=isempty(body) ? Dict{String,String}() : HTTP.queryparams(body)))
        if ts.status != 200
            return HTTP.Response(ts.status, ["Content-Type" => "application/json"],
                JSON.json(Dict("error" => "invalid_client", "error_description" => "AADSTS7000215: Invalid client secret provided.")))
        end
        n = length(ts.requests)
        return HTTP.Response(200, ["Content-Type" => "application/json"],
            JSON.json(Dict("access_token" => "token-$n", "expires_in" => ts.expires_in, "token_type" => "Bearer")))
    end
    ts.port = HTTP.port(ts.server)
    return ts
end
stop(ts::TokenServer) = HTTP.forceclose(ts.server)
url(ts::TokenServer) = "http://127.0.0.1:$(ts.port)"

const lib = OpenSSL_jll.libcrypto
function verify_rs256(cert_pem::String, jwt::String)

    h, p, s = split(jwt, '.')
    pad(x) = x * repeat("=", (4 - length(x) % 4) % 4)
    sig = base64decode(pad(replace(s, '-' => '+', '_' => '/')))
    data = Vector{UInt8}(codeunits(string(h, ".", p)))
    bio = ccall((:BIO_new_mem_buf, lib), Ptr{Cvoid}, (Ptr{UInt8}, Cint), cert_pem, sizeof(cert_pem))
    x = ccall((:PEM_read_bio_X509, lib), Ptr{Cvoid}, (Ptr{Cvoid}, Ptr{Cvoid}, Ptr{Cvoid}, Ptr{Cvoid}), bio, C_NULL, C_NULL, C_NULL)
    pk = ccall((:X509_get_pubkey, lib), Ptr{Cvoid}, (Ptr{Cvoid},), x)
    ctx = ccall((:EVP_MD_CTX_new, lib), Ptr{Cvoid}, ())
    ccall((:EVP_DigestVerifyInit, lib), Cint, (Ptr{Cvoid}, Ptr{Cvoid}, Ptr{Cvoid}, Ptr{Cvoid}, Ptr{Cvoid}), ctx, C_NULL,
        ccall((:EVP_sha256, lib), Ptr{Cvoid}, ()), C_NULL, pk)
    ok = ccall((:EVP_DigestVerify, lib), Cint, (Ptr{Cvoid}, Ptr{UInt8}, Csize_t, Ptr{UInt8}, Csize_t), ctx, sig,
        length(sig), data, length(data))
    ccall((:EVP_MD_CTX_free, lib), Cvoid, (Ptr{Cvoid},), ctx)
    ccall((:EVP_PKEY_free, lib), Cvoid, (Ptr{Cvoid},), pk)
    ccall((:X509_free, lib), Cvoid, (Ptr{Cvoid},), x)
    ccall((:BIO_free, lib), Cint, (Ptr{Cvoid},), bio)
    return ok == 1
end

function jwt_part(jwt, i)
    x = split(jwt, '.')[i]
    JSON.parse(String(base64decode(replace(x, '-' => '+', '_' => '/') * repeat("=", (4 - length(x) % 4) % 4))))
end

@testset "Identity" begin
    ts = start_token_server()
    try
        @testset "ClientSecretCredential" begin
            empty!(ts.requests)
            cred = ClientSecretCredential(TENANT, "client-id", "s3cr3t-value"; authority=url(ts))
            @test !occursin("s3cr3t", sprint(show, cred))
            t1 = get_token(cred, "https://eventhubs.azure.net/.default")
            @test t1.token == "token-1"
            @test t1.expires_on > time() + 3000
            r = ts.requests[1]
            @test r.method == "POST" && r.target == "/$TENANT/oauth2/v2.0/token"
            @test r.form["grant_type"] == "client_credentials" && r.form["client_secret"] == "s3cr3t-value"
            @test r.form["scope"] == "https://eventhubs.azure.net/.default" && r.form["client_id"] == "client-id"
            # cached
            @test get_token(cred, "https://eventhubs.azure.net/.default").token == "token-1"
            @test length(ts.requests) == 1
            # other scope -> new request
            @test get_token(cred, "https://servicebus.azure.net/.default").token == "token-2"
            # claims bypass the cache and CAE adds client capabilities
            tok = get_token(cred, "https://eventhubs.azure.net/.default"; claims="{\"access_token\":{\"nbf\":{\"essential\":true}}}",
                enable_cae=true)
            @test tok.token == "token-3"
            claims = JSON.parse(ts.requests[end].form["claims"])
            @test claims["access_token"]["xms_cc"]["values"] == ["CP1"]
            # multi-tenant
            @test_throws ClientAuthenticationError get_token(cred, "https://x/.default"; tenant_id="other-tenant")
            cred2 = ClientSecretCredential(TENANT, "client-id", "s"; authority=url(ts), additionally_allowed_tenants=["*"])
            get_token(cred2, "https://x/.default"; tenant_id="other-tenant")
            @test startswith(ts.requests[end].target, "/other-tenant/")
            # validation
            @test_throws ArgumentError ClientSecretCredential("bad/tenant", "c", "s")
            @test_throws ArgumentError ClientSecretCredential(TENANT, "c", "s"; authority="http://login.example.com")
            @test_throws ArgumentError get_token(cred)
        end

        @testset "token refresh" begin
            empty!(ts.requests)
            ts.expires_in = 100      # within the 5 minute refresh window -> refreshed on next use
            cred = ClientSecretCredential(TENANT, "c", "s"; authority=url(ts))
            get_token(cred, "a/.default")
            ts.expires_in = 3600
            sleep(0.01)
            # still valid but due for refresh; the retry delay keeps the cached token for 30 s
            @test get_token(cred, "a/.default").token == "token-1"
        end

        @testset "authentication failure" begin
            ts.status = 401
            cred = ClientSecretCredential(TENANT, "c", "wrong-secret-xyz"; authority=url(ts))
            err = try
                get_token(cred, "https://x/.default"); nothing
            catch e
                e
            end
            @test err isa ClientAuthenticationError
            @test occursin("AADSTS7000215", err.message)
            @test !occursin("wrong-secret-xyz", sprint(showerror, err))
            ts.status = 200
        end

        @testset "ClientCertificateCredential" begin
            for pw in (nothing, "pfx-password")
                cert, key, p12 = TestCerts.self_signed("client"; password=pw)
                for data in (Vector{UInt8}(codeunits(cert * key)), p12)
                    empty!(ts.requests)
                    cred = ClientCertificateCredential(TENANT, "cid"; certificate_data=data, password=pw,
                        authority=url(ts), send_certificate_chain=true)
                    @test get_token(cred, "https://x/.default").token == "token-1"
                    f = ts.requests[1].form
                    @test f["client_assertion_type"] == "urn:ietf:params:oauth:client-assertion-type:jwt-bearer"
                    jwt = f["client_assertion"]
                    @test verify_rs256(cert, jwt)
                    hdr = jwt_part(jwt, 1)
                    payload = jwt_part(jwt, 2)
                    @test hdr["alg"] == "RS256" && haskey(hdr, "x5t") && length(hdr["x5c"]) == 1
                    @test payload["aud"] == "$(url(ts))/$TENANT/oauth2/v2.0/token"
                    @test payload["iss"] == "cid" && payload["sub"] == "cid" && payload["exp"] > time()
                end
            end
            cert, key, _ = TestCerts.self_signed("client"; password="right")
            @test_throws ArgumentError ClientCertificateCredential(TENANT, "cid"; certificate_data=cert * key, password="wrong")
            @test_throws ArgumentError ClientCertificateCredential(TENANT, "cid"; certificate_data="not a certificate")
        end

        @testset "WorkloadIdentityCredential" begin
            empty!(ts.requests)
            f = tempname()
            write(f, "federated-jwt-1\n")
            cred = WorkloadIdentityCredential(tenant_id=TENANT, client_id="wid", token_file_path=f, authority=url(ts))
            @test get_token(cred, "https://x/.default").token == "token-1"
            @test ts.requests[1].form["client_assertion"] == "federated-jwt-1"
            withenv("AZURE_TENANT_ID" => TENANT, "AZURE_CLIENT_ID" => "wid", "AZURE_FEDERATED_TOKEN_FILE" => f,
                "AZURE_AUTHORITY_HOST" => url(ts)) do
                c = WorkloadIdentityCredential()
                @test get_token(c, "https://y/.default").token == "token-2"
            end
            withenv("AZURE_TENANT_ID" => nothing, "AZURE_CLIENT_ID" => nothing, "AZURE_FEDERATED_TOKEN_FILE" => nothing) do
                @test_throws ArgumentError WorkloadIdentityCredential()
            end
        end

        @testset "ClientAssertionCredential" begin
            empty!(ts.requests)
            cred = ClientAssertionCredential(TENANT, "cid", () -> "my-assertion"; authority=url(ts))
            get_token(cred, "https://x/.default")
            @test ts.requests[1].form["client_assertion"] == "my-assertion"
        end

        @testset "ManagedIdentityCredential" begin
            empty!(ts.requests)
            withenv("AZURE_POD_IDENTITY_AUTHORITY_HOST" => url(ts), "IDENTITY_ENDPOINT" => nothing, "MSI_ENDPOINT" => nothing,
                "AZURE_FEDERATED_TOKEN_FILE" => nothing) do
                mi = ManagedIdentityCredential(client_id="11111111-2222-3333-4444-555555555555")
                @test mi.kind === :imds
                @test get_token(mi, "https://eventhubs.azure.net/.default").token == "token-1"
                r = ts.requests[1]
                @test r.method == "GET" && r.headers["metadata"] == "true"
                q = HTTP.queryparams(HTTP.URI(r.target).query)
                @test q["resource"] == "https://eventhubs.azure.net" && q["client_id"] == "11111111-2222-3333-4444-555555555555"
                @test q["api-version"] == "2018-02-01"
                @test_throws ArgumentError get_token(mi, "a/.default", "b/.default")
                @test_throws ArgumentError ManagedIdentityCredential(client_id="a", object_id="b")
            end
            empty!(ts.requests)
            withenv("IDENTITY_ENDPOINT" => url(ts) * "/msi/token", "IDENTITY_HEADER" => "header-secret",
                "IDENTITY_SERVER_THUMBPRINT" => nothing) do
                mi = ManagedIdentityCredential(resource_id="/subscriptions/x/resourceGroups/y/providers/z")
                @test mi.kind === :app_service
                @test get_token(mi, "https://servicebus.azure.net/.default").token == "token-1"
                r = ts.requests[1]
                @test r.headers["x-identity-header"] == "header-secret"
                q = HTTP.queryparams(HTTP.URI(r.target).query)
                @test q["api-version"] == "2019-08-01" && q["mi_res_id"] == "/subscriptions/x/resourceGroups/y/providers/z"
            end
            # no IMDS endpoint reachable inside DefaultAzureCredential -> unavailable, quickly
            withenv("AZURE_POD_IDENTITY_AUTHORITY_HOST" => "http://127.0.0.1:9", "IDENTITY_ENDPOINT" => nothing,
                "MSI_ENDPOINT" => nothing, "AZURE_FEDERATED_TOKEN_FILE" => nothing) do
                mi = ManagedIdentityCredential(_within_dac=true)
                t = @elapsed @test_throws CredentialUnavailableError get_token(mi, "https://x/.default")
                @test t < 10
            end
        end

        if !Sys.iswindows()
            @testset "AzureCliCredential" begin
                dir = mktempdir()
                script = joinpath(dir, "az")
                exp = floor(Int, time()) + 3600
                write(script, """
                    #!/bin/sh
                    case "\$*" in
                      *get-access-token*--resource*https://fail*) echo "ERROR: Please run 'az login' to setup account." >&2; exit 1 ;;
                      *get-access-token*) echo '{"accessToken": "cli-token", "expires_on": $exp, "tokenType": "Bearer"}' ;;
                    esac
                    """)
                chmod(script, 0o755)
                withenv("PATH" => dir * ":" * ENV["PATH"]) do
                    cred = AzureCliCredential()
                    t = get_token(cred, "https://eventhubs.azure.net/.default")
                    @test t.token == "cli-token" && t.expires_on == exp
                    @test_throws CredentialUnavailableError get_token(cred, "https://fail/.default")
                    @test_throws ArgumentError get_token(cred, "https://x/.default; rm -rf /")
                end
                withenv("PATH" => mktempdir()) do
                    @test_throws CredentialUnavailableError get_token(AzureCliCredential(), "https://x/.default")
                end
                # expiresOn in local time (older CLI versions)
                t = Identity._parse_cli_token("{\"accessToken\":\"x\",\"expiresOn\":\"2030-01-01 12:00:00.000000\"}")
                @test abs(t.expires_on - Dates.datetime2unix(DateTime(2030, 1, 1, 12)) +
                          Dates.value(Dates.now() - Dates.now(Dates.UTC)) / 1000) < 120
            end
        end

        @testset "EnvironmentCredential and chains" begin
            withenv("AZURE_TENANT_ID" => TENANT, "AZURE_CLIENT_ID" => "c", "AZURE_CLIENT_SECRET" => "s",
                "AZURE_AUTHORITY_HOST" => url(ts)) do
                env = EnvironmentCredential()
                @test env.credential isa ClientSecretCredential
                @test startswith(get_token(env, "https://x/.default").token, "token-")
            end
            withenv("AZURE_TENANT_ID" => nothing, "AZURE_CLIENT_ID" => nothing, "AZURE_CLIENT_SECRET" => nothing,
                "AZURE_CLIENT_CERTIFICATE_PATH" => nothing, "AZURE_USERNAME" => nothing) do
                @test_throws CredentialUnavailableError get_token(EnvironmentCredential(), "https://x/.default")
            end
            ok = ClientSecretCredential(TENANT, "c", "s"; authority=url(ts))
            chain = ChainedTokenCredential(EnvironmentCredential(), ok)
            withenv("AZURE_TENANT_ID" => nothing, "AZURE_CLIENT_SECRET" => nothing, "AZURE_CLIENT_CERTIFICATE_PATH" => nothing) do
                chain = ChainedTokenCredential(EnvironmentCredential(), ok)
                @test startswith(get_token(chain, "https://x/.default").token, "token-")
            end
            ts.status = 400
            bad = ClientSecretCredential(TENANT, "c", "s"; authority=url(ts))
            err = try
                get_token(ChainedTokenCredential(bad, ok), "https://z/.default"); nothing
            catch e
                e
            end
            @test err isa ClientAuthenticationError   # a real failure stops the chain
            ts.status = 200
            p = get_bearer_token_provider(ok, "https://x/.default")
            @test startswith(p(), "token-")
        end

        @testset "DefaultAzureCredential" begin
            withenv("AZURE_TOKEN_CREDENTIALS" => nothing, "AZURE_FEDERATED_TOKEN_FILE" => nothing) do
                d = DefaultAzureCredential()
                names = [nameof(typeof(c)) for c in d.chain.credentials]
                @test names == [:EnvironmentCredential, :ManagedIdentityCredential, :AzureCliCredential,
                    :AzureDeveloperCliCredential]
            end
            withenv("AZURE_TOKEN_CREDENTIALS" => "dev") do
                @test [nameof(typeof(c)) for c in DefaultAzureCredential().chain.credentials] ==
                      [:AzureCliCredential, :AzureDeveloperCliCredential]
            end
            withenv("AZURE_TOKEN_CREDENTIALS" => "ManagedIdentityCredential") do
                @test [nameof(typeof(c)) for c in DefaultAzureCredential().chain.credentials] == [:ManagedIdentityCredential]
            end
            withenv("AZURE_TOKEN_CREDENTIALS" => "nonsense") do
                @test_throws ArgumentError DefaultAzureCredential()
            end
            withenv("AZURE_TENANT_ID" => TENANT, "AZURE_CLIENT_ID" => "c", "AZURE_CLIENT_SECRET" => "s",
                "AZURE_AUTHORITY_HOST" => url(ts), "AZURE_TOKEN_CREDENTIALS" => nothing) do
                d = DefaultAzureCredential(exclude_managed_identity_credential=true)
                @test startswith(get_token(d, "https://x/.default").token, "token-")
            end
        end
    finally
        stop(ts)
    end
end
