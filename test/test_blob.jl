# Unit tests for AzureSDK.Storage and AzureSDK.Storage.Blob (no emulator
# needed): XML handling, URL / connection-string / credential parsing, SAS
# layout, and the HTTP pipeline against an in-process mock server.

using AzureSDK
using AzureSDK.Storage
using AzureSDK.Storage.Blob
using AzureSDK.AzureCore
using Test, HTTP, Dates, Base64
import SHA

const S = AzureSDK.Storage
const DEVKEY = S.DEV_ACCOUNT_KEY

"Programmable stand-in for the Blob service."
mutable struct MockStorage
    server::Any
    port::Int
    requests::Vector{Any}
    handler::Function
end

function start_mock_storage()
    ms = MockStorage(nothing, 0, Any[], req -> HTTP.Response(200))
    ms.server = HTTP.serve!("127.0.0.1", 0; listenany=true) do req
        body = req.body isa HTTP.EmptyBody ? UInt8[] : Vector{UInt8}(copy(req.body))
        r = (method=req.method, target=req.target, headers=Dict(lowercase(k) => v for (k, v) in req.headers), body=body)
        push!(ms.requests, r)
        try
            return Base.invokelatest(ms.handler, r)
        catch err
            @error "mock storage handler failed" exception = (err, catch_backtrace())
            return HTTP.Response(599)
        end
    end
    ms.port = HTTP.port(ms.server)
    return ms
end
stop(ms::MockStorage) = HTTP.forceclose(ms.server)
mockurl(ms::MockStorage) = "http://127.0.0.1:$(ms.port)/devstoreaccount1"

"TokenCredential that records requests and issues `tok-<tenant>` tokens."
mutable struct FakeTokenCredential <: TokenCredential
    calls::Vector{Any}
end
function AzureCore.get_token_info(c::FakeTokenCredential, scopes::AbstractString...; claims=nothing, tenant_id=nothing,
    enable_cae=false)
    push!(c.calls, (scopes=collect(scopes), tenant_id=tenant_id))
    return AccessTokenInfo("tok-$(something(tenant_id, "home"))", floor(Int, time()) + 3600)
end

xml_error(code, msg) = """<?xml version="1.0" encoding="utf-8"?><Error><Code>$code</Code><Message>$msg
RequestId:0000
Time:2026-01-01T00:00:00.0000000Z</Message></Error>"""

@testset "Blob Storage (unit)" begin
    @testset "XML" begin
        x = S.xml_parse("\ufeff<?xml version=\"1.0\"?><!-- c --><A x=\"1\" y='a&amp;b'><B>t&lt;1&gt; &#65;&#x42;</B><C/>" *
                        "<B><![CDATA[<raw>]]></B></A>")
        @test x.tag == "A" && S.xattr(x, "y") == "a&b"
        @test [S.xtext(b) for b in S.xfindall(x, "B")] == ["t<1> AB", "<raw>"]
        @test S.xfind(x, "C") !== nothing && S.xfind(x, "D") === nothing
        @test S.xchildtext(x, "D", "default") == "default"
        @test S.xml_parse(Vector{UInt8}("<a>ü</a>")) |> S.xtext == "ü"
        for bad in ("<!DOCTYPE x [<!ENTITY a \"b\">]><x>&a;</x>", "<a><b></a>", "<a>", "<a>&bogus;</a>", "text",
            "<a></a><b></b>", "<a x=1></a>", String(UInt8[0x3c, 0x61, 0x3e, 0xff, 0x3c, 0x2f, 0x61, 0x3e]))
            @test_throws S.XMLParseError S.xml_parse(bad)
        end
        @test_throws S.XMLParseError S.xml_parse(repeat("<a>", 200) * repeat("</a>", 200))
        @test S.xml_element("K", "a<b & \"c\"") == "<K>a&lt;b &amp; &quot;c&quot;</K>"
        @test S.xml_element("K", nothing) == ""
        @test S.xml_element("L", ["x" => "1", "y" => true]) == "<L><x>1</x><y>true</y></L>"
    end

    @testset "CaseInsensitiveDict and paging" begin
        d = CaseInsensitiveDict{String}()
        d["Owner"] = "me"
        @test d["owner"] == "me" && d["OWNER"] == "me" && haskey(d, "oWnEr")
        @test collect(keys(d)) == ["Owner"]
        @test d == Dict("owner" => "me") && Dict("OWNER" => "me") == d
        @test d != Dict("owner" => "you")
        d["OWNER"] = "you"
        @test length(d) == 1 && d["owner"] == "you"
        delete!(d, "owner")
        @test isempty(d)
        @test_throws KeyError d["x"]
        @test get(CaseInsensitiveDict("A" => 1), "a", 0) == 1

        pages = Dict{Union{Nothing,String},Tuple{Vector{Int},Union{Nothing,String}}}(
            nothing => ([1, 2], "t1"), "t1" => (Int[], "t2"), "t2" => ([3], nothing))
        calls = Ref(0)
        p = ItemPaged{Int}(tok -> (calls[] += 1; pages[tok]))
        @test collect(p) == [1, 2, 3] && calls[] == 3
        @test collect(p) == [1, 2, 3]          # re-iterable
        @test collect(by_page(p)) == [[1, 2], Int[], [3]]
        @test collect(by_page(p; continuation_token="t2")) == [[3]]
        @test isempty(collect(ItemPaged{Int}(_ -> (Int[], nothing))))
        @test eltype(p) == Int
    end

    @testset "helpers" begin
        @test S.parse_datetime("Mon, 05 Jan 2026 10:11:12 GMT") == DateTime(2026, 1, 5, 10, 11, 12)
        @test S.parse_datetime("2026-01-05T10:11:12.1234567Z") == DateTime(2026, 1, 5, 10, 11, 12, 123)
        @test S.parse_datetime("2026-01-05T10:11Z") == DateTime(2026, 1, 5, 10, 11)
        @test S.parse_datetime("garbage") === nothing && S.parse_datetime(nothing) === nothing
        @test S.iso8601(DateTime(2026, 1, 5, 10, 11, 12, 999)) == "2026-01-05T10:11:12Z"
        @test abs(S.to_utc(Dates.Hour(1)) - (Dates.now(Dates.UTC) + Dates.Hour(1))) < Dates.Second(5)
        @test_throws ArgumentError S.to_utc(1.5)
        @test S.quote_url("a b/ü~?&=+") == "a%20b/%C3%BC~%3F%26%3D%2B"
        @test S.quote_url("a/b"; safe="") == "a%2Fb"
        @test S.is_sas_token("sv=2024-08-04&se=2026&sp=r&sig=abc%3D") && !S.is_sas_token("comp=list")
        @test S.md5(Vector{UInt8}("abc")) == hex2bytes("900150983cd24fb0d6963f7d28e17f72")
        @test Blob._decode_block_id(Blob._encode_block_id("block-1")) == "block-1"
        k = CustomerProvidedEncryptionKey(zeros(UInt8, 32))
        @test k.key_hash == base64encode(SHA.sha256(zeros(UInt8, 32))) && !occursin(k.key_value, repr(k))
        @test_throws ArgumentError CustomerProvidedEncryptionKey(zeros(UInt8, 16))
        @test Blob.enum_parse(BlobType.T, "pageblob") == BlobType.PAGEBLOB
        @test Blob.enum_parse(BlobType.T, "nope") === nothing
        @test Blob.enum_value(StandardBlobTier.COOL) == "Cool"
    end

    @testset "URLs, connection strings and credentials" begin
        u = S.parse_storage_url("https://myacct.blob.core.windows.net/cont/dir/a%20b.txt?sv=1&sig=x&se=2", "blob")
        @test !u.path_style && u.account_name == "myacct" && u.segments == ["cont", "dir", "a b.txt"]
        u = S.parse_storage_url("http://127.0.0.1:10000/devstoreaccount1/c", "blob")
        @test u.path_style && u.account_name == "devstoreaccount1" && u.segments == ["c"]
        u = S.parse_storage_url("http://azurite:10000/devstoreaccount1", "blob")     # docker-compose service name
        @test u.path_style && u.account_name == "devstoreaccount1"
        @test S.parse_storage_url("https://myacct.blob.core.chinacloudapi.cn", "blob").account_name == "myacct"
        @test S.parse_storage_url("myacct.blob.core.windows.net", "blob").scheme == "https"
        @test_throws ArgumentError S.parse_storage_url("http://myacct.blob.core.windows.net", "blob")
        # plain HTTP is limited to loopback hosts and single-label (container) names
        @test S.parse_storage_url("http://localhost:10000/devstoreaccount1", "blob").path_style
        @test S.parse_storage_url("http://[::1]:10000/devstoreaccount1", "blob").path_style
        for h in ("127.evil.example", "127.0.0.1.nip.io", "storage.test", "10.0.0.5", "localhost.evil.example")
            @test_throws ArgumentError S.parse_storage_url("http://$h/devstoreaccount1", "blob")
        end
        @test_throws ArgumentError S.parse_storage_url("ftp://x", "blob")

        ep, cred = S.parse_storage_connection_string(
            "DefaultEndpointsProtocol=https;AccountName=acct;AccountKey=$(DEVKEY);EndpointSuffix=core.usgovcloudapi.net")
        @test ep == "https://acct.blob.core.usgovcloudapi.net"
        @test cred isa AzureNamedKeyCredential && named_key(cred).name == "acct"
        ep, cred = S.parse_storage_connection_string("BlobEndpoint=https://custom.example.com/;SharedAccessSignature=sv=1&sig=abc")
        @test ep == "https://custom.example.com" && cred isa AzureSasCredential
        ep, cred = S.parse_storage_connection_string("UseDevelopmentStorage=true")
        @test ep == "http://127.0.0.1:10000/devstoreaccount1"
        @test_throws ArgumentError S.parse_storage_connection_string("AccountKey=abc")

        @test S.normalize_credential("sv=2024&sig=abc&se=1", "a") isa AzureSasCredential
        @test S.normalize_credential("?sv=2024&sig=abc&se=1", "a") isa AzureSasCredential
        c = S.normalize_credential(DEVKEY, "acct")
        @test c isa AzureNamedKeyCredential && named_key(c).name == "acct"
        @test S.normalize_credential((account_name="n", account_key=DEVKEY), nothing) isa AzureNamedKeyCredential
        @test S.normalize_credential(Dict("account_name" => "n", "account_key" => DEVKEY), nothing) isa AzureNamedKeyCredential
        @test_throws ArgumentError S.normalize_credential(DEVKEY, nothing)
        @test_throws ArgumentError S.normalize_credential(42, "a")

        # tokens are never sent over plain HTTP to a remote host; SAS in URL + AzureSasCredential is ambiguous
        @test_throws ArgumentError BlobServiceClient("http://acct.blob.core.windows.net"; credential=FakeTokenCredential([]))
        # ... and over plain HTTP only to loopback, not to emulator container names
        @test_throws ArgumentError BlobServiceClient("http://azurite:10000/devstoreaccount1"; credential=FakeTokenCredential([]))
        @test BlobServiceClient("http://127.0.0.1:10000/devstoreaccount1"; credential=FakeTokenCredential([])) isa BlobServiceClient
        @test_throws ArgumentError BlobServiceClient("https://acct.blob.core.windows.net?sv=1&sig=x&se=2";
            credential=AzureSasCredential("sv=1&sig=y&se=2"))
        @test_throws ArgumentError BlobServiceClient("https://acct.blob.core.windows.net"; max_block_size=0)
        @test_throws ArgumentError BlobServiceClient("https://acct.blob.core.windows.net"; unknown_option=1)
        @test_throws ArgumentError BlobServiceClient("https://acct.blob.core.windows.net/container")

        bc = from_blob_url("https://acct.blob.core.windows.net/c/a/b%2Bc.txt?snapshot=2026-01-01T00%3A00%3A00.0000000Z&sv=1&sig=S3CR3T&se=2")
        @test bc.container_name == "c" && bc.blob_name == "a/b+c.txt"
        @test bc.snapshot == "2026-01-01T00:00:00.0000000Z"
        @test bc.url == "https://acct.blob.core.windows.net/c/a/b%2Bc.txt?snapshot=2026-01-01T00%3A00%3A00.0000000Z&sv=1&sig=S3CR3T&se=2"
        @test !occursin("S3CR3T", repr(bc))
        @test bc.primary_hostname == "acct.blob.core.windows.net"
        sb = get_blob_client(BlobServiceClient("https://acct.blob.core.windows.net"), "c", "x"; snapshot=Dict("snapshot" => "s1"))
        @test sb.snapshot == "s1" && endswith(sb.url, "/c/x?snapshot=s1")
        @test_throws ArgumentError from_blob_url("https://acct.blob.core.windows.net/onlycontainer")
        @test_throws ArgumentError get_blob_client(ContainerClient("https://a.blob.core.windows.net", "c"), "")
        @test :url in propertynames(bc)
    end

    @testset "Shared Key canonicalization" begin
        hdrs = ["x-ms-date" => "Mon, 05 Jan 2026 10:11:12 GMT", "X-Ms-Version" => "2024-08-04", "Content-Length" => "0",
            "x-ms-meta-a" => "  v  "]
        url = "https://acct.blob.core.windows.net/c/b%20x?comp=Metadata&B=2&a=1&a=0"
        sig = S.shared_key_signature("acct", DEVKEY, "PUT", url, hdrs)
        expected_to_sign = "PUT\n\n\n\n\n\n\n\n\n\n\n\nx-ms-date:Mon, 05 Jan 2026 10:11:12 GMT\nx-ms-meta-a:v\n" *
                           "x-ms-version:2024-08-04\n/acct/c/b%20x\na:0,1\nb:2\ncomp:Metadata"
        @test sig == "SharedKey acct:" * base64encode(SHA.hmac_sha256(base64decode(DEVKEY), expected_to_sign))
    end

    @testset "SAS layout" begin
        st, se = DateTime(2026, 1, 1), DateTime(2026, 1, 2)
        sv = Blob.SAS_VERSION
        sign(s) = base64encode(SHA.hmac_sha256(base64decode(DEVKEY), s))
        q(tok) = Dict(S.query_pairs(tok))
        tok = generate_blob_sas("acct", "cont", "dir/file.txt"; account_key=DEVKEY, permission=BlobSasPermissions(write=true, read=true),
            start=st, expiry=se, ip="1.2.3.4", protocol="https", content_type="text/plain")
        p = q(tok)
        @test p["sp"] == "rw" && p["sr"] == "b" && p["sv"] == sv && p["st"] == "2026-01-01T00:00:00Z" && p["rsct"] == "text/plain"
        @test p["sig"] == sign(join(["rw", "2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z", "/blob/acct/cont/dir/file.txt", "",
            "1.2.3.4", "https", sv, "b", "", "", "", "", "", "", "text/plain"], "\n"))
        @test occursin("st=2026-01-01T00%3A00%3A00Z", tok)
        # container SAS with a stored access policy only
        p = q(generate_container_sas("acct", "cont"; account_key=DEVKEY, policy_id="pol"))
        @test p["si"] == "pol" && !haskey(p, "sp") && p["sr"] == "c"
        @test p["sig"] == sign(join(["", "", "", "/blob/acct/cont", "pol", "", "", sv, "c", "", "", "", "", "", "", ""], "\n"))
        # snapshot SAS
        p = q(generate_blob_sas("acct", "cont", "b"; snapshot="snap1", account_key=DEVKEY, permission="r", expiry=se))
        @test p["sr"] == "bs" && p["snapshot"] == "snap1"
        # user delegation SAS
        udk = UserDelegationKey(signed_oid="oid", signed_tid="tid", signed_start="2026-01-01T00:00:00Z",
            signed_expiry="2026-01-03T00:00:00Z", signed_service="b", signed_version="2024-08-04", value=DEVKEY)
        @test !occursin(DEVKEY, repr(udk))
        p = q(generate_blob_sas("acct", "cont", "b"; user_delegation_key=udk, permission="r", expiry=se, correlation_id="corr"))
        @test p["skoid"] == "oid" && p["sktid"] == "tid" && p["sks"] == "b" && p["scid"] == "corr"
        @test p["sig"] == sign(join(["r", "", "2026-01-02T00:00:00Z", "/blob/acct/cont/b", "oid", "tid", "2026-01-01T00:00:00Z",
            "2026-01-03T00:00:00Z", "b", "2024-08-04", "", "", "corr", "", "", sv, "b", "", "", "", "", "", "", ""], "\n"))
        # account SAS
        p = q(generate_account_sas("acct", DEVKEY, "sco", "rwl", se))
        @test p["sp"] == "rwl" && p["srt"] == "sco" && p["ss"] == "b"
        @test p["sig"] == sign(join(["acct", "rwl", "b", "sco", "", "2026-01-02T00:00:00Z", "", "", sv, ""], "\n") * "\n")
        # permissions
        @test string(ContainerSasPermissions("lwr")) == "rwl"
        @test string(ContainerSasPermissions(read=true, add=true, create=true, write=true, delete=true,
            delete_previous_version=true, permanent_delete=true, list=true, tag=true, filter_by_tags=true, move=true,
            execute=true, set_immutability_policy=true)) == "racwdxyltfmei"
        @test string(AccountSasPermissions("icfptuacalyxdwr")) == "rwdxylacuptfi"
        @test_throws ArgumentError BlobSasPermissions("rl")
        @test string(ResourceTypes(object=true, service=true)) == "so"
        @test string(Services(blob=true, fileshare=true)) == "bf"
        @test_throws ArgumentError generate_blob_sas("a", "c", "b"; account_key=DEVKEY, user_delegation_key=udk,
            permission="r", expiry=se)
        @test_throws ArgumentError generate_blob_sas("a", "c", "b"; account_key=DEVKEY, permission="r", expiry=se,
            protocol="http")
    end

    ms = start_mock_storage()
    base = mockurl(ms)
    fast = (retry_backoff_factor=0.01, retry_backoff_max=0.05)
    try
        @testset "pipeline: headers, retries, errors" begin
            ms.handler = r -> HTTP.Response(200, ["x-ms-sku-name" => "Standard_LRS", "x-ms-account-kind" => "StorageV2"])
            svc = BlobServiceClient(base; credential=DEVKEY, user_agent="myapp/1.0", fast...)
            empty!(ms.requests)
            info = get_account_information(svc; timeout=7)
            @test info["sku_name"] == "Standard_LRS"
            r = ms.requests[1]
            @test r.method == "GET" && startswith(r.target, "/devstoreaccount1/?")
            @test occursin("restype=account", r.target) && occursin("timeout=7", r.target)
            @test r.headers["x-ms-version"] == S.DEFAULT_API_VERSION
            @test startswith(r.headers["authorization"], "SharedKey devstoreaccount1:")
            @test startswith(r.headers["user-agent"], "myapp/1.0 azsdk-julia-storage-blob/")
            @test haskey(r.headers, "x-ms-client-request-id") && haskey(r.headers, "x-ms-date")
            # the signature matches what the server received
            hdrs = [k => v for (k, v) in r.headers if k != "authorization"]
            @test r.headers["authorization"] == S.shared_key_signature("devstoreaccount1", DEVKEY, "GET",
                "http://127.0.0.1:$(ms.port)" * r.target, hdrs)

            # transient failures are retried (honouring Retry-After), then succeed
            n = Ref(0)
            ms.handler = r -> (n[] += 1; n[] <= 2 ? HTTP.Response(503, ["Retry-After" => "0"], "busy") : HTTP.Response(200))
            get_account_information(svc)
            @test n[] == 3
            # ...but not forever
            n[] = 0
            ms.handler = r -> (n[] += 1; HTTP.Response(500, ["x-ms-error-code" => "InternalError"],
                xml_error("InternalError", "Server failed.")))
            err = try
                get_account_information(svc)
            catch e
                e
            end
            @test err isa HttpResponseError && err.status_code == 500 && err.error_code == "InternalError"
            @test n[] == 4
            # client errors are not retried; codes decide the exception type
            for (status, code, T) in ((404, "BlobNotFound", ResourceNotFoundError), (409, "ContainerAlreadyExists", ResourceExistsError),
                (409, "LeaseAlreadyPresent", HttpResponseError), (412, "ConditionNotMet", ResourceModifiedError),
                (412, "LeaseIdMissing", HttpResponseError), (403, "AuthenticationFailed", ClientAuthenticationError),
                (403, "AuthorizationPermissionMismatch", ClientAuthenticationError), (400, "InvalidInput", HttpResponseError))
                n[] = 0
                ms.handler = r -> (n[] += 1; HTTP.Response(status, ["x-ms-error-code" => code, "Content-Type" => "application/xml"],
                    xml_error(code, "Something about $(code).")))
                e = try
                    get_account_information(svc)
                catch e
                    e
                end
                @test e isa T && e.error_code == code && e.status_code == status && n[] == 1
                @test occursin("Something about $(code).", e.message) && occursin("ErrorCode:$(code)", e.message)
                @test !occursin("RequestId", e.message)
            end
            # HEAD errors carry only the header
            ms.handler = r -> HTTP.Response(404, ["x-ms-error-code" => "BlobNotFound"])
            e = try
                get_blob_properties(get_blob_client(svc, "c", "b"))
            catch e
                e
            end
            @test e isa ResourceNotFoundError && e.error_code == StorageErrorCode.blob_not_found
            @test !exists(get_blob_client(svc, "c", "b"))
            ms.handler = r -> HTTP.Response(409, ["x-ms-error-code" => "BlobUsesCustomerSpecifiedEncryption"])
            @test exists(get_blob_client(svc, "c", "b"))
            # no status codes without code
            ms.handler = r -> HTTP.Response(304)
            @test_throws ResourceNotModifiedError get_account_information(svc)
            # redirects are never followed (credentials must not go elsewhere)
            ms.handler = r -> HTTP.Response(302, ["Location" => "http://127.0.0.2:1/steal"])
            e = try
                get_account_information(svc)
            catch e
                e
            end
            @test e isa HttpResponseError && e.status_code == 302
            # unreachable endpoints -> ServiceRequestError without leaking the SAS signature
            dead = BlobServiceClient("http://127.0.0.1:9/devstoreaccount1?sv=1&se=2&sig=TOPSECRET"; retry_total=1, fast...)
            e = try
                get_account_information(dead)
            catch e
                e
            end
            @test e isa ServiceRequestError && !occursin("TOPSECRET", e.message)
        end

        @testset "pipeline: SAS and bearer tokens" begin
            ms.handler = r -> HTTP.Response(200)
            empty!(ms.requests)
            c = ContainerClient(base, "c"; credential="sv=1&se=2&sig=abc%2B")
            get_container_properties(c)
            r = ms.requests[end]
            @test endswith(r.target, "restype=container&sv=1&se=2&sig=abc%2B")
            @test !haskey(r.headers, "authorization")
            cred = AzureSasCredential("sv=1&se=2&sig=one")
            c = ContainerClient(base, "c"; credential=cred)
            get_container_properties(c)
            update!(cred, "sv=1&se=3&sig=two")          # rotation is picked up
            get_container_properties(c)
            @test endswith(ms.requests[end-1].target, "sig=one") && endswith(ms.requests[end].target, "sig=two")

            # bearer tokens, audience and tenant discovery through a 401 challenge
            tc = FakeTokenCredential([])
            svc = BlobServiceClient(base; credential=tc, audience="https://acct.blob.core.windows.net", fast...)
            ms.handler = function (r)
                auth = get(r.headers, "authorization", "")
                auth == "Bearer tok-tenant2" && return HTTP.Response(200)
                return HTTP.Response(401, ["WWW-Authenticate" => "Bearer authorization_uri=https://login.microsoftonline.com/tenant2/oauth2/authorize resource_id=https://storage.azure.com",
                    "x-ms-error-code" => "InvalidAuthenticationInfo"])
            end
            get_account_information(svc)
            @test tc.calls[1].scopes == ["https://acct.blob.core.windows.net/.default"] && tc.calls[1].tenant_id === nothing
            @test tc.calls[end].tenant_id == "tenant2"
            # a persistent 401 is raised as an authentication error
            ms.handler = r -> HTTP.Response(401, ["x-ms-error-code" => "InvalidAuthenticationInfo"])
            @test_throws ClientAuthenticationError get_account_information(BlobServiceClient(base; credential=FakeTokenCredential([])))

            # user delegation key
            ms.handler = r -> HTTP.Response(200, ["Content-Type" => "application/xml"],
                """<?xml version="1.0" encoding="utf-8"?><UserDelegationKey><SignedOid>o</SignedOid><SignedTid>t</SignedTid>
                <SignedStart>2026-01-01T00:00:00Z</SignedStart><SignedExpiry>2026-01-02T00:00:00Z</SignedExpiry>
                <SignedService>b</SignedService><SignedVersion>2024-08-04</SignedVersion><Value>$(DEVKEY)</Value></UserDelegationKey>""")
            empty!(ms.requests)
            k = get_user_delegation_key(BlobServiceClient(base; credential=FakeTokenCredential([])), DateTime(2026, 1, 1),
                DateTime(2026, 1, 2))
            @test k.signed_oid == "o" && k.value == DEVKEY && k.signed_version == "2024-08-04"
            @test ms.requests[1].method == "POST"
            @test String(ms.requests[1].body) ==
                  "<?xml version=\"1.0\" encoding=\"utf-8\"?><KeyInfo><Start>2026-01-01T00:00:00Z</Start><Expiry>2026-01-02T00:00:00Z</Expiry></KeyInfo>"
            @test_throws ArgumentError get_user_delegation_key(BlobServiceClient(base; credential=DEVKEY), DateTime(2026), DateTime(2026))
        end

        @testset "downloads: etag pinning, truncation, MD5" begin
            data = rand(UInt8, 10_000)
            etag = Ref("\"v1\"")
            mode = Ref(:ok)
            ms.handler = function (r)
                m = match(r"bytes=(\d+)-(\d+)", get(r.headers, "x-ms-range", ""))
                a, b = parse(Int, m.captures[1]), min(parse(Int, m.captures[2]), length(data) - 1)
                im = get(r.headers, "if-match", nothing)
                if im !== nothing && im != etag[]
                    return HTTP.Response(412, ["x-ms-error-code" => "ConditionNotMet"], xml_error("ConditionNotMet", "changed"))
                end
                chunk = data[a+1:b+1]
                if mode[] === :short && a > 0
                    chunk = chunk[1:end-1]
                end
                hdrs = ["Content-Range" => "bytes $a-$b/$(length(data))", "ETag" => etag[], "x-ms-blob-type" => "BlockBlob",
                    "Content-Type" => "application/octet-stream"]
                if get(r.headers, "x-ms-range-get-content-md5", "") == "true"
                    push!(hdrs, "Content-MD5" => base64encode(mode[] === :badmd5 ? S.md5(UInt8[1]) : S.md5(chunk)))
                end
                return HTTP.Response(206, hdrs, chunk)
            end
            small = BlobClient(base, "c", "b"; credential=DEVKEY, max_single_get_size=4000, max_chunk_get_size=1000,
                retry_total=2, fast...)
            empty!(ms.requests)
            d = download_blob(small; max_concurrency=3)
            @test d.size == 10_000 && d.properties.etag == "\"v1\""
            @test readall(d) == data
            @test length(ms.requests) == 1 + 6
            @test ms.requests[1].headers["x-ms-range"] == "bytes=0-3999"
            @test all(r -> get(r.headers, "if-match", "") == "\"v1\"", ms.requests[2:end])
            # the blob changes while downloading -> ResourceModifiedError instead of a torn read
            d = download_blob(small)
            etag[] = "\"v2\""
            @test_throws ResourceModifiedError readall(d)
            etag[] = "\"v1\""
            # an explicit etag condition replaces the automatic pinning
            d = download_blob(small; etag="\"v1\"", match_condition=MatchConditions.IfNotModified)
            @test readall(d) == data
            # truncated range responses are retried, then reported
            mode[] = :short
            d = download_blob(small)
            @test_throws AzureCore.IncompleteReadError readall(d)
            # MD5 validation
            mode[] = :ok
            @test readall(download_blob(small; validate_content=true, max_concurrency=2)) == data
            mode[] = :badmd5
            @test_throws ContentValidationError download_blob(small; validate_content=true)
            mode[] = :ok
            # IO interface over the mock
            d = download_blob(small; offset=500, length=2500)
            @test d.properties.content_range == "bytes 500-2999/10000"
            @test read(d, 100) == data[501:600]
            @test collect(Iterators.take(chunks(d), 1))[1] == data[601:3000]
            @test eof(d)
        end

        @testset "uploads: request shapes" begin
            ms.handler = r -> HTTP.Response(r.method == "PUT" ? 201 : 200, ["ETag" => "\"e\""])
            bc = BlobClient(base, "c", "dir/ü b.bin"; credential=DEVKEY, max_single_put_size=1000, max_block_size=400, fast...)
            empty!(ms.requests)
            upload_blob(bc, rand(UInt8, 500); overwrite=false, metadata=Dict("m" => "1"), tags=Dict("t k" => "v&"),
                standard_blob_tier=StandardBlobTier.COOL, content_settings=ContentSettings(content_type="x/y"))
            r = ms.requests[1]
            @test r.method == "PUT" && startswith(r.target, "/devstoreaccount1/c/dir/%C3%BC%20b.bin")
            @test r.headers["x-ms-blob-type"] == "BlockBlob" && r.headers["if-none-match"] == "*"
            @test r.headers["x-ms-meta-m"] == "1" && r.headers["x-ms-tags"] == "t%20k=v%26"
            @test r.headers["x-ms-access-tier"] == "Cool" && r.headers["x-ms-blob-content-type"] == "x/y"
            @test length(r.body) == 500
            empty!(ms.requests)
            upload_blob(bc, rand(UInt8, 1500); overwrite=true, max_concurrency=2)
            puts = [r for r in ms.requests if occursin("comp=block&", r.target)]
            @test length(puts) == 4 && sort(length.(getfield.(puts, :body))) == [300, 400, 400, 400]
            commit = ms.requests[end]
            @test occursin("comp=blocklist", commit.target) && !haskey(commit.headers, "if-none-match")
            ids = [m.captures[1] for m in eachmatch(r"<Latest>([^<]+)</Latest>", String(commit.body))]
            staged = [S.unquote_url(match(r"blockid=([^&]+)", r.target).captures[1]) for r in puts]
            @test length(ids) == 4 && Set(ids) == Set(staged)
            @test issorted(Blob._decode_block_id.(ids))
            @test length(unique(length.(ids))) == 1
            # errors in parallel block uploads stop the upload
            ms.handler = r -> occursin("comp=block&", r.target) ?
                              HTTP.Response(400, ["x-ms-error-code" => "InvalidInput"], xml_error("InvalidInput", "bad")) :
                              HTTP.Response(201)
            empty!(ms.requests)
            @test_throws HttpResponseError upload_blob(bc, rand(UInt8, 3000); overwrite=true, max_concurrency=3)
            @test !any(r -> occursin("comp=blocklist", r.target), ms.requests)
            @test_throws ArgumentError upload_blob(bc, "x"; bogus=true)
            @test_throws ArgumentError upload_blob(bc, "x"; blob_type="Nope")
            @test_throws ArgumentError upload_blob(bc, "x"; metadata=Dict("bad name" => "v"))
            @test_throws ArgumentError upload_blob(bc, "x"; metadata=Dict("ok" => "line\nbreak"))
        end

        @testset "batch: no header injection" begin
            # the multipart body is assembled by hand, so line breaks must be refused before sending
            cc = ContainerClient(base, "c"; credential=DEVKEY, fast...)
            empty!(ms.requests)
            @test_throws ArgumentError delete_blobs(cc, Dict("name" => "a", "lease_id" => "x\r\nx-ms-evil: 1"))
            @test_throws ArgumentError set_standard_blob_tier_blobs(cc, "Hot\r\nx-ms-evil: 1", "a")
            @test_throws ArgumentError delete_blobs(cc, "a"; if_tags_match_condition="\"t\"='v'\nx: y")
            @test isempty(ms.requests)
        end
    finally
        stop(ms)
    end
end
