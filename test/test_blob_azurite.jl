# Integration tests for AzureSDK.Storage.Blob against Azurite, the local
# Azure Storage emulator (which validates Shared Key and SAS signatures).
# Enable with AZURESDK_AZURITE=1; see test/emulator/azurite-compose.yml.

using AzureSDK
using AzureSDK.Storage.Blob
using AzureSDK.AzureCore: AzureNamedKeyCredential
using Dates
using Test
import UUIDs

# AZURESDK_AZURITE_CONNECTION_STRING points the suite at another account (with
# an account key), e.g. a real storage account.
const AZURITE_CONN = get(ENV, "AZURESDK_AZURITE_CONNECTION_STRING", "UseDevelopmentStorage=true")
const AZURITE_URL, AZURITE_ACCOUNT, AZURITE_KEY = let (url, cred) = AzureSDK.Storage.parse_storage_connection_string(AZURITE_CONN)
    nk = AzureSDK.AzureCore.named_key(cred)
    url, nk.name, nk.key
end
const AZURITE_LOCAL = startswith(AZURITE_URL, "http://")

"Poll `f` until it returns true (Azure updates some indexes, e.g. blob tags, asynchronously)."
function eventually(f; timeout = 60)
    t0 = time()
    while !f()
        time() - t0 > timeout && return false
        sleep(2)
    end
    return true
end

_uniq(prefix) = string(prefix, "-", lowercase(string(rand(UInt32); base = 36)), lowercase(string(rand(UInt32); base = 36)))

if get(ENV, "AZURESDK_AZURITE", "") == "1"
    @testset "Blob Storage (Azurite)" begin
        svc = from_connection_string(BlobServiceClient, AZURITE_CONN)
        created = String[]
        newcontainer(prefix = "c"; kwargs...) = (
            name = _uniq(prefix); push!(created, name);
            create_container(svc, name; kwargs...)
        )
        try
            @testset "clients and URLs" begin
                @test svc.account_name == AZURITE_ACCOUNT
                @test svc.primary_endpoint == AZURITE_URL
                @test !occursin(AZURITE_KEY, repr(svc)) && !occursin(AZURITE_KEY, repr(svc.credential))
                cc = from_connection_string(ContainerClient, AZURITE_CONN, "abc")
                @test cc.url == AZURITE_URL * "/abc"
                bc = from_connection_string(BlobClient, AZURITE_CONN, "abc", "d/e f.txt")
                @test bc.url == AZURITE_URL * "/abc/d/e%20f.txt"
                bc2 = from_blob_url(bc.url; credential = AzureNamedKeyCredential(AZURITE_ACCOUNT, AZURITE_KEY))
                @test bc2.container_name == "abc" && bc2.blob_name == "d/e f.txt"
                @test from_container_url(cc.url).container_name == "abc"
                # account key given as a plain string (Python semantics)
                svc2 = BlobServiceClient(AZURITE_URL; credential = AZURITE_KEY)
                @test svc2.credential isa AzureNamedKeyCredential
                info = get_account_information(svc)
                @test haskey(info, "sku_name") && haskey(info, "account_kind")
            end

            @testset "containers" begin
                name = _uniq("cont")
                cc = get_container_client(svc, name)
                @test !exists(cc)
                r = create_container(cc; metadata = Dict("owner" => "tests", "Mixed" => "Case"))
                push!(created, name)
                @test r["etag"] isa String && r["last_modified"] isa DateTime
                @test exists(cc)
                err = try
                    create_container(cc)
                    nothing
                catch e
                    e
                end
                @test err isa ResourceExistsError
                @test err.error_code == StorageErrorCode.container_already_exists
                p = get_container_properties(cc)
                @test p.name == name
                @test p.metadata["owner"] == "tests"
                @test p.metadata["Mixed"] == "Case"
                @test p.metadata["OWNER"] == "tests"      # metadata names are case-insensitive
                set_container_metadata(cc, Dict("a" => "1"))
                @test get_container_properties(cc).metadata == Dict("a" => "1")
                @test_throws ResourceNotFoundError get_container_properties(get_container_client(svc, _uniq("missing")))

                # listing with a prefix, metadata and pagination
                prefix = _uniq("list")
                for i in 1:5
                    push!(created, "$(prefix)-$(i)")
                    create_container(svc, "$(prefix)-$(i)"; metadata = Dict("i" => string(i)))
                end
                all_ = collect(list_containers(svc; name_starts_with = prefix, include_metadata = true))
                @test [c.name for c in all_] == ["$(prefix)-$(i)" for i in 1:5]
                @test all_[3].metadata["i"] == "3"
                pages = collect(by_page(list_containers(svc; name_starts_with = prefix, results_per_page = 2)))
                @test length.(pages) == [2, 2, 1]
                pi = by_page(list_containers(svc; name_starts_with = prefix, results_per_page = 2))
                first_page, _ = iterate(pi)
                token = pi.continuation_token
                @test token !== nothing
                rest = collect(
                    by_page(
                        list_containers(svc; name_starts_with = prefix, results_per_page = 2);
                        continuation_token = token
                    )
                )
                @test [c.name for c in vcat(first_page, rest...)] == [c.name for c in all_]

                delete_container(svc, "$(prefix)-5")
                @test !exists(get_container_client(svc, "$(prefix)-5"))
            end

            @testset "access policies" begin
                cc = newcontainer("acl")
                exp = Dates.now(Dates.UTC) + Dates.Hour(1)
                set_container_access_policy(
                    cc, Dict(
                        "read" => AccessPolicy(
                            permission = ContainerSasPermissions(read = true, list = true),
                            expiry = exp, start = Dates.now(Dates.UTC) - Dates.Minute(5)
                        ),
                        "write" => AccessPolicy(permission = "w", expiry = exp)
                    );
                    # new Azure accounts disallow anonymous access, so only Azurite tests public containers
                    public_access = AZURITE_LOCAL ? PublicAccess.BLOB : nothing
                )
                acl = get_container_access_policy(cc)
                @test acl["public_access"] == (AZURITE_LOCAL ? PublicAccess.BLOB : nothing)
                ids = Dict(si.id => si.access_policy for si in acl["signed_identifiers"])
                @test sort(collect(keys(ids))) == ["read", "write"]
                @test ids["read"].permission == "rl"
                @test abs(ids["read"].expiry - floor(exp, Dates.Second)) < Dates.Second(2)
                @test_throws ArgumentError set_container_access_policy(cc, Dict(string(i) => AccessPolicy() for i in 1:6))
                # anonymous read of a blob in a public container
                if AZURITE_LOCAL
                    upload_blob(cc, "public.txt", "anyone can read")
                    anon = BlobClient(AZURITE_URL, cc.container_name, "public.txt")
                    @test anon.credential === nothing
                    @test String(readall(download_blob(anon))) == "anyone can read"
                end
            end

            @testset "upload and download" begin
                cc = newcontainer("data")
                # string, bytes, IO, empty
                bc = upload_blob(
                    cc, "a.txt", "hello"; content_settings = ContentSettings(
                        content_type = "text/plain",
                        content_language = "en", cache_control = "no-cache"
                    ), metadata = Dict("k" => "v")
                )
                @test bc isa BlobClient
                @test String(readall(download_blob(bc))) == "hello"
                @test readall(download_blob(bc; encoding = "UTF-8")) == "hello"
                @test content_as_text(download_blob(cc, "a.txt")) == "hello"
                p = get_blob_properties(bc)
                @test p.size == 5 && p.blob_type == BlobType.BLOCKBLOB
                @test p.content_settings.content_type == "text/plain"
                @test p.content_settings.content_language == "en"
                @test p.content_settings.cache_control == "no-cache"
                @test p.content_settings.content_md5 == Storage.md5(Vector{UInt8}("hello"))
                @test p.metadata == Dict("k" => "v")
                @test p.etag !== nothing && p.last_modified isa DateTime

                bytes = rand(UInt8, 1000)
                upload_blob(cc, "b.bin", bytes)
                @test readall(download_blob(cc, "b.bin")) == bytes
                upload_blob(cc, "io.bin", IOBuffer(bytes))
                @test readall(download_blob(cc, "io.bin")) == bytes
                upload_blob(cc, "empty", UInt8[])
                d = download_blob(cc, "empty")
                @test d.size == 0 && readall(d) == UInt8[] && eof(d)
                @test get_blob_properties(get_blob_client(cc, "empty")).size == 0

                # overwrite semantics
                err = try
                    upload_blob(cc, "a.txt", "again")
                    nothing
                catch e
                    e
                end
                @test err isa ResourceExistsError && err.error_code == StorageErrorCode.blob_already_exists
                upload_blob(cc, "a.txt", "again"; overwrite = true)
                @test String(readall(download_blob(bc))) == "again"
                @test exists(bc)
                @test !exists(get_blob_client(cc, "nope"))

                # ranged downloads
                d = download_blob(cc, "b.bin"; offset = 100, length = 50)
                @test d.size == 50 && readall(d) == bytes[101:150]
                @test readall(download_blob(cc, "b.bin"; offset = 990)) == bytes[991:1000]
                @test_throws ArgumentError download_blob(cc, "b.bin"; length = 5)

                # validate_content
                @test readall(download_blob(cc, "b.bin"; validate_content = true)) == bytes
                upload_blob(cc, "v.bin", bytes; validate_content = true)
                @test readall(download_blob(cc, "v.bin")) == bytes
            end

            @testset "chunked parallel transfers" begin
                small = Dict(
                    :max_single_put_size => 1024 * 1024, :max_block_size => 256 * 1024,
                    :max_single_get_size => 300 * 1024, :max_chunk_get_size => 200 * 1024
                )
                csvc = from_connection_string(BlobServiceClient, AZURITE_CONN; small...)
                name = _uniq("big")
                push!(created, name)
                cc = create_container(csvc, name)
                data = rand(UInt8, 3 * 1024 * 1024 + 12345)
                progress = Tuple{Int, Any}[]
                lk = ReentrantLock()
                bc = upload_blob(
                    cc, "big.bin", data; max_concurrency = 4,
                    progress_hook = (cur, tot) -> @lock(lk, push!(progress, (cur, tot)))
                )
                committed, uncommitted = get_block_list(bc)
                @test length(committed) == cld(length(data), 256 * 1024)
                @test isempty(uncommitted)
                @test maximum(first.(progress)) == length(data)
                @test get_blob_properties(bc).size == length(data)
                # parallel readall, sequential readall, readinto, chunks and IO reads
                @test readall(download_blob(bc; max_concurrency = 4)) == data
                @test readall(download_blob(bc)) == data
                io = IOBuffer()
                @test readinto(download_blob(bc; max_concurrency = 3), io) == length(data)
                @test take!(io) == data
                d = download_blob(bc)
                parts = collect(chunks(d))
                @test length(parts[1]) == 300 * 1024 && length(parts[2]) == 200 * 1024
                @test reduce(vcat, parts) == data
                d = download_blob(bc)
                @test read(d, 10) == data[1:10]
                @test read(d, UInt8) == data[11]
                @test position(d) == 11
                @test readall(d) == data[12:end]
                @test eof(d)
                d = download_blob(bc; offset = 250_000, length = 700_000, max_concurrency = 4)
                @test read(d) == data[250_001:950_000]
                @test readall(download_blob(bc; validate_content = true, max_concurrency = 2)) == data
                # unknown-length stream (a pipe) is uploaded in blocks
                p = Pipe()
                Base.link_pipe!(p; reader_supports_async = true, writer_supports_async = true)
                writer = @async begin
                    write(p.in, data)
                    close(p.in)
                end
                upload_blob(cc, "stream.bin", p; max_concurrency = 2)
                wait(writer)
                @test readall(download_blob(cc, "stream.bin")) == data
                # a tiny stream becomes a single Put Blob
                p = Pipe()
                Base.link_pipe!(p; reader_supports_async = true, writer_supports_async = true)
                write(p.in, "tiny")
                close(p.in)
                upload_blob(cc, "tiny", p)
                @test String(readall(download_blob(cc, "tiny"))) == "tiny"
                @test get_block_list(get_blob_client(cc, "tiny"))[1] == BlobBlock[]
                # a file on disk
                path, fio = mktemp()
                write(fio, data)
                close(fio)
                open(path) do f
                    upload_blob(cc, "file.bin", f; max_concurrency = 4)
                end
                open(path * ".out", "w") do f
                    readinto(download_blob(cc, "file.bin"; max_concurrency = 4), f)
                end
                @test read(path * ".out") == data
                rm(path)
                rm(path * ".out")
                # concurrent independent uploads share the client
                @sync for i in 1:20
                    Threads.@spawn upload_blob(cc, "many/$(i)", "blob $(i)")
                end
                @test length(collect(list_blob_names(cc; name_starts_with = "many/"))) == 20
            end

            @testset "properties, metadata, tags and conditions" begin
                cc = newcontainer("props")
                bc = get_blob_client(cc, "p.txt")
                r = upload_blob(bc, "x")
                set_blob_metadata(bc, Dict("one" => "1", "Two" => "2"))
                @test get_blob_properties(bc).metadata == Dict("one" => "1", "Two" => "2")
                set_blob_metadata(bc)
                @test isempty(get_blob_properties(bc).metadata)
                set_http_headers(bc, ContentSettings(content_type = "application/json", content_disposition = "inline"))
                p = get_blob_properties(bc)
                @test p.content_settings.content_type == "application/json"
                @test p.content_settings.content_disposition == "inline"

                # etag conditions
                etag = p.etag
                @test get_blob_properties(bc; etag = etag, match_condition = MatchConditions.IfNotModified).etag == etag
                @test_throws ArgumentError get_blob_properties(bc; etag = etag)
                upload_blob(bc, "y"; overwrite = true, etag = etag, match_condition = MatchConditions.IfNotModified)
                err = try
                    upload_blob(bc, "z"; overwrite = true, etag = etag, match_condition = MatchConditions.IfNotModified)
                    nothing
                catch e
                    e
                end
                @test err isa ResourceModifiedError && err.status_code == 412
                @test err.error_code == StorageErrorCode.condition_not_met
                @test_throws ResourceModifiedError download_blob(bc; if_unmodified_since = Dates.now(Dates.UTC) - Dates.Day(1))
                @test String(readall(download_blob(bc; if_modified_since = Dates.now(Dates.UTC) - Dates.Day(1)))) == "y"

                # tags
                set_blob_tags(bc, Dict("project" => "alpha", "stage" => "1"))
                @test get_blob_tags(bc) == Dict("project" => "alpha", "stage" => "1")
                @test get_blob_properties(bc).tag_count == 2
                bc2 = upload_blob(cc, "t2.txt", "t"; tags = Dict("project" => "beta"))
                @test get_blob_tags(bc2) == Dict("project" => "beta")
                tagged = collect(list_blobs(cc; include = ["tags"]))
                @test Dict(b.name => b.tags for b in tagged)["p.txt"]["project"] == "alpha"
                @test eventually(() -> !isempty(collect(find_blobs_by_tags(cc, "\"project\"='alpha'"))))
                found = collect(find_blobs_by_tags(cc, "\"project\"='alpha'"))
                @test [f.name for f in found] == ["p.txt"]
                @test found[1].container_name == cc.container_name
                @test eventually(() -> !isempty(collect(find_blobs_by_tags(svc, "\"project\"='beta'"))))
                @test get_blob_properties(bc; if_tags_match_condition = "\"project\"='alpha'").name == "p.txt"
                @test_throws ResourceModifiedError get_blob_properties(bc; if_tags_match_condition = "\"project\"='zeta'")
                set_blob_tags(bc)
                @test isempty(get_blob_tags(bc))

                # tiers
                set_standard_blob_tier(bc, StandardBlobTier.COOL)
                @test get_blob_properties(bc).blob_tier == "Cool"
                set_standard_blob_tier(bc, "Hot")
                @test get_blob_properties(bc).blob_tier == "Hot"

                @test_throws ArgumentError get_blob_properties(bc; bogus = 1)
            end

            @testset "snapshots, delete and listing" begin
                cc = newcontainer("snap")
                bc = upload_blob(cc, "s.txt", "v1")
                snap = create_snapshot(bc; metadata = Dict("snap" => "yes"))
                @test snap["snapshot"] isa String
                upload_blob(bc, "v2"; overwrite = true)
                sc = get_blob_client(cc, "s.txt"; snapshot = snap["snapshot"])
                @test String(readall(download_blob(sc))) == "v1"
                @test get_blob_properties(sc).metadata == Dict("snap" => "yes")
                @test String(readall(download_blob(bc))) == "v2"
                listed = collect(list_blobs(cc; include = ["snapshots"]))
                @test length(listed) == 2
                @test count(b -> b.snapshot !== nothing, listed) == 1
                @test_throws HttpResponseError delete_blob(bc)
                delete_blob(sc)
                @test length(collect(list_blobs(cc; include = "snapshots"))) == 1
                create_snapshot(bc)
                delete_blob(bc; delete_snapshots = "only")
                @test length(collect(list_blobs(cc; include = "snapshots"))) == 1
                create_snapshot(bc)
                delete_blob(cc, "s.txt"; delete_snapshots = "include")
                @test isempty(collect(list_blobs(cc; include = "snapshots")))

                # hierarchical listing
                for n in ("a/1", "a/2", "a/b/3", "c/4", "top")
                    upload_blob(cc, n, n)
                end
                top = collect(walk_blobs(cc))
                @test [x.name for x in top] == ["a/", "c/", "top"]
                @test top[1] isa BlobPrefix && top[3] isa BlobProperties
                inner = collect(top[1])
                @test [x.name for x in inner] == ["a/b/", "a/1", "a/2"]
                @test [x.name for x in collect(inner[1])] == ["a/b/3"]
                @test collect(list_blob_names(cc; name_starts_with = "a/")) == ["a/1", "a/2", "a/b/3"]
                @test length(collect(by_page(list_blobs(cc; results_per_page = 2)))) == 3
            end

            @testset "leases" begin
                cc = newcontainer("lease")
                bc = upload_blob(cc, "l.txt", "leased")
                lease = acquire_lease(bc; lease_duration = 15)
                @test lease isa BlobLeaseClient && !isempty(lease.id)
                @test get_blob_properties(bc).lease.state == "leased"
                @test_throws HttpResponseError upload_blob(bc, "no lease"; overwrite = true)
                upload_blob(bc, "with lease"; overwrite = true, lease = lease)
                set_blob_metadata(bc, Dict("x" => "1"); lease = lease.id)
                renew(lease)
                newid = string(UUIDs.uuid4())
                change(lease, newid)
                @test lease.id == newid
                release(lease)
                @test get_blob_properties(bc).lease.state == "available"
                lease2 = acquire_lease(bc)
                @test get_blob_properties(bc).lease.duration == "infinite"
                @test break_lease(lease2; lease_break_period = 0) == 0
                # container leases
                cl = acquire_lease(cc; lease_duration = -1)
                @test get_container_properties(cc).lease.state == "leased"
                @test_throws HttpResponseError delete_container(cc)
                release(cl)
            end

            @testset "block, page and append blobs" begin
                cc = newcontainer("kinds")
                bc = get_blob_client(cc, "blocks")
                stage_block(bc, "1", "Hello, ")
                stage_block(bc, "2", "World")
                stage_block(bc, "3", "!"; validate_content = true)
                committed, uncommitted = get_block_list(bc, "all")
                @test isempty(committed)
                @test sort([b.id for b in uncommitted]) == ["1", "2", "3"]
                commit_block_list(bc, [BlobBlock("1"), BlobBlock("2"), "3"]; metadata = Dict("m" => "1"))
                @test String(readall(download_blob(bc))) == "Hello, World!"
                committed, _ = get_block_list(bc)
                @test [(b.id, b.size) for b in committed] == [("1", 7), ("2", 5), ("3", 1)]
                @test committed[1].state == BlockState.COMMITTED

                pb = get_blob_client(cc, "pages")
                create_page_blob(pb, 4096; sequence_number = 7)
                upload_page(pb, fill(0x01, 512), 0, 512)
                upload_page(pb, fill(0x02, 1024), 2048, 1024; validate_content = true)
                filled, cleared = get_page_ranges(pb)
                @test [(r.start, r.stop) for r in filled] == [(0, 511), (2048, 3071)]
                @test isempty(cleared)
                clear_page(pb, 2048, 512)
                filled, _ = get_page_ranges(pb)
                @test [(r.start, r.stop) for r in filled] == [(0, 511), (2560, 3071)]
                @test [(r.start, r.stop) for r in list_page_ranges(pb)] == [(0, 511), (2560, 3071)]
                content = readall(download_blob(pb))
                @test length(content) == 4096 && all(==(0x01), content[1:512]) && all(==(0x02), content[2561:3072])
                @test all(iszero, content[513:2560])
                resize_blob(pb, 8192)
                @test get_blob_properties(pb).size == 8192
                set_sequence_number(pb, SequenceNumberAction.UPDATE, 42)
                @test get_blob_properties(pb).page_blob_sequence_number == 42
                @test_throws ArgumentError upload_page(pb, zeros(UInt8, 100), 0, 100)
                # page blob upload skips zero pages
                data = zeros(UInt8, 3 * 512)
                data[513:1024] .= 0x07
                pcc = get_container_client(
                    from_connection_string(BlobServiceClient, AZURITE_CONN; max_page_size = 512),
                    cc.container_name
                )
                upload_blob(pcc, "pages2", data; blob_type = BlobType.PAGEBLOB, max_concurrency = 2)
                filled, _ = get_page_ranges(get_blob_client(cc, "pages2"))
                @test [(r.start, r.stop) for r in filled] == [(512, 1023)]
                @test readall(download_blob(cc, "pages2")) == data
                @test_throws ArgumentError upload_blob(cc, "pages3", zeros(UInt8, 100); blob_type = BlobType.PAGEBLOB)

                ab = get_blob_client(cc, "append")
                create_append_blob(ab)
                r1 = append_block(ab, "line 1\n")
                r2 = append_block(ab, "line 2\n"; appendpos_condition = 7)
                @test r1["blob_append_offset"] == 0 && r2["blob_append_offset"] == 7
                @test_throws ResourceModifiedError append_block(ab, "x"; appendpos_condition = 0)
                @test_throws ResourceModifiedError append_block(ab, "x"; maxsize_condition = 10)
                # upload_blob appends to existing append blobs and creates missing ones
                upload_blob(ab, "line 3\n"; blob_type = BlobType.APPENDBLOB)
                @test String(readall(download_blob(ab))) == "line 1\nline 2\nline 3\n"
                @test get_blob_properties(ab).append_blob_committed_block_count == 3
                upload_blob(cc, "append2", "fresh"; blob_type = "AppendBlob")
                @test String(readall(download_blob(cc, "append2"))) == "fresh"
                upload_blob(cc, "append2", "new"; blob_type = BlobType.APPENDBLOB, overwrite = true)
                @test String(readall(download_blob(cc, "append2"))) == "new"
                upload_blob(cc, "append3", ""; blob_type = BlobType.APPENDBLOB)
                @test get_blob_properties(get_blob_client(cc, "append3")).blob_type == BlobType.APPENDBLOB
            end

            @testset "copy" begin
                cc = newcontainer("copy")
                src = upload_blob(cc, "src.txt", "copy me"; metadata = Dict("from" => "src"))
                dst = get_blob_client(cc, "dst.txt")
                r = start_copy_from_url(dst, src.url)
                @test r["copy_id"] isa String
                for _ in 1:50
                    get_blob_properties(dst).copy.status == "pending" || break
                    sleep(0.1)
                end
                p = get_blob_properties(dst)
                @test p.copy.status == "success" && p.copy.id == r["copy_id"]
                @test String(readall(download_blob(dst))) == "copy me"
                @test p.metadata == Dict("from" => "src")
                r = start_copy_from_url(get_blob_client(cc, "dst2.txt"), src.url; metadata = Dict("new" => "meta"))
                @test get_blob_properties(get_blob_client(cc, "dst2.txt")).metadata == Dict("new" => "meta")
            end

            @testset "batch" begin
                cc = newcontainer("batch")
                for i in 1:5
                    upload_blob(cc, "b$(i)", "x")
                end
                upload_blob(cc, "c1", "x")
                res = delete_blobs(cc, "b1", "b2", Dict("name" => "b3"))
                @test [r.status_code for r in res] == [202, 202, 202]
                @test [r.blob_name for r in res] == ["b1", "b2", "b3"]
                @test sort(collect(list_blob_names(cc))) == ["b4", "b5", "c1"]
                err = try
                    delete_blobs(cc, "b4", "missing")
                    nothing
                catch e
                    e
                end
                @test err isa PartialBatchErrorException
                @test [p.status_code for p in err.parts] == [202, 404]
                res = delete_blobs(cc, "missing2"; raise_on_any_failure = false)
                @test res[1].status_code == 404
                res = set_standard_blob_tier_blobs(cc, StandardBlobTier.COOL, "b5", "c1")
                @test all(r -> r.status_code in (200, 202), res)
                @test get_blob_properties(get_blob_client(cc, "c1")).blob_tier == "Cool"
            end

            @testset "shared access signatures" begin
                cc = newcontainer("sas")
                upload_blob(cc, "secret.txt", "classified")
                exp = Dates.now(Dates.UTC) + Dates.Hour(1)
                # account SAS
                tok = generate_account_sas(
                    AZURITE_ACCOUNT, AZURITE_KEY, ResourceTypes(service = true, container = true, object = true),
                    AccountSasPermissions(read = true, list = true), exp
                )
                @test occursin("sig=", tok) && occursin("sv=", tok)
                sas_svc = BlobServiceClient(AZURITE_URL * "?" * tok)
                @test sas_svc.credential === nothing
                @test cc.container_name in [c.name for c in list_containers(sas_svc; name_starts_with = "sas")]
                @test String(readall(download_blob(get_blob_client(sas_svc, cc.container_name, "secret.txt")))) == "classified"
                @test_throws ClientAuthenticationError upload_blob(get_blob_client(sas_svc, cc.container_name, "w.txt"), "no")
                @test !occursin("sig=", repr(sas_svc))
                # the same token as a credential (string and AzureSasCredential)
                @test length(collect(list_blobs(ContainerClient(AZURITE_URL, cc.container_name; credential = tok)))) == 1
                cred = AzureSasCredential(tok)
                @test exists(BlobClient(AZURITE_URL, cc.container_name, "secret.txt"; credential = cred))
                @test_throws ArgumentError BlobServiceClient(AZURITE_URL * "?" * tok; credential = cred)

                # container SAS
                ctok = generate_container_sas(
                    AZURITE_ACCOUNT, cc.container_name; account_key = AZURITE_KEY,
                    permission = ContainerSasPermissions(read = true, list = true, write = true, create = true), expiry = exp
                )
                ccs = from_container_url(cc.url * "?" * ctok)
                @test [b.name for b in list_blobs(ccs)] == ["secret.txt"]
                upload_blob(ccs, "via-sas.txt", "written with a container SAS")
                @test String(readall(download_blob(cc, "via-sas.txt"))) == "written with a container SAS"
                @test_throws ClientAuthenticationError delete_blob(ccs, "via-sas.txt")

                # blob SAS (read only, with response header overrides)
                btok = generate_blob_sas(
                    AZURITE_ACCOUNT, cc.container_name, "secret.txt"; account_key = AZURITE_KEY,
                    permission = BlobSasPermissions(read = true), expiry = Dates.Hour(1), content_type = "text/x-custom",
                    protocol = "https,http"
                )
                bsc = from_blob_url(get_blob_client(cc, "secret.txt").url * "?" * btok)
                d = download_blob(bsc)
                @test String(readall(d)) == "classified"
                @test d.properties.content_settings.content_type == "text/x-custom"
                @test_throws ClientAuthenticationError upload_blob(bsc, "overwrite"; overwrite = true)
                @test_throws ClientAuthenticationError download_blob(from_blob_url(get_blob_client(cc, "via-sas.txt").url * "?" * btok))

                # stored access policy
                set_container_access_policy(cc, Dict("readers" => AccessPolicy(permission = "rl", expiry = exp)))
                ptok = generate_container_sas(
                    AZURITE_ACCOUNT, cc.container_name; account_key = AZURITE_KEY,
                    policy_id = "readers"
                )
                @test length(collect(list_blobs(from_container_url(cc.url * "?" * ptok)))) == 2

                # snapshot SAS
                snap = create_snapshot(get_blob_client(cc, "secret.txt"))["snapshot"]
                stok = generate_blob_sas(
                    AZURITE_ACCOUNT, cc.container_name, "secret.txt"; snapshot = snap,
                    account_key = AZURITE_KEY, permission = "r", expiry = exp
                )
                ssc = BlobClient(AZURITE_URL, cc.container_name, "secret.txt"; snapshot = snap, credential = stok)
                @test String(readall(download_blob(ssc))) == "classified"

                # expired and tampered tokens
                old = generate_blob_sas(
                    AZURITE_ACCOUNT, cc.container_name, "secret.txt"; account_key = AZURITE_KEY,
                    permission = "r", expiry = Dates.now(Dates.UTC) - Dates.Hour(1)
                )
                @test_throws ClientAuthenticationError download_blob(
                    BlobClient(
                        AZURITE_URL, cc.container_name,
                        "secret.txt"; credential = old
                    )
                )
                @test_throws ArgumentError generate_blob_sas("a", "c", "b"; account_key = AZURITE_KEY)
                @test_throws ArgumentError generate_blob_sas("a", "c", "b"; permission = "r", expiry = exp)
            end

            @testset "service properties" begin
                props = get_service_properties(svc)
                @test haskey(props, "cors") && haskey(props, "hour_metrics")
                set_service_properties(
                    svc; cors = [
                        CorsRule(
                            ["https://example.com"], ["GET", "PUT"];
                            allowed_headers = ["x-ms-meta-*"], max_age_in_seconds = 60
                        ),
                    ]
                )
                props = get_service_properties(svc)
                @test length(props["cors"]) == 1
                @test props["cors"][1].allowed_origins == ["https://example.com"]
                @test props["cors"][1].allowed_methods == ["GET", "PUT"]
                @test props["cors"][1].max_age_in_seconds == 60
                set_service_properties(svc; cors = CorsRule[])
                @test isempty(get_service_properties(svc)["cors"])
            end

            @testset "Event Hubs BlobCheckpointStore" begin
                cc = newcontainer("ehcp")
                store = from_connection_string(AzureSDK.EventHubs.BlobCheckpointStore, AZURITE_CONN, cc.container_name)
                EH = AzureSDK.EventHubs
                fq, eh, cg = "myns.servicebus.windows.net", "EH1", "\$Default"
                o = Dict{String, Any}(
                    "fully_qualified_namespace" => fq, "eventhub_name" => eh, "consumer_group" => cg,
                    "partition_id" => "0", "owner_id" => "me", "etag" => nothing
                )
                c = EH.claim_ownership(store, [o])
                @test length(c) == 1 && startswith(c[1]["etag"], "\"") && c[1]["last_modified_time"] > 0
                @test isempty(EH.claim_ownership(store, [o]))                       # already exists
                stolen = EH.claim_ownership(store, [merge(c[1], Dict("owner_id" => "other"))])
                @test length(stolen) == 1
                @test isempty(EH.claim_ownership(store, [merge(c[1], Dict("owner_id" => "late"))]))   # stale etag
                listed = EH.list_ownership(store, fq, eh, cg)
                @test length(listed) == 1 && listed[1]["owner_id"] == "other" && listed[1]["partition_id"] == "0"
                @test listed[1]["etag"] == stolen[1]["etag"]
                # the layout shared with the other SDKs (lower-cased prefix, metadata names)
                @test collect(list_blob_names(cc)) == ["myns.servicebus.windows.net/eh1/\$default/ownership/0"]
                EH.update_checkpoint(
                    store, Dict{String, Any}(
                        "fully_qualified_namespace" => fq, "eventhub_name" => eh,
                        "consumer_group" => cg, "partition_id" => "0", "offset" => "42", "sequence_number" => 7
                    )
                )
                cp = EH.list_checkpoints(store, fq, eh, cg)
                @test cp[1]["offset"] == "42" && cp[1]["sequence_number"] == 7 && cp[1]["partition_id"] == "0"
                md = get_blob_properties(get_blob_client(cc, "myns.servicebus.windows.net/eh1/\$default/checkpoint/0")).metadata
                @test md == Dict("offset" => "42", "sequencenumber" => "7")
            end

            @testset "errors" begin
                bad = BlobServiceClient(
                    AZURITE_URL; credential = AzureNamedKeyCredential(
                        AZURITE_ACCOUNT,
                        "AAAA" * AZURITE_KEY[5:end]
                    )
                )
                err = try
                    collect(list_containers(bad))
                    nothing
                catch e
                    e
                end
                @test err isa ClientAuthenticationError
                @test err.status_code == 403
                @test !occursin(AZURITE_KEY, sprint(showerror, err))
                @test_throws ResourceNotFoundError download_blob(get_blob_client(svc, _uniq("nope"), "x"))
                e2 = try
                    get_blob_properties(get_blob_client(svc, created[1], "missing"))
                catch e
                    e
                end
                @test e2 isa ResourceNotFoundError && e2.error_code == StorageErrorCode.blob_not_found
                # customer-provided keys are refused over plain HTTP
                AZURITE_LOCAL && @test_throws ArgumentError upload_blob(
                    get_blob_client(svc, created[1], "cpk"), "x";
                    cpk = CustomerProvidedEncryptionKey(rand(UInt8, 32))
                )
                # unreachable endpoint -> ServiceRequestError after retries
                dead = BlobServiceClient(
                    "http://127.0.0.1:9/devstoreaccount1"; credential = AZURITE_KEY, retry_total = 1,
                    retry_backoff_factor = 0.01, connection_timeout = 2
                )
                @test_throws ServiceRequestError get_account_information(dead)
            end
        finally
            for name in created
                try
                    delete_container(svc, name)
                catch
                end
            end
        end
    end
else
    @info "Skipping Blob Storage integration tests (set AZURESDK_AZURITE=1 and start test/emulator/azurite-compose.yml)"
end
