# Blob batch operations: delete_blobs and set_*_blob_tier_blobs
# (Python: ContainerClient.delete_blobs etc.), sent as one multipart/mixed
# request of up to 256 sub-requests.

const MAX_BATCH = 256

"""
    BatchSubResponse

Result of one sub-request of a batch: `status_code`, `reason`, `headers`,
`body` and `blob_name`.
"""
struct BatchSubResponse
    status_code::Int
    reason::String
    headers::Vector{Pair{String,String}}
    body::String
    blob_name::String
end
Base.show(io::IO, r::BatchSubResponse) = print(io, "BatchSubResponse(", repr(r.blob_name), ", ", r.status_code, ")")

"""
    PartialBatchErrorException

Raised when some sub-requests of a batch failed (and
`raise_on_any_failure=true`). `parts` holds every [`BatchSubResponse`](@ref).
"""
struct PartialBatchErrorException <: AzureError
    message::String
    parts::Vector{BatchSubResponse}
    error::Any
end

function _batch_item(c::ContainerClient, item)
    if item isa AbstractString
        return Dict{Symbol,Any}(:name => String(item))
    elseif item isa BlobProperties
        return Dict{Symbol,Any}(:name => item.name, :snapshot => item.snapshot)
    elseif item isa BlobClient
        return Dict{Symbol,Any}(:name => item.blob_name, :snapshot => item.snapshot, :version_id => item.version_id)
    elseif item isa AbstractDict
        d = Dict{Symbol,Any}(Symbol(k) => v for (k, v) in item)
        haskey(d, :name) || throw(ArgumentError("batch items given as dicts need a \"name\""))
        return d
    end
    throw(ArgumentError("unsupported batch item $(typeof(item))"))
end

function _subrequest(cfg::StorageConfig, method, path, query, headers)
    url = build_url(cfg, path, query)
    hdrs = Pair{String,String}["x-ms-date" => rfc1123(Dates.now(Dates.UTC)), "Content-Length" => "0"]
    append!(hdrs, headers)
    _authorize!(cfg, method, url, hdrs)
    u = URIs.URI(url)
    target = isempty(u.query) ? u.path : string(u.path, "?", u.query)
    return method, target, hdrs
end

function _parse_batch_response(resp, names)
    ct = HTTP.header(resp, "Content-Type", "")
    m = match(r"boundary=\"?([^\";]+)\"?", ct)
    m === nothing && throw(ServiceResponseError("batch response without a multipart boundary"))
    boundary = "--" * m.captures[1]
    body = String(copy(resp.body))
    parts = BatchSubResponse[]
    for (i, chunk) in enumerate(split(body, boundary))
        sm = match(r"HTTP/1\.[01] (\d{3}) ?([^\r\n]*)\r?\n", chunk)
        sm === nothing && continue
        idm = match(r"Content-ID:\s*(\d+)"i, chunk)
        idx = idm === nothing ? length(parts) : parse(Int, idm.captures[1])
        rest = chunk[sm.offset+ncodeunits(sm.match):end]
        sep = findfirst(r"\r?\n\r?\n", rest)
        head, content = sep === nothing ? (rest, "") : (rest[1:first(sep)-1], rest[last(sep)+1:end])
        headers = Pair{String,String}[]
        for line in split(head, r"\r?\n")
            j = findfirst(':', line)
            j === nothing && continue
            push!(headers, strip(line[1:j-1]) => strip(line[j+1:end]))
        end
        name = 0 <= idx < length(names) ? names[idx+1] : ""
        push!(parts, BatchSubResponse(parse(Int, sm.captures[1]), sm.captures[2], headers, rstrip(content), name))
    end
    return parts
end

function _sub_error_code(p::BatchSubResponse)
    for (k, v) in p.headers
        lowercase(k) == "x-ms-error-code" && return v
    end
    return p.reason
end

function _send_batch(c::ContainerClient, subrequests, names; raise_on_any_failure::Bool=true, timeout=nothing)
    isempty(subrequests) && return BatchSubResponse[]
    length(subrequests) > MAX_BATCH && throw(ArgumentError("a batch can contain at most $(MAX_BATCH) sub-requests"))
    boundary = "batch_" * string(uuid4())
    io = IOBuffer()
    for (i, (method, target, hdrs)) in enumerate(subrequests)
        print(io, "--", boundary, "\r\n", "Content-Type: application/http\r\n", "Content-Transfer-Encoding: binary\r\n",
            "Content-ID: ", i - 1, "\r\n\r\n", method, " ", target, " HTTP/1.1\r\n")
        for (k, v) in hdrs
            print(io, k, ": ", v, "\r\n")
        end
        print(io, "\r\n")
    end
    print(io, "--", boundary, "--\r\n")
    body = take!(io)
    resp = _req(c, "POST", _container_path(c); query=[_CONTAINER_Q, "comp" => "batch"],
        headers=["Content-Type" => "multipart/mixed; boundary=$(boundary)"], body=body, expected=(202,),
        timeout=timeout, operation="batch")
    parts = _parse_batch_response(resp, names)
    if raise_on_any_failure && any(p -> !(200 <= p.status_code < 300), parts)
        failed = [p for p in parts if !(200 <= p.status_code < 300)]
        msg = "There is a partial failure in the batch operation: " *
              join(("$(p.blob_name): $(p.status_code) $(_sub_error_code(p))" for p in failed[1:min(end, 5)]), "; ")
        throw(PartialBatchErrorException(msg, parts, nothing))
    end
    return parts
end

"""
    delete_blobs(container_client, blobs...; delete_snapshots=nothing, if_modified_since=nothing,
                 if_unmodified_since=nothing, if_tags_match_condition=nothing, raise_on_any_failure=true,
                 timeout=nothing) -> Vector{BatchSubResponse}

Delete up to 256 blobs with one batch request. Each blob is a name,
`BlobProperties`, `BlobClient` or a dict with `name` and optionally
`snapshot`, `version_id`, `lease_id`, `delete_snapshots`, `etag`,
`match_condition`, `if_modified_since`, `if_unmodified_since`,
`if_tags_match_condition`. Raises [`PartialBatchErrorException`](@ref) if
any deletion fails (unless `raise_on_any_failure=false`).
"""
function delete_blobs(c::ContainerClient, blobs...; delete_snapshots=nothing, raise_on_any_failure::Bool=true,
    timeout=nothing, kwargs...)
    defaults = _opts(kwargs, (:if_modified_since, :if_unmodified_since, :if_tags_match_condition))
    subs = Any[]
    names = String[]
    for item in blobs
        d = _batch_item(c, item)
        o = merge(defaults, Dict(k => v for (k, v) in d if k in (_COND..., :lease)))
        haskey(d, :lease_id) && (o[:lease] = d[:lease_id])
        q = Pair{String,String}[]
        get(d, :snapshot, nothing) === nothing || push!(q, "snapshot" => String(d[:snapshot]))
        get(d, :version_id, nothing) === nothing || push!(q, "versionid" => String(d[:version_id]))
        h = _condition_headers(o)
        ds = something(get(d, :delete_snapshots, nothing), delete_snapshots, Some(nothing))
        ds === nothing || push!(h, "x-ms-delete-snapshots" => String(ds))
        push!(subs, _subrequest(c.config, "DELETE", _blob_path(c.container_name, d[:name]), q, h))
        push!(names, d[:name])
    end
    return _send_batch(c, subs, names; raise_on_any_failure, timeout)
end

function _tier_batch(c::ContainerClient, tier, blobs; rehydrate_priority=nothing, raise_on_any_failure=true,
    timeout=nothing, if_tags_match_condition=nothing)
    subs = Any[]
    names = String[]
    for item in blobs
        d = _batch_item(c, item)
        t = enum_value(something(get(d, :blob_tier, nothing), tier, Some(nothing)))
        t === nothing && throw(ArgumentError("a blob tier must be given for every blob"))
        q = Pair{String,String}[]
        get(d, :snapshot, nothing) === nothing || push!(q, "snapshot" => String(d[:snapshot]))
        get(d, :version_id, nothing) === nothing || push!(q, "versionid" => String(d[:version_id]))
        push!(q, "comp" => "tier")
        h = Pair{String,String}["x-ms-access-tier" => t]
        rp = something(get(d, :rehydrate_priority, nothing), rehydrate_priority, Some(nothing))
        rp === nothing || push!(h, "x-ms-rehydrate-priority" => enum_value(rp))
        lid = get(d, :lease_id, nothing)
        lid === nothing || push!(h, "x-ms-lease-id" => String(lid))
        tg = something(get(d, :if_tags_match_condition, nothing), if_tags_match_condition, Some(nothing))
        tg === nothing || push!(h, "x-ms-if-tags" => String(tg))
        push!(subs, _subrequest(c.config, "PUT", _blob_path(c.container_name, d[:name]), q, h))
        push!(names, d[:name])
    end
    return _send_batch(c, subs, names; raise_on_any_failure, timeout)
end

"""
    set_standard_blob_tier_blobs(container_client, standard_blob_tier, blobs...; rehydrate_priority=nothing,
                                 if_tags_match_condition=nothing, raise_on_any_failure=true, timeout=nothing)
        -> Vector{BatchSubResponse}

Set the tier of up to 256 block blobs with one batch request. A dict item
may carry its own `blob_tier`, `rehydrate_priority`, `lease_id`, `snapshot`
and `version_id`.
"""
set_standard_blob_tier_blobs(c::ContainerClient, standard_blob_tier, blobs...; kwargs...) =
    _tier_batch(c, standard_blob_tier, blobs; kwargs...)

"""
    set_premium_page_blob_tier_blobs(container_client, premium_page_blob_tier, blobs...; raise_on_any_failure=true,
                                     timeout=nothing) -> Vector{BatchSubResponse}
"""
set_premium_page_blob_tier_blobs(c::ContainerClient, premium_page_blob_tier, blobs...; kwargs...) =
    _tier_batch(c, premium_page_blob_tier, blobs; kwargs...)
