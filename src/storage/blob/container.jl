# ContainerClient operations.

const _CONTAINER_Q = "restype" => "container"

"""
    create_container(container_client; metadata=nothing, public_access=nothing,
                     container_encryption_scope=nothing, timeout=nothing) -> Dict

Create the container. `public_access` is a [`PublicAccess`](@ref) value (or
`"blob"`/`"container"`). `container_encryption_scope` is a scope name or a
dict/named tuple with `default_encryption_scope` and
`prevent_encryption_scope_override`. Returns `etag` and `last_modified`.
"""
function create_container(
        c::ContainerClient; metadata = nothing, public_access = nothing,
        container_encryption_scope = nothing, timeout = nothing
    )
    h = _metadata_headers(metadata)
    pa = enum_value(public_access)
    (pa === nothing || pa == "off") || push!(h, "x-ms-blob-public-access" => pa)
    if container_encryption_scope !== nothing
        sc = container_encryption_scope
        if sc isa AbstractString
            push!(h, "x-ms-default-encryption-scope" => String(sc))
        else
            g(k) = sc isa AbstractDict ? get(sc, k, get(sc, string(k), nothing)) : get(sc, k, nothing)
            push!(h, "x-ms-default-encryption-scope" => String(g(:default_encryption_scope)))
            deny = g(:prevent_encryption_scope_override)
            deny === nothing || push!(h, "x-ms-deny-encryption-scope-override" => deny ? "true" : "false")
        end
    end
    resp = _req(
        c, "PUT", _container_path(c); query = [_CONTAINER_Q], headers = h, expected = (201,), timeout = timeout,
        operation = "create_container"
    )
    return response_dict(resp)
end

"""
    delete_container(container_client; lease=nothing, if_modified_since=nothing, if_unmodified_since=nothing,
                     etag=nothing, match_condition=nothing, timeout=nothing)

Mark the container (and its blobs) for deletion.
"""
function delete_container(c::ContainerClient; kwargs...)
    o = _opts(kwargs, _COND, (:lease, :timeout))
    _req(
        c, "DELETE", _container_path(c); query = [_CONTAINER_Q], headers = _condition_headers(o; tags = false),
        expected = (202,), timeout = _timeout(o), operation = "delete_container"
    )
    return nothing
end

"""
    exists(client) -> Bool

Whether the container (`ContainerClient`) or blob (`BlobClient`) exists.
"""
function exists(c::ContainerClient; timeout = nothing)
    resp = _req(c, "GET", _container_path(c); query = [_CONTAINER_Q], timeout = timeout, raise = false)
    200 <= resp.status < 300 && return true
    resp.status == 404 && return false
    throw(storage_error(resp; operation = "exists"))
end

"""
    get_container_properties(container_client; lease=nothing, timeout=nothing) -> ContainerProperties
"""
function get_container_properties(c::ContainerClient; lease = nothing, timeout = nothing)
    h = Pair{String, String}[]
    lid = _lease_id(lease)
    lid === nothing || push!(h, "x-ms-lease-id" => lid)
    resp = _req(
        c, "GET", _container_path(c); query = [_CONTAINER_Q], headers = h, timeout = timeout,
        operation = "get_container_properties"
    )
    return _container_properties_from_headers(resp, c.container_name)
end

"""
    set_container_metadata(container_client, metadata=nothing; lease=nothing, if_modified_since=nothing,
                           timeout=nothing) -> Dict

Replace the container's metadata (`nothing` or an empty dict clears it).
"""
function set_container_metadata(c::ContainerClient, metadata = nothing; kwargs...)
    o = _opts(kwargs, (:lease, :if_modified_since, :timeout))
    h = vcat(_metadata_headers(metadata), _condition_headers(o; tags = false))
    resp = _req(
        c, "PUT", _container_path(c); query = [_CONTAINER_Q, "comp" => "metadata"], headers = h,
        expected = (200,), timeout = _timeout(o), operation = "set_container_metadata"
    )
    return response_dict(resp)
end

"""
    get_container_access_policy(container_client; lease=nothing, timeout=nothing) -> Dict

Returns `"public_access"` (a [`PublicAccess`](@ref) value or `nothing`) and
`"signed_identifiers"` (a vector of [`SignedIdentifier`](@ref)).
"""
function get_container_access_policy(c::ContainerClient; lease = nothing, timeout = nothing)
    h = Pair{String, String}[]
    lid = _lease_id(lease)
    lid === nothing || push!(h, "x-ms-lease-id" => lid)
    resp = _req(
        c, "GET", _container_path(c); query = [_CONTAINER_Q, "comp" => "acl"], headers = h, timeout = timeout,
        operation = "get_container_access_policy"
    )
    ids = SignedIdentifier[]
    if !isempty(resp.body)
        for si in xfindall(xml_parse(resp.body), "SignedIdentifier")
            ap = xfind(si, "AccessPolicy")
            push!(
                ids, SignedIdentifier(
                    something(_xstr(si, "Id"), ""), ap === nothing ? nothing :
                        AccessPolicy(permission = _xstr(ap, "Permission"), start = _xdate(ap, "Start"), expiry = _xdate(ap, "Expiry"))
                )
            )
        end
    end
    pa = _hdr(resp, "x-ms-blob-public-access")
    return Dict{String, Any}(
        "public_access" => pa === nothing ? nothing : enum_parse(PublicAccess.T, pa),
        "signed_identifiers" => ids, "etag" => _hdr(resp, "ETag"), "last_modified" => _hdate(resp, "Last-Modified")
    )
end

"""
    set_container_access_policy(container_client, signed_identifiers; public_access=nothing, lease=nothing,
                                if_modified_since=nothing, if_unmodified_since=nothing, timeout=nothing) -> Dict

Set up to five stored access policies (`Dict{String,AccessPolicy}`, keyed
by identifier) and the container's anonymous access level.
"""
function set_container_access_policy(c::ContainerClient, signed_identifiers::AbstractDict; public_access = nothing, kwargs...)
    o = _opts(kwargs, (:lease, :if_modified_since, :if_unmodified_since, :timeout))
    length(signed_identifiers) > 5 && throw(
        ArgumentError(
            "Too many access policies provided. The server does not support setting more than 5 access policies on a single resource."
        )
    )
    items = String[]
    for (id, p) in signed_identifiers
        inner = p === nothing ? "" : xml_element(
                "AccessPolicy", RawXML(
                    string(
                        p.start === nothing ? "" : xml_element("Start", iso8601(p.start)),
                        p.expiry === nothing ? "" : xml_element("Expiry", iso8601(p.expiry)),
                        xml_element("Permission", _perm_string(p.permission))
                    )
                )
            )
        push!(items, xml_element("SignedIdentifier", RawXML(xml_element("Id", String(id)) * inner)))
    end
    h = _condition_headers(o; tags = false)
    pa = enum_value(public_access)
    (pa === nothing || pa == "off") || push!(h, "x-ms-blob-public-access" => pa)
    push!(h, "Content-Type" => "application/xml")
    resp = _req(
        c, "PUT", _container_path(c); query = [_CONTAINER_Q, "comp" => "acl"], headers = h,
        body = xml_document("SignedIdentifiers", RawXML(join(items))), expected = (200,), timeout = _timeout(o),
        operation = "set_container_access_policy"
    )
    return response_dict(resp)
end

# --- listing ---------------------------------------------------------------------------------

const _LIST_INCLUDE = (
    "snapshots", "metadata", "uncommittedblobs", "copy", "deleted", "deletedwithversions", "tags",
    "versions", "immutabilitypolicy", "legalhold",
)

function _include_param(include)
    include === nothing && return nothing
    inc = include isa AbstractString ? [include] : collect(include)
    vals = String[]
    for i in inc
        s = lowercase(string(i))
        s in _LIST_INCLUDE || throw(ArgumentError("invalid include value $(repr(i)); expected one of $(join(_LIST_INCLUDE, ", "))"))
        push!(vals, s)
    end
    return isempty(vals) ? nothing : join(vals, ",")
end

"""
    BlobPrefix

A virtual directory returned by [`walk_blobs`](@ref): `name` is the prefix
(ending in the delimiter). Iterating a `BlobPrefix` walks its contents.
"""
struct BlobPrefix
    name::String
    container::String
    delimiter::String
    client::ContainerClient
    include::Union{Nothing, String}
    results_per_page::Union{Nothing, Int}
end
Base.show(io::IO, p::BlobPrefix) = print(io, "BlobPrefix(", repr(p.name), ")")
Base.IteratorSize(::Type{BlobPrefix}) = Base.SizeUnknown()
Base.eltype(::Type{BlobPrefix}) = Union{BlobProperties, BlobPrefix}
# Iteration state lives in the ItemPaged; restart it for each new iteration.
function Base.iterate(p::BlobPrefix)
    it = _walk(p.client, p.name, p.delimiter, p.include, p.results_per_page, nothing)
    r = iterate(it)
    r === nothing && return nothing
    return r[1], (it, r[2])
end
function Base.iterate(::BlobPrefix, state::Tuple{ItemPaged, Any})
    it, st = state
    r = iterate(it, st)
    r === nothing && return nothing
    return r[1], (it, r[2])
end

function _list_page(c::ContainerClient, prefix, delimiter, include, results_per_page, timeout, marker)
    q = Pair{String, String}[_CONTAINER_Q, "comp" => "list"]
    prefix === nothing || push!(q, "prefix" => String(prefix))
    delimiter === nothing || push!(q, "delimiter" => String(delimiter))
    marker === nothing || push!(q, "marker" => marker)
    results_per_page === nothing || push!(q, "maxresults" => string(results_per_page))
    include === nothing || push!(q, "include" => include)
    resp = _req(c, "GET", _container_path(c); query = q, timeout = timeout, operation = "list_blobs")
    x = xml_parse(resp.body)
    return x, _xstr(x, "NextMarker")
end

"""
    list_blobs(container_client; name_starts_with=nothing, include=nothing, results_per_page=nothing,
               timeout=nothing) -> ItemPaged{BlobProperties}

Lazily list the blobs in the container (flat). `include` is one or more of
`"snapshots"`, `"metadata"`, `"uncommittedblobs"`, `"copy"`, `"deleted"`,
`"deletedwithversions"`, `"tags"`, `"versions"`, `"immutabilitypolicy"`,
`"legalhold"`.
"""
function list_blobs(c::ContainerClient; name_starts_with = nothing, include = nothing, results_per_page = nothing, timeout = nothing)
    inc = _include_param(include)
    fetch = function (marker)
        x, next = _list_page(c, name_starts_with, nothing, inc, results_per_page, timeout, marker)
        return [_blob_properties_from_xml(b, c.container_name) for b in xfindall(xfind(x, "Blobs"), "Blob")], next
    end
    return ItemPaged{BlobProperties}(fetch)
end

"""
    list_blob_names(container_client; name_starts_with=nothing, results_per_page=nothing, timeout=nothing) -> ItemPaged{String}
"""
function list_blob_names(c::ContainerClient; name_starts_with = nothing, results_per_page = nothing, timeout = nothing)
    fetch = function (marker)
        x, next = _list_page(c, name_starts_with, nothing, nothing, results_per_page, timeout, marker)
        return String[_xname(b) for b in xfindall(xfind(x, "Blobs"), "Blob")], next
    end
    return ItemPaged{String}(fetch)
end

function _walk(c::ContainerClient, prefix, delimiter, inc, results_per_page, timeout)
    fetch = function (marker)
        x, next = _list_page(c, prefix, delimiter, inc, results_per_page, timeout, marker)
        # Prefixes first, then blobs (the order of Python's walk_blobs).
        blobs = xfind(x, "Blobs")
        items = Union{BlobProperties, BlobPrefix}[
            BlobPrefix(
                _xname(e), c.container_name, delimiter, c, inc,
                results_per_page
            ) for e in xfindall(blobs, "BlobPrefix")
        ]
        append!(items, [_blob_properties_from_xml(e, c.container_name) for e in xfindall(blobs, "Blob")])
        return items, next
    end
    return ItemPaged{Union{BlobProperties, BlobPrefix}}(fetch)
end

"""
    walk_blobs(container_client; name_starts_with=nothing, include=nothing, delimiter="/",
               results_per_page=nothing, timeout=nothing) -> ItemPaged{Union{BlobProperties,BlobPrefix}}

List blobs hierarchically: blobs directly under `name_starts_with` are
returned as `BlobProperties`, deeper "directories" as [`BlobPrefix`](@ref)
items that can themselves be iterated.
"""
walk_blobs(
    c::ContainerClient; name_starts_with = nothing, include = nothing, delimiter::AbstractString = "/",
    results_per_page = nothing, timeout = nothing
) =
    _walk(c, name_starts_with, String(delimiter), _include_param(include), results_per_page, timeout)

find_blobs_by_tags(c::ContainerClient, filter_expression::AbstractString; results_per_page = nothing, timeout = nothing) =
    _filtered_blobs(c, _container_path(c), [_CONTAINER_Q], filter_expression, results_per_page, timeout)

# --- blob shortcuts -------------------------------------------------------------------------

"""
    upload_blob(container_client, name, data; kwargs...) -> BlobClient

Upload `data` to blob `name` (see [`upload_blob(::BlobClient, data)`](@ref)
for the keyword arguments) and return a client for it.
"""
function upload_blob(c::ContainerClient, name, data; kwargs...)
    b = get_blob_client(c, name)
    upload_blob(b, data; kwargs...)
    return b
end

"""
    download_blob(container_client, blob; kwargs...) -> StorageStreamDownloader
"""
download_blob(c::ContainerClient, blob; kwargs...) = download_blob(get_blob_client(c, blob); kwargs...)

"""
    delete_blob(container_client, blob; delete_snapshots=nothing, kwargs...)
"""
delete_blob(c::ContainerClient, blob; kwargs...) = delete_blob(get_blob_client(c, blob); kwargs...)
