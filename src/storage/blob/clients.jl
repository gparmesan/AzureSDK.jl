# Client types, constructors and shared request helpers.

function _check_container_name(name::AbstractString)
    isempty(name) && throw(ArgumentError("Please specify a container name."))
    return String(name)
end

function _check_blob_name(name::AbstractString)
    isempty(name) && throw(ArgumentError("Please specify a blob name."))
    ncodeunits(name) > 1024 && throw(ArgumentError("blob names are limited to 1024 characters"))
    return String(name)
end

"""
    BlobServiceClient(account_url; credential=nothing, kwargs...)
    BlobServiceClient(f::Function, account_url; kwargs...)      # do-block form
    from_connection_string(BlobServiceClient, conn_str; credential=nothing, kwargs...)

Client for the Blob service of a storage account (Python:
`azure.storage.blob.BlobServiceClient`). `account_url` is
`https://<account>.blob.core.windows.net` (optionally with a SAS query
string). `credential` may be `nothing` (anonymous or SAS in the URL), a SAS
token string, an account key string, an `AzureNamedKeyCredential`, a dict /
named tuple with `account_name` and `account_key`, an `AzureSasCredential`
or any `TokenCredential` (e.g. `DefaultAzureCredential()`).

Keyword arguments: `api_version`, `audience`, `connection_timeout` (20 s),
`read_timeout` (60 s), `retry_total` (3), `retry_backoff_factor`,
`retry_backoff_max`, `retry_mode`, `connection_verify`, `http_proxy`,
`user_agent`, `max_single_put_size` (64 MiB), `max_block_size` (4 MiB),
`max_page_size` (4 MiB), `max_single_get_size` (32 MiB) and
`max_chunk_get_size` (4 MiB).

Properties: `url`, `account_name`, `primary_endpoint`, `primary_hostname`,
`credential`, `api_version`.
"""
struct BlobServiceClient
    config::StorageConfig
end

"""
    ContainerClient(account_url, container_name; credential=nothing, kwargs...)
    from_container_url(container_url; credential=nothing, kwargs...)
    from_connection_string(ContainerClient, conn_str, container_name; kwargs...)
    get_container_client(service_client, container_name)

Client for one container (Python: `azure.storage.blob.ContainerClient`).
The container need not exist yet. Keyword arguments as for
[`BlobServiceClient`](@ref).

Properties: `url`, `container_name`, `account_name`, `primary_endpoint`,
`primary_hostname`, `credential`, `api_version`.
"""
struct ContainerClient
    config::StorageConfig
    container_name::String
end

"""
    BlobClient(account_url, container_name, blob_name; snapshot=nothing, version_id=nothing, credential=nothing, kwargs...)
    from_blob_url(blob_url; credential=nothing, snapshot=nothing, kwargs...)
    from_connection_string(BlobClient, conn_str, container_name, blob_name; kwargs...)
    get_blob_client(container_client, blob_name)

Client for one blob, which need not exist yet (Python:
`azure.storage.blob.BlobClient`). With `snapshot` or `version_id` the client
addresses that snapshot/version for read, delete, tag and tier operations.

Properties: `url`, `container_name`, `blob_name`, `snapshot`, `version_id`,
`account_name`, `primary_endpoint`, `primary_hostname`, `credential`,
`api_version`.
"""
struct BlobClient
    config::StorageConfig
    container_name::String
    blob_name::String
    snapshot::Union{Nothing, String}
    version_id::Union{Nothing, String}
end

const AnyBlobClient = Union{BlobServiceClient, ContainerClient, BlobClient}

# --- constructors -----------------------------------------------------------------

function _snapshot_id(s)
    s === nothing && return nothing
    s isa AbstractString && return String(s)
    s isa AbstractDict && return String(get(s, "snapshot", get(s, :snapshot, "")))
    s isa BlobProperties && return s.snapshot
    throw(ArgumentError("snapshot must be a string, a dict with a \"snapshot\" key or BlobProperties"))
end

function BlobServiceClient(account_url::AbstractString; credential = nothing, kwargs...)
    cfg, u = StorageConfig(account_url, credential, "blob"; kwargs...)
    isempty(u.segments) || throw(ArgumentError("account_url must not contain a container or blob path"))
    return BlobServiceClient(cfg)
end

function ContainerClient(account_url::AbstractString, container_name::AbstractString; credential = nothing, kwargs...)
    cfg, u = StorageConfig(account_url, credential, "blob"; kwargs...)
    isempty(u.segments) || throw(ArgumentError("account_url must not contain a container or blob path; use from_container_url"))
    return ContainerClient(cfg, _check_container_name(container_name))
end

function BlobClient(
        account_url::AbstractString, container_name::AbstractString, blob_name::AbstractString;
        snapshot = nothing, version_id = nothing, credential = nothing, kwargs...
    )
    cfg, u = StorageConfig(account_url, credential, "blob"; kwargs...)
    isempty(u.segments) || throw(ArgumentError("account_url must not contain a container or blob path; use from_blob_url"))
    return BlobClient(
        cfg, _check_container_name(container_name), _check_blob_name(blob_name), _snapshot_id(snapshot),
        version_id === nothing ? nothing : String(version_id)
    )
end

for T in (:BlobServiceClient, :ContainerClient, :BlobClient)
    @eval function $T(f::Function, args...; kwargs...)
        client = $T(args...; kwargs...)
        try
            return f(client)
        finally
            close(client)
        end
    end
end

"""
    from_container_url(container_url; credential=nothing, kwargs...) -> ContainerClient

Create a [`ContainerClient`](@ref) from a full container URL (which may
carry a SAS query string).
"""
function from_container_url(container_url::AbstractString; credential = nothing, kwargs...)
    cfg, u = StorageConfig(container_url, credential, "blob"; kwargs...)
    length(u.segments) == 1 || throw(ArgumentError("Invalid URL. Provide a container_url with a valid container name."))
    return ContainerClient(cfg, _check_container_name(u.segments[1]))
end

"""
    from_blob_url(blob_url; credential=nothing, snapshot=nothing, kwargs...) -> BlobClient

Create a [`BlobClient`](@ref) from a full blob URL. A `snapshot` or
`versionid` in the URL's query string is honoured.
"""
function from_blob_url(blob_url::AbstractString; credential = nothing, snapshot = nothing, kwargs...)
    cfg, u = StorageConfig(blob_url, credential, "blob"; kwargs...)
    length(u.segments) >= 2 || throw(ArgumentError("Invalid URL. Provide a blob_url with a valid blob and container name."))
    q = Dict(lowercase(k) => v for (k, v) in query_pairs(u.query))
    snap = snapshot !== nothing ? _snapshot_id(snapshot) : get(q, "snapshot", nothing)
    return BlobClient(
        cfg, _check_container_name(u.segments[1]), _check_blob_name(join(u.segments[2:end], "/")),
        snap, get(q, "versionid", nothing)
    )
end

function _from_cs(conn_str, credential)
    endpoint, cs_cred = parse_storage_connection_string(conn_str, "blob")
    return endpoint, credential === nothing ? cs_cred : credential
end

from_connection_string(::Type{BlobServiceClient}, conn_str::AbstractString; credential = nothing, kwargs...) =
    ((ep, cred) = _from_cs(conn_str, credential); BlobServiceClient(ep; credential = cred, kwargs...))
from_connection_string(
    ::Type{ContainerClient}, conn_str::AbstractString, container_name::AbstractString;
    credential = nothing, kwargs...
) =
    ((ep, cred) = _from_cs(conn_str, credential); ContainerClient(ep, container_name; credential = cred, kwargs...))
from_connection_string(
    ::Type{BlobClient}, conn_str::AbstractString, container_name::AbstractString,
    blob_name::AbstractString; credential = nothing, kwargs...
) =
    ((ep, cred) = _from_cs(conn_str, credential); BlobClient(ep, container_name, blob_name; credential = cred, kwargs...))
function from_connection_string(f::Function, T::Type{<:AnyBlobClient}, args...; kwargs...)
    client = from_connection_string(T, args...; kwargs...)
    try
        return f(client)
    finally
        close(client)
    end
end

"""
    get_container_client(service_client, container_name) -> ContainerClient
"""
get_container_client(s::BlobServiceClient, container) =
    ContainerClient(s.config, _check_container_name(container isa ContainerProperties ? container.name : container))

"""
    get_blob_client(service_client, container_name, blob_name; snapshot=nothing, version_id=nothing) -> BlobClient
    get_blob_client(container_client, blob_name; snapshot=nothing, version_id=nothing) -> BlobClient
"""
get_blob_client(s::BlobServiceClient, container, blob; snapshot = nothing, version_id = nothing) =
    BlobClient(
    s.config, _check_container_name(container isa ContainerProperties ? container.name : container),
    _check_blob_name(blob isa BlobProperties ? blob.name : blob), _snapshot_id(snapshot),
    version_id === nothing ? nothing : String(version_id)
)
get_blob_client(c::ContainerClient, blob; snapshot = nothing, version_id = nothing) =
    BlobClient(
    c.config, c.container_name, _check_blob_name(blob isa BlobProperties ? blob.name : blob),
    _snapshot_id(snapshot === nothing && blob isa BlobProperties ? blob.snapshot : snapshot),
    version_id === nothing ? nothing : String(version_id)
)

"Clients hold no dedicated resources; `close` exists for parity with the Python SDK and do-block use."
Base.close(::AnyBlobClient) = nothing

# --- paths, URLs and properties ---------------------------------------------------------

_container_path(name::AbstractString) = "/" * quote_url(name; safe = "")
_container_path(c::ContainerClient) = _container_path(c.container_name)
_blob_path(container::AbstractString, blob::AbstractString) = _container_path(container) * "/" * quote_url(blob; safe = "/")
_blob_path(b::BlobClient) = _blob_path(b.container_name, b.blob_name)

"Snapshot / version query parameters of a blob client."
function _blob_query(b::BlobClient)
    q = Pair{String, String}[]
    b.snapshot === nothing || push!(q, "snapshot" => b.snapshot)
    b.version_id === nothing || push!(q, "versionid" => b.version_id)
    return q
end

function _common_property(cfg::StorageConfig, name::Symbol)
    name === :account_name && return cfg.account_name
    name === :primary_endpoint && return cfg.primary_endpoint
    name === :primary_hostname && return cfg.port == "" ? cfg.host : "$(cfg.host):$(cfg.port)"
    name === :credential && return cfg.credential
    name === :api_version && return cfg.api_version
    name === :location_mode && return "primary"
    return nothing
end

const _COMMON_PROPS = (:account_name, :primary_endpoint, :primary_hostname, :credential, :api_version, :location_mode)

function Base.getproperty(c::BlobServiceClient, name::Symbol)
    cfg = getfield(c, :config)
    name === :config && return cfg
    name === :url && return build_url(cfg, "/")
    name in _COMMON_PROPS && return _common_property(cfg, name)
    return getfield(c, name)
end
function Base.getproperty(c::ContainerClient, name::Symbol)
    name in (:config, :container_name) && return getfield(c, name)
    cfg = getfield(c, :config)
    name === :url && return build_url(cfg, _container_path(getfield(c, :container_name)))
    name in _COMMON_PROPS && return _common_property(cfg, name)
    return getfield(c, name)
end
function Base.getproperty(c::BlobClient, name::Symbol)
    name in fieldnames(BlobClient) && return getfield(c, name)
    cfg = getfield(c, :config)
    name === :url && return build_url(cfg, _blob_path(getfield(c, :container_name), getfield(c, :blob_name)), _blob_query(c))
    name in _COMMON_PROPS && return _common_property(cfg, name)
    return getfield(c, name)
end
Base.propertynames(::BlobServiceClient, private::Bool = false) = (:url, _COMMON_PROPS..., (private ? (:config,) : ())...)
Base.propertynames(::ContainerClient, private::Bool = false) = (:url, :container_name, _COMMON_PROPS..., (private ? (:config,) : ())...)
Base.propertynames(::BlobClient, private::Bool = false) =
    (:url, :container_name, :blob_name, :snapshot, :version_id, _COMMON_PROPS..., (private ? (:config,) : ())...)

# URLs are shown without their query string: a SAS must not end up in logs.
Base.show(io::IO, c::BlobServiceClient) = print(io, "BlobServiceClient(", c.primary_endpoint, ")")
Base.show(io::IO, c::ContainerClient) = print(io, "ContainerClient(", c.primary_endpoint, _container_path(c), ")")
Base.show(io::IO, c::BlobClient) = print(
    io, "BlobClient(", c.primary_endpoint, _blob_path(c),
    c.snapshot === nothing ? "" : ", snapshot=" * c.snapshot, c.version_id === nothing ? "" : ", version_id=" * c.version_id, ")"
)

# --- keyword options -----------------------------------------------------------------------

const _COND = (:etag, :match_condition, :if_modified_since, :if_unmodified_since, :if_tags_match_condition)
const _SOURCE_COND = (:source_etag, :source_match_condition, :source_if_modified_since, :source_if_unmodified_since)
const _CPK = (:cpk, :encryption_scope)

"Validate keyword arguments against the allowed names; returns a Dict."
function _opts(kwargs, allowed...)
    d = Dict{Symbol, Any}()
    for (k, v) in kwargs
        any(g -> k in g, allowed) || throw(ArgumentError("unexpected keyword argument `$(k)`"))
        d[k] = v
    end
    return d
end

_timeout(o) = get(o, :timeout, nothing)

function _match_headers!(h, etag, match_condition, if_match = "If-Match", if_none_match = "If-None-Match")
    mc = match_condition
    if mc === nothing || mc == MatchConditions.Unconditionally
        etag === nothing || throw(ArgumentError("'etag' specified without 'match_condition'."))
    elseif mc == MatchConditions.IfNotModified
        etag === nothing && throw(ArgumentError("'match_condition' specified without 'etag'."))
        push!(h, if_match => string(etag))
    elseif mc == MatchConditions.IfModified
        etag === nothing && throw(ArgumentError("'match_condition' specified without 'etag'."))
        push!(h, if_none_match => string(etag))
    elseif mc == MatchConditions.IfPresent
        push!(h, if_match => "*")
    elseif mc == MatchConditions.IfMissing
        push!(h, if_none_match => "*")
    end
    return h
end

_lease_id(lease) = lease === nothing ? nothing : lease isa AbstractString ? String(lease) : lease.id

"Access-condition headers (lease, etag/match_condition, modified-since, tags)."
function _condition_headers(o; lease::Bool = true, tags::Bool = true)
    h = Pair{String, String}[]
    if lease
        lid = _lease_id(get(o, :lease, nothing))
        lid === nothing || push!(h, "x-ms-lease-id" => lid)
    end
    _match_headers!(h, get(o, :etag, nothing), get(o, :match_condition, nothing))
    ims = get(o, :if_modified_since, nothing)
    ims === nothing || push!(h, "If-Modified-Since" => rfc1123(to_utc(ims)))
    ius = get(o, :if_unmodified_since, nothing)
    ius === nothing || push!(h, "If-Unmodified-Since" => rfc1123(to_utc(ius)))
    if tags
        t = get(o, :if_tags_match_condition, nothing)
        t === nothing || push!(h, "x-ms-if-tags" => String(t))
    end
    return h
end

function _source_condition_headers(o)
    h = Pair{String, String}[]
    _match_headers!(
        h, get(o, :source_etag, nothing), get(o, :source_match_condition, nothing),
        "x-ms-source-if-match", "x-ms-source-if-none-match"
    )
    ims = get(o, :source_if_modified_since, nothing)
    ims === nothing || push!(h, "x-ms-source-if-modified-since" => rfc1123(to_utc(ims)))
    ius = get(o, :source_if_unmodified_since, nothing)
    ius === nothing || push!(h, "x-ms-source-if-unmodified-since" => rfc1123(to_utc(ius)))
    return h
end

function _cpk_headers(cfg::StorageConfig, o)
    h = Pair{String, String}[]
    cpk = get(o, :cpk, nothing)
    if cpk !== nothing
        cfg.scheme == "https" || throw(ArgumentError("Customer provided encryption key must be used over HTTPS."))
        cpk isa CustomerProvidedEncryptionKey || throw(ArgumentError("cpk must be a CustomerProvidedEncryptionKey"))
        push!(
            h, "x-ms-encryption-key" => cpk.key_value, "x-ms-encryption-key-sha256" => cpk.key_hash,
            "x-ms-encryption-algorithm" => cpk.algorithm
        )
    end
    scope = get(o, :encryption_scope, nothing)
    scope === nothing || push!(h, "x-ms-encryption-scope" => String(scope))
    return h
end

const _METADATA_NAME_RE = r"^[A-Za-z_][A-Za-z0-9_]*$"

function _metadata_headers(md)
    h = Pair{String, String}[]
    md === nothing && return h
    for (k, v) in md
        ks = string(k)
        occursin(_METADATA_NAME_RE, ks) || throw(ArgumentError("invalid metadata name $(repr(ks)) (must be a C# identifier)"))
        vs = string(v)
        (occursin('\n', vs) || occursin('\r', vs)) && throw(ArgumentError("metadata values must not contain line breaks"))
        push!(h, "x-ms-meta-" * ks => vs)
    end
    return h
end

"Blob HTTP header settings, with `prefix` \"x-ms-blob-\" for most operations."
function _content_settings_headers(cs; prefix::AbstractString = "x-ms-blob-")
    h = Pair{String, String}[]
    cs === nothing && return h
    cs isa ContentSettings || throw(ArgumentError("content_settings must be a ContentSettings"))
    for (f, name) in (
            (:content_type, "content-type"), (:content_encoding, "content-encoding"),
            (:content_language, "content-language"), (:content_disposition, "content-disposition"),
            (:cache_control, "cache-control"),
        )
        v = getfield(cs, f)
        v === nothing || push!(h, prefix * name => v)
    end
    cs.content_md5 === nothing || push!(h, prefix * "content-md5" => base64encode(cs.content_md5))
    return h
end

function _tags_header(tags)
    tags === nothing && return Pair{String, String}[]
    return ["x-ms-tags" => encode_query([string(k) => string(v) for (k, v) in tags])]
end

function _immutability_headers(o)
    h = Pair{String, String}[]
    p = get(o, :immutability_policy, nothing)
    if p !== nothing
        p.expiry_time === nothing || push!(h, "x-ms-immutability-policy-until-date" => rfc1123(to_utc(p.expiry_time)))
        p.policy_mode === nothing || push!(h, "x-ms-immutability-policy-mode" => enum_value(p.policy_mode))
    end
    lh = get(o, :legal_hold, nothing)
    lh === nothing || push!(h, "x-ms-legal-hold" => lh ? "true" : "false")
    return h
end

# --- response parsing ---------------------------------------------------------------------

_hdr(resp, name) = (v = HTTP.header(resp, name, ""); isempty(v) ? nothing : String(v))
_hbool(resp, name) = (v = _hdr(resp, name); v === nothing ? nothing : lowercase(v) == "true")
_hint(resp, name) = (v = _hdr(resp, name); v === nothing ? nothing : tryparse(Int64, v))
_hdate(resp, name) = parse_datetime(_hdr(resp, name))

# HTTP.jl canonicalises response header names (`X-Ms-Meta-Owner`), so the
# original spelling of metadata names is lost; lookups are case-insensitive.
function _metadata_from_headers(resp)
    md = CaseInsensitiveDict{String}()
    for (k, v) in resp.headers
        lk = lowercase(k)
        startswith(lk, "x-ms-meta-") && (md[String(k)[11:end]] = String(v))
    end
    return md
end

function _lease_from_headers(resp)
    return LeaseProperties(
        status = _hdr(resp, "x-ms-lease-status"), state = _hdr(resp, "x-ms-lease-state"),
        duration = _hdr(resp, "x-ms-lease-duration")
    )
end

function _b64(v)
    v === nothing && return nothing
    try
        return base64decode(v)
    catch
        return nothing
    end
end

function _blob_properties_from_headers(resp, b::BlobClient)
    p = BlobProperties(name = b.blob_name, container = b.container_name, snapshot = b.snapshot)
    p.version_id = something(_hdr(resp, "x-ms-version-id"), b.version_id, Some(nothing))
    p.is_current_version = _hbool(resp, "x-ms-is-current-version")
    p.blob_type = enum_parse(BlobType.T, _hdr(resp, "x-ms-blob-type"))
    p.metadata = _metadata_from_headers(resp)
    p.last_modified = _hdate(resp, "Last-Modified")
    p.etag = _hdr(resp, "ETag")
    p.size = something(_hint(resp, "Content-Length"), 0)
    p.content_range = _hdr(resp, "Content-Range")
    cbc = _hint(resp, "x-ms-blob-committed-block-count")
    p.append_blob_committed_block_count = cbc === nothing ? nothing : Int(cbc)
    p.is_append_blob_sealed = _hbool(resp, "x-ms-blob-sealed")
    p.page_blob_sequence_number = _hint(resp, "x-ms-blob-sequence-number")
    p.server_encrypted = _hbool(resp, "x-ms-server-encrypted")
    p.request_server_encrypted = _hbool(resp, "x-ms-request-server-encrypted")
    p.copy = CopyProperties(
        id = _hdr(resp, "x-ms-copy-id"), source = _hdr(resp, "x-ms-copy-source"),
        status = _hdr(resp, "x-ms-copy-status"), progress = _hdr(resp, "x-ms-copy-progress"),
        completion_time = _hdate(resp, "x-ms-copy-completion-time"),
        status_description = _hdr(resp, "x-ms-copy-status-description"),
        incremental_copy = _hbool(resp, "x-ms-incremental-copy"),
        destination_snapshot = _hdr(resp, "x-ms-copy-destination-snapshot")
    )
    p.content_settings = ContentSettings(
        content_type = _hdr(resp, "Content-Type"),
        content_encoding = _hdr(resp, "Content-Encoding"), content_language = _hdr(resp, "Content-Language"),
        content_disposition = _hdr(resp, "Content-Disposition"), cache_control = _hdr(resp, "Cache-Control"),
        content_md5 = _b64(something(_hdr(resp, "x-ms-blob-content-md5"), _hdr(resp, "Content-MD5"), Some(nothing)))
    )
    p.lease = _lease_from_headers(resp)
    p.blob_tier = _hdr(resp, "x-ms-access-tier")
    p.blob_tier_inferred = _hbool(resp, "x-ms-access-tier-inferred")
    p.blob_tier_change_time = _hdate(resp, "x-ms-access-tier-change-time")
    p.archive_status = _hdr(resp, "x-ms-archive-status")
    p.rehydrate_priority = _hdr(resp, "x-ms-rehydrate-priority")
    p.creation_time = _hdate(resp, "x-ms-creation-time")
    p.encryption_key_sha256 = _hdr(resp, "x-ms-encryption-key-sha256")
    p.encryption_scope = _hdr(resp, "x-ms-encryption-scope")
    p.last_accessed_on = _hdate(resp, "x-ms-last-access-time")
    tc = _hint(resp, "x-ms-tag-count")
    p.tag_count = tc === nothing ? nothing : Int(tc)
    p.immutability_policy = ImmutabilityPolicy(
        expiry_time = _hdate(resp, "x-ms-immutability-policy-until-date"),
        policy_mode = enum_parse(BlobImmutabilityPolicyMode.T, _hdr(resp, "x-ms-immutability-policy-mode"))
    )
    p.has_legal_hold = _hbool(resp, "x-ms-legal-hold")
    return p
end

function _container_properties_from_headers(resp, name)
    pa = _hdr(resp, "x-ms-blob-public-access")
    return ContainerProperties(
        name = name, last_modified = _hdate(resp, "Last-Modified"), etag = _hdr(resp, "ETag"),
        lease = _lease_from_headers(resp), public_access = pa === nothing ? nothing : enum_parse(PublicAccess.T, pa),
        has_immutability_policy = _hbool(resp, "x-ms-has-immutability-policy"),
        has_legal_hold = _hbool(resp, "x-ms-has-legal-hold"), metadata = _metadata_from_headers(resp),
        default_encryption_scope = _hdr(resp, "x-ms-default-encryption-scope"),
        prevent_encryption_scope_override = _hbool(resp, "x-ms-deny-encryption-scope-override"),
        immutable_storage_with_versioning_enabled = _hbool(resp, "x-ms-immutable-storage-with-versioning-enabled")
    )
end

# XML helpers for listings.
_xbool(e, tag) = (t = xchildtext(e, tag); t === nothing ? nothing : lowercase(strip(t)) == "true")
_xint(e, tag) = (t = xchildtext(e, tag); t === nothing ? nothing : tryparse(Int64, strip(t)))
_xdate(e, tag) = parse_datetime(xchildtext(e, tag))
_xstr(e, tag) = (t = xchildtext(e, tag); t === nothing || isempty(t) ? nothing : t)
# Listings return ETags unquoted; use the quoted header form everywhere.
_xetag(e) = (t = _xstr(e, "Etag"); t === nothing || startswith(t, '"') ? t : string('"', t, '"'))

function _xmetadata(e)
    md = CaseInsensitiveDict{String}()
    m = xfind(e, "Metadata")
    m === nothing && return md
    for c in xfindall(m)
        md[c.tag] = xtext(c)
    end
    return md
end

function _xtags(e)
    t = xfind(xfind(e, "Tags"), "TagSet")
    t === nothing && (t = xfind(e, "TagSet"))
    t === nothing && return nothing
    return Dict{String, String}(
        something(xchildtext(tag, "Key"), "") => something(xchildtext(tag, "Value"), "")
            for tag in xfindall(t, "Tag")
    )
end

"Blob name from a listing element, decoding `Encoded=\"true\"` names."
function _xname(e)
    n = xfind(e, "Name")
    n === nothing && return ""
    s = xtext(n)
    return lowercase(xattr(n, "Encoded", "false")) == "true" ? unquote_url(s) : s
end

function _blob_properties_from_xml(e::XElement, container::AbstractString)
    p = BlobProperties(
        name = _xname(e), container = container, snapshot = _xstr(e, "Snapshot"),
        version_id = _xstr(e, "VersionId"), is_current_version = _xbool(e, "IsCurrentVersion"),
        deleted = something(_xbool(e, "Deleted"), false), has_versions_only = _xbool(e, "HasVersionsOnly")
    )
    p.metadata = _xmetadata(e)
    p.tags = _xtags(e)
    x = xfind(e, "Properties")
    x === nothing && return p
    p.creation_time = _xdate(x, "Creation-Time")
    p.last_modified = _xdate(x, "Last-Modified")
    p.etag = _xetag(x)
    p.size = something(_xint(x, "Content-Length"), 0)
    p.content_settings = ContentSettings(
        content_type = _xstr(x, "Content-Type"),
        content_encoding = _xstr(x, "Content-Encoding"), content_language = _xstr(x, "Content-Language"),
        content_disposition = _xstr(x, "Content-Disposition"), cache_control = _xstr(x, "Cache-Control"),
        content_md5 = _b64(_xstr(x, "Content-MD5"))
    )
    p.page_blob_sequence_number = _xint(x, "x-ms-blob-sequence-number")
    p.blob_type = enum_parse(BlobType.T, _xstr(x, "BlobType"))
    p.blob_tier = _xstr(x, "AccessTier")
    p.blob_tier_inferred = _xbool(x, "AccessTierInferred")
    p.blob_tier_change_time = _xdate(x, "AccessTierChangeTime")
    p.archive_status = _xstr(x, "ArchiveStatus")
    p.rehydrate_priority = _xstr(x, "RehydratePriority")
    p.lease = LeaseProperties(
        status = _xstr(x, "LeaseStatus"), state = _xstr(x, "LeaseState"),
        duration = _xstr(x, "LeaseDuration")
    )
    p.copy = CopyProperties(
        id = _xstr(x, "CopyId"), source = _xstr(x, "CopySource"), status = _xstr(x, "CopyStatus"),
        progress = _xstr(x, "CopyProgress"), completion_time = _xdate(x, "CopyCompletionTime"),
        status_description = _xstr(x, "CopyStatusDescription"), incremental_copy = _xbool(x, "IncrementalCopy"),
        destination_snapshot = _xstr(x, "CopyDestinationSnapshot")
    )
    p.server_encrypted = _xbool(x, "ServerEncrypted")
    p.encryption_key_sha256 = _xstr(x, "CustomerProvidedKeySha256")
    p.encryption_scope = _xstr(x, "EncryptionScope")
    p.deleted_time = _xdate(x, "DeletedTime")
    rrd = _xint(x, "RemainingRetentionDays")
    p.remaining_retention_days = rrd === nothing ? nothing : Int(rrd)
    tc = _xint(x, "TagCount")
    p.tag_count = tc === nothing ? nothing : Int(tc)
    p.last_accessed_on = _xdate(x, "LastAccessTime")
    p.immutability_policy = ImmutabilityPolicy(
        expiry_time = _xdate(x, "ImmutabilityPolicyUntilDate"),
        policy_mode = enum_parse(BlobImmutabilityPolicyMode.T, _xstr(x, "ImmutabilityPolicyMode"))
    )
    p.has_legal_hold = _xbool(x, "LegalHold")
    p.is_append_blob_sealed = _xbool(x, "Sealed")
    return p
end

function _container_properties_from_xml(e::XElement)
    x = xfind(e, "Properties")
    pa = _xstr(x, "PublicAccess")
    rrd = _xint(x, "RemainingRetentionDays")
    return ContainerProperties(
        name = _xname(e), deleted = something(_xbool(e, "Deleted"), false),
        version = _xstr(e, "Version"), metadata = _xmetadata(e), last_modified = _xdate(x, "Last-Modified"),
        etag = _xetag(x), lease = LeaseProperties(
            status = _xstr(x, "LeaseStatus"), state = _xstr(x, "LeaseState"),
            duration = _xstr(x, "LeaseDuration")
        ),
        public_access = pa === nothing ? nothing : enum_parse(PublicAccess.T, pa),
        has_immutability_policy = _xbool(x, "HasImmutabilityPolicy"), has_legal_hold = _xbool(x, "HasLegalHold"),
        deleted_time = _xdate(x, "DeletedTime"), remaining_retention_days = rrd === nothing ? nothing : Int(rrd),
        default_encryption_scope = _xstr(x, "DefaultEncryptionScope"),
        prevent_encryption_scope_override = _xbool(x, "DenyEncryptionScopeOverride"),
        immutable_storage_with_versioning_enabled = _xbool(x, "ImmutableStorageWithVersioningEnabled")
    )
end
