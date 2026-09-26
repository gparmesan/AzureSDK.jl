# BlobClient operations (everything except upload_blob / download_blob).

"Bytes for a request body: strings are UTF-8 encoded, IO is read (up to `length`)."
function _as_bytes(data, length=nothing)
    if data isa AbstractString
        bytes = codeunits(String(data))
    elseif data isa AbstractVector{UInt8}
        bytes = data
    elseif data isa IO
        return length === nothing ? read(data) : read(data, length)
    else
        throw(ArgumentError("unsupported data type $(typeof(data)); expected bytes, a string or an IO"))
    end
    length === nothing && return bytes
    length > Base.length(bytes) && throw(ArgumentError("length exceeds the size of the data"))
    return view(bytes, 1:length)
end

_md5_header(validate::Bool, body) = validate ? ["Content-MD5" => base64encode(md5(body))] : Pair{String,String}[]

function _check_md5_response(validate::Bool, body, resp)
    validate || return nothing
    got = _hdr(resp, "Content-MD5")
    got === nothing && return nothing
    base64decode(got) == md5(body) ||
        throw(ContentValidationError("MD5 mismatch: the uploaded content was corrupted in transit"))
    return nothing
end

_range_header(offset, length) = "bytes=$(offset)-$(offset + length - 1)"

_encode_block_id(id) = base64encode(string(id))
function _decode_block_id(s)
    try
        b = base64decode(s)
        return isvalid(String, b) ? String(b) : s
    catch
        return s
    end
end

function _seq_headers(o)
    h = Pair{String,String}[]
    for (k, name) in ((:if_sequence_number_lte, "x-ms-if-sequence-number-le"), (:if_sequence_number_lt, "x-ms-if-sequence-number-lt"),
        (:if_sequence_number_eq, "x-ms-if-sequence-number-eq"))
        v = get(o, k, nothing)
        v === nothing || push!(h, name => string(v))
    end
    return h
end
const _SEQ = (:if_sequence_number_lte, :if_sequence_number_lt, :if_sequence_number_eq)

function _source_auth_header(o)
    a = get(o, :source_authorization, nothing)
    a === nothing && return Pair{String,String}[]
    return ["x-ms-copy-source-authorization" => String(a)]
end

# --- properties, metadata, deletion ---------------------------------------------------------

"""
    get_blob_properties(blob_client; lease=nothing, version_id=nothing, etag=nothing, match_condition=nothing,
                        if_modified_since=nothing, if_unmodified_since=nothing, if_tags_match_condition=nothing,
                        cpk=nothing, timeout=nothing) -> BlobProperties
"""
function get_blob_properties(b::BlobClient; kwargs...)
    o = _opts(kwargs, _COND, (:lease, :cpk, :timeout, :version_id))
    q = _blob_query(b)
    v = get(o, :version_id, nothing)
    v === nothing || (filter!(p -> p.first != "versionid", q); push!(q, "versionid" => String(v)))
    h = vcat(_condition_headers(o), _cpk_headers(b.config, o))
    resp = _req(b, "HEAD", _blob_path(b); query=q, headers=h, timeout=_timeout(o), operation="get_blob_properties")
    return _blob_properties_from_headers(resp, b)
end

function exists(b::BlobClient; version_id=nothing, timeout=nothing)
    q = _blob_query(b)
    version_id === nothing || (filter!(p -> p.first != "versionid", q); push!(q, "versionid" => String(version_id)))
    resp = _req(b, "HEAD", _blob_path(b); query=q, timeout=timeout, raise=false)
    200 <= resp.status < 300 && return true
    code = _hdr(resp, "x-ms-error-code")
    code == "BlobUsesCustomerSpecifiedEncryption" && return true
    resp.status == 404 && return false
    throw(storage_error(resp; operation="exists"))
end

"""
    set_http_headers(blob_client, content_settings=nothing; lease=nothing, <access conditions>, timeout=nothing) -> Dict

Replace the blob's content headers (unset fields are cleared).
"""
function set_http_headers(b::BlobClient, content_settings=nothing; kwargs...)
    o = _opts(kwargs, _COND, (:lease, :timeout))
    h = vcat(_content_settings_headers(content_settings), _condition_headers(o))
    resp = _req(b, "PUT", _blob_path(b); query=["comp" => "properties"], headers=h, expected=(200,),
        timeout=_timeout(o), operation="set_http_headers")
    return response_dict(resp)
end

"""
    set_blob_metadata(blob_client, metadata=nothing; lease=nothing, <access conditions>, cpk=nothing,
                      encryption_scope=nothing, timeout=nothing) -> Dict

Replace the blob's metadata (`nothing` clears it).
"""
function set_blob_metadata(b::BlobClient, metadata=nothing; kwargs...)
    o = _opts(kwargs, _COND, _CPK, (:lease, :timeout))
    h = vcat(_metadata_headers(metadata), _condition_headers(o), _cpk_headers(b.config, o))
    resp = _req(b, "PUT", _blob_path(b); query=["comp" => "metadata"], headers=h, expected=(200,),
        timeout=_timeout(o), operation="set_blob_metadata")
    return response_dict(resp)
end

"""
    delete_blob(blob_client; delete_snapshots=nothing, version_id=nothing, lease=nothing, <access conditions>,
                timeout=nothing)

Delete the blob (or the client's snapshot/version). A blob with snapshots
can only be deleted with `delete_snapshots="include"` (blob and snapshots)
or `"only"` (just the snapshots).
"""
function delete_blob(b::BlobClient; kwargs...)
    o = _opts(kwargs, _COND, (:lease, :timeout, :delete_snapshots, :version_id))
    q = _blob_query(b)
    v = get(o, :version_id, nothing)
    v === nothing || (filter!(p -> p.first != "versionid", q); push!(q, "versionid" => String(v)))
    h = _condition_headers(o)
    ds = get(o, :delete_snapshots, nothing)
    if ds !== nothing
        ds in ("include", "only") || throw(ArgumentError("delete_snapshots must be \"include\" or \"only\""))
        push!(h, "x-ms-delete-snapshots" => ds)
    end
    _req(b, "DELETE", _blob_path(b); query=q, headers=h, expected=(202,), timeout=_timeout(o), operation="delete_blob")
    return nothing
end

"""
    undelete_blob(blob_client; timeout=nothing)

Restore a soft-deleted blob and its soft-deleted snapshots.
"""
function undelete_blob(b::BlobClient; timeout=nothing)
    _req(b, "PUT", _blob_path(b); query=["comp" => "undelete"], expected=(200,), timeout=timeout, operation="undelete_blob")
    return nothing
end

"""
    create_snapshot(blob_client; metadata=nothing, lease=nothing, <access conditions>, cpk=nothing,
                    encryption_scope=nothing, timeout=nothing) -> Dict

Create a read-only snapshot; the result's `"snapshot"` entry identifies it
(use it with `get_blob_client(...; snapshot=...)`).
"""
function create_snapshot(b::BlobClient; metadata=nothing, kwargs...)
    o = _opts(kwargs, _COND, _CPK, (:lease, :timeout))
    h = vcat(_metadata_headers(metadata), _condition_headers(o), _cpk_headers(b.config, o))
    resp = _req(b, "PUT", _blob_path(b); query=["comp" => "snapshot"], headers=h, expected=(201,),
        timeout=_timeout(o), operation="create_snapshot")
    return response_dict(resp)
end

# --- copy ------------------------------------------------------------------------------------

"""
    start_copy_from_url(blob_client, source_url; metadata=nothing, incremental_copy=false, requires_sync=nothing,
                        tags=nothing, standard_blob_tier=nothing, premium_page_blob_tier=nothing,
                        rehydrate_priority=nothing, seal_destination_blob=nothing, source_authorization=nothing,
                        immutability_policy=nothing, legal_hold=nothing, destination_lease=nothing, source_lease=nothing,
                        <access conditions>, <source access conditions>, encryption_scope=nothing, timeout=nothing) -> Dict

Copy a blob (or any public URL / URL with SAS) into this blob. Asynchronous
unless `requires_sync=true` (sources up to 256 MiB). Returns `"copy_id"` and
`"copy_status"`; poll `get_blob_properties(...).copy` to track progress.
"""
function start_copy_from_url(b::BlobClient, source_url::AbstractString; kwargs...)
    o = _opts(kwargs, _COND, _SOURCE_COND, (:metadata, :incremental_copy, :requires_sync, :tags, :standard_blob_tier,
            :premium_page_blob_tier, :rehydrate_priority, :seal_destination_blob, :source_authorization,
            :immutability_policy, :legal_hold, :destination_lease, :source_lease, :encryption_scope, :timeout))
    incremental = get(o, :incremental_copy, false)
    h = Pair{String,String}["x-ms-copy-source" => String(source_url)]
    append!(h, _metadata_headers(get(o, :metadata, nothing)))
    o2 = Dict{Symbol,Any}(o)
    o2[:lease] = get(o, :destination_lease, nothing)
    append!(h, _condition_headers(o2))
    if incremental
        resp = _req(b, "PUT", _blob_path(b); query=["comp" => "incrementalcopy"], headers=h, expected=(202,),
            timeout=_timeout(o), operation="start_copy_from_url")
        return response_dict(resp)
    end
    append!(h, _source_condition_headers(o))
    append!(h, _tags_header(get(o, :tags, nothing)))
    tier = something(get(o, :standard_blob_tier, nothing), get(o, :premium_page_blob_tier, nothing), Some(nothing))
    tier === nothing || push!(h, "x-ms-access-tier" => enum_value(tier))
    rp = get(o, :rehydrate_priority, nothing)
    rp === nothing || push!(h, "x-ms-rehydrate-priority" => enum_value(rp))
    get(o, :seal_destination_blob, nothing) === true && push!(h, "x-ms-seal-blob" => "true")
    sl = _lease_id(get(o, :source_lease, nothing))
    sl === nothing || push!(h, "x-ms-source-lease-id" => sl)
    append!(h, _immutability_headers(o))
    scope = get(o, :encryption_scope, nothing)
    scope === nothing || push!(h, "x-ms-encryption-scope" => String(scope))
    if get(o, :requires_sync, nothing) === true
        push!(h, "x-ms-requires-sync" => "true")
        append!(h, _source_auth_header(o))
    elseif haskey(o, :source_authorization)
        throw(ArgumentError("source_authorization requires requires_sync=true"))
    end
    resp = _req(b, "PUT", _blob_path(b); headers=h, expected=(202,), timeout=_timeout(o), operation="start_copy_from_url")
    return response_dict(resp)
end

"""
    abort_copy(blob_client, copy_id; lease=nothing, timeout=nothing)

Abort a pending copy (`copy_id` is a string, a result dict or `BlobProperties`).
"""
function abort_copy(b::BlobClient, copy_id; lease=nothing, timeout=nothing)
    id = copy_id isa AbstractString ? String(copy_id) : copy_id isa BlobProperties ? copy_id.copy.id :
         String(copy_id["copy_id"])
    h = Pair{String,String}["x-ms-copy-action" => "abort"]
    lid = _lease_id(lease)
    lid === nothing || push!(h, "x-ms-lease-id" => lid)
    _req(b, "PUT", _blob_path(b); query=["comp" => "copy", "copyid" => id], headers=h, expected=(204,),
        timeout=timeout, operation="abort_copy")
    return nothing
end

"""
    upload_blob_from_url(blob_client, source_url; overwrite=false, include_source_blob_properties=true,
                         content_settings=nothing, metadata=nothing, tags=nothing, standard_blob_tier=nothing,
                         source_authorization=nothing, source_content_md5=nothing, lease=nothing,
                         <access conditions>, <source access conditions>, cpk=nothing, encryption_scope=nothing,
                         timeout=nothing) -> Dict

Create a block blob from the content of `source_url` in one synchronous
server-side operation (Put Blob From URL; sources up to 5000 MiB).
"""
function upload_blob_from_url(b::BlobClient, source_url::AbstractString; overwrite::Bool=false, kwargs...)
    o = _opts(kwargs, _COND, _SOURCE_COND, _CPK, (:include_source_blob_properties, :content_settings, :metadata, :tags,
            :standard_blob_tier, :source_authorization, :source_content_md5, :lease, :timeout))
    h = Pair{String,String}["x-ms-blob-type" => "BlockBlob", "x-ms-copy-source" => String(source_url),
        "x-ms-copy-source-blob-properties" => get(o, :include_source_blob_properties, true) ? "true" : "false"]
    append!(h, _content_settings_headers(get(o, :content_settings, nothing)))
    append!(h, _metadata_headers(get(o, :metadata, nothing)))
    append!(h, _tags_header(get(o, :tags, nothing)))
    tier = get(o, :standard_blob_tier, nothing)
    tier === nothing || push!(h, "x-ms-access-tier" => enum_value(tier))
    md = get(o, :source_content_md5, nothing)
    md === nothing || push!(h, "x-ms-source-content-md5" => base64encode(md))
    overwrite || haskey(o, :match_condition) || push!(h, "If-None-Match" => "*")
    append!(h, _condition_headers(o))
    append!(h, _source_condition_headers(o))
    append!(h, _source_auth_header(o))
    append!(h, _cpk_headers(b.config, o))
    resp = _req(b, "PUT", _blob_path(b); headers=h, expected=(201,), timeout=_timeout(o), operation="upload_blob_from_url")
    return response_dict(resp)
end

# --- tiers, tags, immutability -----------------------------------------------------------------

"""
    set_standard_blob_tier(blob_client, standard_blob_tier; rehydrate_priority=nothing, version_id=nothing,
                           lease=nothing, if_tags_match_condition=nothing, timeout=nothing)

Set the access tier (`StandardBlobTier.HOT`, `COOL`, `COLD` or `ARCHIVE`) of a block blob.
"""
function set_standard_blob_tier(b::BlobClient, tier; kwargs...)
    o = _opts(kwargs, (:rehydrate_priority, :version_id, :lease, :if_tags_match_condition, :timeout))
    tier === nothing && throw(ArgumentError("A StandardBlobTier must be specified"))
    _set_tier(b, enum_value(tier), o)
end

"""
    set_premium_page_blob_tier(blob_client, premium_page_blob_tier; lease=nothing, if_tags_match_condition=nothing,
                               timeout=nothing)
"""
function set_premium_page_blob_tier(b::BlobClient, tier; kwargs...)
    o = _opts(kwargs, (:lease, :if_tags_match_condition, :timeout))
    tier === nothing && throw(ArgumentError("A PremiumPageBlobTier must be specified"))
    _set_tier(b, enum_value(tier), o)
end

function _set_tier(b::BlobClient, tier::String, o)
    q = _blob_query(b)
    v = get(o, :version_id, nothing)
    v === nothing || (filter!(p -> p.first != "versionid", q); push!(q, "versionid" => String(v)))
    push!(q, "comp" => "tier")
    h = Pair{String,String}["x-ms-access-tier" => tier]
    rp = get(o, :rehydrate_priority, nothing)
    rp === nothing || push!(h, "x-ms-rehydrate-priority" => enum_value(rp))
    append!(h, _condition_headers(o))
    _req(b, "PUT", _blob_path(b); query=q, headers=h, expected=(200, 202), timeout=_timeout(o), operation="set_blob_tier")
    return nothing
end

"""
    set_blob_tags(blob_client, tags=nothing; version_id=nothing, validate_content=false, lease=nothing,
                  if_tags_match_condition=nothing, timeout=nothing) -> Dict

Replace the blob's index tags (up to 10; `nothing` removes all tags).
"""
function set_blob_tags(b::BlobClient, tags=nothing; kwargs...)
    o = _opts(kwargs, (:version_id, :validate_content, :lease, :if_tags_match_condition, :timeout))
    tags !== nothing && length(tags) > 10 && throw(ArgumentError("a blob can have at most 10 tags"))
    items = tags === nothing ? "" :
            join(xml_element("Tag", RawXML(xml_element("Key", string(k)) * xml_element("Value", string(v)))) for (k, v) in tags)
    body = xml_document("Tags", RawXML(xml_element("TagSet", RawXML(items))))
    q = Pair{String,String}["comp" => "tags"]
    v = something(get(o, :version_id, nothing), b.version_id, Some(nothing))
    v === nothing || push!(q, "versionid" => String(v))
    validate = get(o, :validate_content, false)
    h = vcat(Pair{String,String}["Content-Type" => "application/xml"], _condition_headers(o),
        _md5_header(validate, codeunits(body)))
    resp = _req(b, "PUT", _blob_path(b); query=q, headers=h, body=body, expected=(204,), timeout=_timeout(o),
        operation="set_blob_tags")
    return response_dict(resp)
end

"""
    get_blob_tags(blob_client; version_id=nothing, lease=nothing, if_tags_match_condition=nothing,
                  timeout=nothing) -> Dict{String,String}
"""
function get_blob_tags(b::BlobClient; kwargs...)
    o = _opts(kwargs, (:version_id, :lease, :if_tags_match_condition, :timeout))
    q = _blob_query(b)
    v = get(o, :version_id, nothing)
    v === nothing || (filter!(p -> p.first != "versionid", q); push!(q, "versionid" => String(v)))
    push!(q, "comp" => "tags")
    resp = _req(b, "GET", _blob_path(b); query=q, headers=_condition_headers(o), expected=(200,),
        timeout=_timeout(o), operation="get_blob_tags")
    return something(_xtags(xml_parse(resp.body)), Dict{String,String}())
end

"""
    set_immutability_policy(blob_client, immutability_policy::ImmutabilityPolicy; version_id=nothing,
                            timeout=nothing) -> Dict
"""
function set_immutability_policy(b::BlobClient, policy::ImmutabilityPolicy; version_id=nothing, timeout=nothing)
    q = Pair{String,String}["comp" => "immutabilityPolicies"]
    v = something(version_id, b.version_id, Some(nothing))
    v === nothing || push!(q, "versionid" => String(v))
    policy.expiry_time === nothing && throw(ArgumentError("immutability_policy.expiry_time is required"))
    h = ["x-ms-immutability-policy-until-date" => rfc1123(to_utc(policy.expiry_time)),
        "x-ms-immutability-policy-mode" => enum_value(something(policy.policy_mode, BlobImmutabilityPolicyMode.UNLOCKED))]
    resp = _req(b, "PUT", _blob_path(b); query=q, headers=h, expected=(200,), timeout=timeout,
        operation="set_immutability_policy")
    return Dict{String,Any}("immutability_policy_until_date" => _hdate(resp, "x-ms-immutability-policy-until-date"),
        "immutability_policy_mode" => _hdr(resp, "x-ms-immutability-policy-mode"))
end

"""
    delete_immutability_policy(blob_client; version_id=nothing, timeout=nothing)
"""
function delete_immutability_policy(b::BlobClient; version_id=nothing, timeout=nothing)
    q = Pair{String,String}["comp" => "immutabilityPolicies"]
    v = something(version_id, b.version_id, Some(nothing))
    v === nothing || push!(q, "versionid" => String(v))
    _req(b, "DELETE", _blob_path(b); query=q, expected=(200,), timeout=timeout, operation="delete_immutability_policy")
    return nothing
end

"""
    set_legal_hold(blob_client, legal_hold::Bool; version_id=nothing, timeout=nothing) -> Dict
"""
function set_legal_hold(b::BlobClient, legal_hold::Bool; version_id=nothing, timeout=nothing)
    q = Pair{String,String}["comp" => "legalhold"]
    v = something(version_id, b.version_id, Some(nothing))
    v === nothing || push!(q, "versionid" => String(v))
    resp = _req(b, "PUT", _blob_path(b); query=q, headers=["x-ms-legal-hold" => legal_hold ? "true" : "false"],
        expected=(200,), timeout=timeout, operation="set_legal_hold")
    return Dict{String,Any}("legal_hold" => _hbool(resp, "x-ms-legal-hold"))
end

# --- block blobs ------------------------------------------------------------------------------

"""
    stage_block(blob_client, block_id, data; length=nothing, validate_content=false, lease=nothing,
                cpk=nothing, encryption_scope=nothing, timeout=nothing) -> Dict

Upload one uncommitted block (up to 4000 MiB). `block_id` is any string;
all block ids of a blob must have the same length.
"""
function stage_block(b::BlobClient, block_id, data; kwargs...)
    o = _opts(kwargs, _CPK, (:length, :validate_content, :lease, :timeout))
    body = _as_bytes(data, get(o, :length, nothing))
    validate = get(o, :validate_content, false)
    h = vcat(_md5_header(validate, body), _cpk_headers(b.config, o))
    lid = _lease_id(get(o, :lease, nothing))
    lid === nothing || push!(h, "x-ms-lease-id" => lid)
    resp = _req(b, "PUT", _blob_path(b); query=["comp" => "block", "blockid" => _encode_block_id(block_id)], headers=h,
        body=body, expected=(201,), timeout=_timeout(o), operation="stage_block")
    _check_md5_response(validate, body, resp)
    return response_dict(resp)
end

"""
    stage_block_from_url(blob_client, block_id, source_url; source_offset=nothing, source_length=nothing,
                         source_content_md5=nothing, source_authorization=nothing, lease=nothing, cpk=nothing,
                         encryption_scope=nothing, timeout=nothing) -> Dict
"""
function stage_block_from_url(b::BlobClient, block_id, source_url::AbstractString; kwargs...)
    o = _opts(kwargs, _CPK, (:source_offset, :source_length, :source_content_md5, :source_authorization, :lease, :timeout))
    h = Pair{String,String}["x-ms-copy-source" => String(source_url)]
    off = get(o, :source_offset, nothing)
    len = get(o, :source_length, nothing)
    if off !== nothing
        push!(h, "x-ms-source-range" => len === nothing ? "bytes=$(off)-" : _range_header(off, len))
    end
    md = get(o, :source_content_md5, nothing)
    md === nothing || push!(h, "x-ms-source-content-md5" => base64encode(md))
    append!(h, _source_auth_header(o))
    append!(h, _cpk_headers(b.config, o))
    lid = _lease_id(get(o, :lease, nothing))
    lid === nothing || push!(h, "x-ms-lease-id" => lid)
    resp = _req(b, "PUT", _blob_path(b); query=["comp" => "block", "blockid" => _encode_block_id(block_id)], headers=h,
        expected=(201,), timeout=_timeout(o), operation="stage_block_from_url")
    return response_dict(resp)
end

"""
    get_block_list(blob_client, block_list_type="committed"; lease=nothing, if_tags_match_condition=nothing,
                   timeout=nothing) -> (committed::Vector{BlobBlock}, uncommitted::Vector{BlobBlock})

`block_list_type` is `"committed"`, `"uncommitted"` or `"all"`.
"""
function get_block_list(b::BlobClient, block_list_type::AbstractString="committed"; kwargs...)
    o = _opts(kwargs, (:lease, :if_tags_match_condition, :timeout))
    lowercase(block_list_type) in ("committed", "uncommitted", "all") ||
        throw(ArgumentError("block_list_type must be \"committed\", \"uncommitted\" or \"all\""))
    q = vcat(_blob_query(b), ["comp" => "blocklist", "blocklisttype" => lowercase(block_list_type)])
    h = vcat(_condition_headers(o))
    resp = _req(b, "GET", _blob_path(b); query=q, headers=h, expected=(200,), timeout=_timeout(o),
        operation="get_block_list")
    x = xml_parse(resp.body)
    parse_blocks(tag, state) = [BlobBlock(_decode_block_id(something(_xstr(e, "Name"), "")), state, _xint(e, "Size"))
                                for e in xfindall(xfind(x, tag), "Block")]
    return parse_blocks("CommittedBlocks", BlockState.COMMITTED), parse_blocks("UncommittedBlocks", BlockState.UNCOMMITTED)
end

function _block_list_xml(block_list)
    items = String[]
    for blk in block_list
        id, state = blk isa BlobBlock ? (blk.id, blk.state) : (string(blk), BlockState.LATEST)
        push!(items, xml_element(enum_value(state), _encode_block_id(id)))
    end
    return xml_document("BlockList", RawXML(join(items)))
end

"""
    commit_block_list(blob_client, block_list; content_settings=nothing, metadata=nothing, validate_content=false,
                      tags=nothing, standard_blob_tier=nothing, immutability_policy=nothing, legal_hold=nothing,
                      lease=nothing, <access conditions>, cpk=nothing, encryption_scope=nothing, timeout=nothing) -> Dict

Commit blocks (a vector of [`BlobBlock`](@ref)s or block id strings) as the
blob's content.
"""
function commit_block_list(b::BlobClient, block_list; kwargs...)
    o = _opts(kwargs, _COND, _CPK, (:content_settings, :metadata, :validate_content, :tags, :standard_blob_tier,
            :immutability_policy, :legal_hold, :lease, :timeout))
    body = _block_list_xml(block_list)
    validate = get(o, :validate_content, false)
    h = vcat(Pair{String,String}["Content-Type" => "application/xml"], _content_settings_headers(get(o, :content_settings, nothing)),
        _metadata_headers(get(o, :metadata, nothing)), _tags_header(get(o, :tags, nothing)), _condition_headers(o),
        _cpk_headers(b.config, o), _immutability_headers(o), _md5_header(validate, codeunits(body)))
    tier = get(o, :standard_blob_tier, nothing)
    tier === nothing || push!(h, "x-ms-access-tier" => enum_value(tier))
    resp = _req(b, "PUT", _blob_path(b); query=["comp" => "blocklist"], headers=h, body=body, expected=(201,),
        timeout=_timeout(o), operation="commit_block_list")
    return response_dict(resp)
end

# --- page blobs ---------------------------------------------------------------------------------

function _check_page_range(offset, length)
    offset % 512 == 0 || throw(ArgumentError("offset must be a multiple of 512"))
    length % 512 == 0 || throw(ArgumentError("length must be a multiple of 512"))
    length > 0 || throw(ArgumentError("length must be positive"))
end

"""
    create_page_blob(blob_client, size; content_settings=nothing, metadata=nothing, premium_page_blob_tier=nothing,
                     sequence_number=nothing, tags=nothing, immutability_policy=nothing, legal_hold=nothing,
                     lease=nothing, <access conditions>, cpk=nothing, encryption_scope=nothing, timeout=nothing) -> Dict

Create an empty page blob of `size` bytes (a multiple of 512).
"""
function create_page_blob(b::BlobClient, size::Integer; kwargs...)
    o = _opts(kwargs, _COND, _CPK, (:content_settings, :metadata, :premium_page_blob_tier, :sequence_number, :tags,
            :immutability_policy, :legal_hold, :lease, :timeout))
    size % 512 == 0 || throw(ArgumentError("page blob size must be a multiple of 512"))
    h = Pair{String,String}["x-ms-blob-type" => "PageBlob", "x-ms-blob-content-length" => string(size)]
    sn = get(o, :sequence_number, nothing)
    sn === nothing || push!(h, "x-ms-blob-sequence-number" => string(sn))
    tier = get(o, :premium_page_blob_tier, nothing)
    tier === nothing || push!(h, "x-ms-access-tier" => enum_value(tier))
    append!(h, vcat(_content_settings_headers(get(o, :content_settings, nothing)), _metadata_headers(get(o, :metadata, nothing)),
        _tags_header(get(o, :tags, nothing)), _condition_headers(o), _cpk_headers(b.config, o), _immutability_headers(o)))
    resp = _req(b, "PUT", _blob_path(b); headers=h, expected=(201,), timeout=_timeout(o), operation="create_page_blob")
    return response_dict(resp)
end

"""
    upload_page(blob_client, page, offset, length; validate_content=false, lease=nothing,
                if_sequence_number_lte=nothing, if_sequence_number_lt=nothing, if_sequence_number_eq=nothing,
                <access conditions>, cpk=nothing, encryption_scope=nothing, timeout=nothing) -> Dict

Write `length` bytes of `page` at `offset` (both multiples of 512, at most 4 MiB).
"""
function upload_page(b::BlobClient, page, offset::Integer, length::Integer; kwargs...)
    o = _opts(kwargs, _COND, _CPK, _SEQ, (:validate_content, :lease, :timeout))
    _check_page_range(offset, length)
    body = _as_bytes(page, length)
    validate = get(o, :validate_content, false)
    h = Pair{String,String}["x-ms-page-write" => "update", "x-ms-range" => _range_header(offset, length)]
    append!(h, vcat(_md5_header(validate, body), _seq_headers(o), _condition_headers(o), _cpk_headers(b.config, o)))
    resp = _req(b, "PUT", _blob_path(b); query=["comp" => "page"], headers=h, body=body, expected=(201,),
        timeout=_timeout(o), operation="upload_page")
    _check_md5_response(validate, body, resp)
    return response_dict(resp)
end

"""
    upload_pages_from_url(blob_client, source_url, offset, length, source_offset; source_content_md5=nothing,
                          source_authorization=nothing, lease=nothing, <sequence/access/source conditions>,
                          cpk=nothing, encryption_scope=nothing, timeout=nothing) -> Dict
"""
function upload_pages_from_url(b::BlobClient, source_url::AbstractString, offset::Integer, length::Integer,
    source_offset::Integer; kwargs...)
    o = _opts(kwargs, _COND, _SOURCE_COND, _CPK, _SEQ, (:source_content_md5, :source_authorization, :lease, :timeout))
    _check_page_range(offset, length)
    h = Pair{String,String}["x-ms-page-write" => "update", "x-ms-range" => _range_header(offset, length),
        "x-ms-copy-source" => String(source_url), "x-ms-source-range" => _range_header(source_offset, length)]
    md = get(o, :source_content_md5, nothing)
    md === nothing || push!(h, "x-ms-source-content-md5" => base64encode(md))
    append!(h, vcat(_seq_headers(o), _condition_headers(o), _source_condition_headers(o), _source_auth_header(o),
        _cpk_headers(b.config, o)))
    resp = _req(b, "PUT", _blob_path(b); query=["comp" => "page"], headers=h, expected=(201,), timeout=_timeout(o),
        operation="upload_pages_from_url")
    return response_dict(resp)
end

"""
    clear_page(blob_client, offset, length; lease=nothing, <sequence/access conditions>, cpk=nothing,
               timeout=nothing) -> Dict
"""
function clear_page(b::BlobClient, offset::Integer, length::Integer; kwargs...)
    o = _opts(kwargs, _COND, _CPK, _SEQ, (:lease, :timeout))
    _check_page_range(offset, length)
    h = Pair{String,String}["x-ms-page-write" => "clear", "x-ms-range" => _range_header(offset, length)]
    append!(h, vcat(_seq_headers(o), _condition_headers(o), _cpk_headers(b.config, o)))
    resp = _req(b, "PUT", _blob_path(b); query=["comp" => "page"], headers=h, expected=(201,), timeout=_timeout(o),
        operation="clear_page")
    return response_dict(resp)
end

function _page_list(b::BlobClient, o, marker, results_per_page)
    q = vcat(_blob_query(b), ["comp" => "pagelist"])
    prev = get(o, :previous_snapshot_diff, nothing)
    prev === nothing || push!(q, "prevsnapshot" => _snapshot_id(prev))
    marker === nothing || push!(q, "marker" => marker)
    results_per_page === nothing || push!(q, "maxresults" => string(results_per_page))
    h = _condition_headers(o)
    off = get(o, :offset, nothing)
    len = get(o, :length, nothing)
    if off !== nothing || len !== nothing
        o1 = something(off, 0)
        push!(h, "x-ms-range" => len === nothing ? "bytes=$(o1)-" : _range_header(o1, len))
    end
    resp = _req(b, "GET", _blob_path(b); query=q, headers=h, expected=(200,), timeout=_timeout(o),
        operation="get_page_ranges")
    x = xml_parse(resp.body)
    ranges = PageRange[]
    for e in xfindall(x)
        e.tag in ("PageRange", "ClearRange") || continue
        push!(ranges, PageRange(something(_xint(e, "Start"), 0), something(_xint(e, "End"), 0); cleared=e.tag == "ClearRange"))
    end
    sort!(ranges; by=r -> r.start)
    return ranges, _xstr(x, "NextMarker")
end

const _PAGE_LIST_OPTS = (:offset, :length, :previous_snapshot_diff, :lease, :timeout)

"""
    get_page_ranges(blob_client; offset=nothing, length=nothing, previous_snapshot_diff=nothing, lease=nothing,
                    <access conditions>, timeout=nothing) -> (filled::Vector{PageRange}, cleared::Vector{PageRange})

Valid page ranges of a page blob, or with `previous_snapshot_diff` (a
snapshot id) the ranges changed (`filled`) and cleared since that snapshot.
"""
function get_page_ranges(b::BlobClient; kwargs...)
    o = _opts(kwargs, _COND, _PAGE_LIST_OPTS)
    filled, cleared = PageRange[], PageRange[]
    marker = nothing
    while true
        ranges, marker = _page_list(b, o, marker, nothing)
        for r in ranges
            push!(r.cleared ? cleared : filled, r)
        end
        marker === nothing && break
    end
    return filled, cleared
end

"""
    list_page_ranges(blob_client; offset=nothing, length=nothing, previous_snapshot=nothing, results_per_page=nothing,
                     lease=nothing, <access conditions>, timeout=nothing) -> ItemPaged{PageRange}
"""
function list_page_ranges(b::BlobClient; previous_snapshot=nothing, results_per_page=nothing, kwargs...)
    o = _opts(kwargs, _COND, _PAGE_LIST_OPTS)
    previous_snapshot === nothing || (o[:previous_snapshot_diff] = previous_snapshot)
    return ItemPaged{PageRange}(marker -> _page_list(b, o, marker, results_per_page))
end

"""
    resize_blob(blob_client, size; premium_page_blob_tier=nothing, lease=nothing, <access conditions>,
                timeout=nothing) -> Dict

Resize a page blob (to a multiple of 512 bytes).
"""
function resize_blob(b::BlobClient, size::Integer; kwargs...)
    o = _opts(kwargs, _COND, _CPK, (:premium_page_blob_tier, :lease, :timeout))
    size % 512 == 0 || throw(ArgumentError("page blob size must be a multiple of 512"))
    h = vcat(Pair{String,String}["x-ms-blob-content-length" => string(size)], _condition_headers(o), _cpk_headers(b.config, o))
    resp = _req(b, "PUT", _blob_path(b); query=["comp" => "properties"], headers=h, expected=(200,),
        timeout=_timeout(o), operation="resize_blob")
    return response_dict(resp)
end

"""
    set_sequence_number(blob_client, sequence_number_action, sequence_number=nothing; lease=nothing,
                        <access conditions>, timeout=nothing) -> Dict

Change a page blob's sequence number ([`SequenceNumberAction`](@ref)).
"""
function set_sequence_number(b::BlobClient, action, sequence_number=nothing; kwargs...)
    o = _opts(kwargs, _COND, (:lease, :timeout))
    act = enum_value(action)
    act in ("max", "update", "increment") || throw(ArgumentError("invalid sequence number action $(repr(act))"))
    (act != "increment" && sequence_number === nothing) &&
        throw(ArgumentError("sequence_number is required for the \"$(act)\" action"))
    h = Pair{String,String}["x-ms-sequence-number-action" => act]
    sequence_number === nothing || push!(h, "x-ms-blob-sequence-number" => string(sequence_number))
    append!(h, _condition_headers(o))
    resp = _req(b, "PUT", _blob_path(b); query=["comp" => "properties"], headers=h, expected=(200,),
        timeout=_timeout(o), operation="set_sequence_number")
    return response_dict(resp)
end

# --- append blobs --------------------------------------------------------------------------------

"""
    create_append_blob(blob_client; content_settings=nothing, metadata=nothing, tags=nothing,
                       immutability_policy=nothing, legal_hold=nothing, lease=nothing, <access conditions>,
                       cpk=nothing, encryption_scope=nothing, timeout=nothing) -> Dict

Create (or replace) an empty append blob.
"""
function create_append_blob(b::BlobClient; kwargs...)
    o = _opts(kwargs, _COND, _CPK, (:content_settings, :metadata, :tags, :immutability_policy, :legal_hold, :lease, :timeout))
    h = vcat(Pair{String,String}["x-ms-blob-type" => "AppendBlob"], _content_settings_headers(get(o, :content_settings, nothing)),
        _metadata_headers(get(o, :metadata, nothing)), _tags_header(get(o, :tags, nothing)), _condition_headers(o),
        _cpk_headers(b.config, o), _immutability_headers(o))
    resp = _req(b, "PUT", _blob_path(b); headers=h, expected=(201,), timeout=_timeout(o), operation="create_append_blob")
    return response_dict(resp)
end

function _append_headers(o)
    h = Pair{String,String}[]
    ms = get(o, :maxsize_condition, nothing)
    ms === nothing || push!(h, "x-ms-blob-condition-maxsize" => string(ms))
    ap = get(o, :appendpos_condition, nothing)
    ap === nothing || push!(h, "x-ms-blob-condition-appendpos" => string(ap))
    return h
end

"""
    append_block(blob_client, data; length=nothing, validate_content=false, maxsize_condition=nothing,
                 appendpos_condition=nothing, lease=nothing, <access conditions>, cpk=nothing,
                 encryption_scope=nothing, timeout=nothing) -> Dict

Append a block (up to 100 MiB) to an append blob. The result's
`"blob_append_offset"` is where the block was written.
"""
function append_block(b::BlobClient, data; kwargs...)
    o = _opts(kwargs, _COND, _CPK, (:length, :validate_content, :maxsize_condition, :appendpos_condition, :lease, :timeout))
    body = _as_bytes(data, get(o, :length, nothing))
    validate = get(o, :validate_content, false)
    h = vcat(_md5_header(validate, body), _append_headers(o), _condition_headers(o), _cpk_headers(b.config, o))
    resp = _req(b, "PUT", _blob_path(b); query=["comp" => "appendblock"], headers=h, body=body, expected=(201,),
        timeout=_timeout(o), operation="append_block")
    _check_md5_response(validate, body, resp)
    return response_dict(resp)
end

"""
    append_block_from_url(blob_client, copy_source_url; source_offset=nothing, source_length=nothing,
                          source_content_md5=nothing, source_authorization=nothing, maxsize_condition=nothing,
                          appendpos_condition=nothing, lease=nothing, <access/source conditions>, cpk=nothing,
                          encryption_scope=nothing, timeout=nothing) -> Dict
"""
function append_block_from_url(b::BlobClient, copy_source_url::AbstractString; kwargs...)
    o = _opts(kwargs, _COND, _SOURCE_COND, _CPK, (:source_offset, :source_length, :source_content_md5,
            :source_authorization, :maxsize_condition, :appendpos_condition, :lease, :timeout))
    h = Pair{String,String}["x-ms-copy-source" => String(copy_source_url)]
    off = get(o, :source_offset, nothing)
    len = get(o, :source_length, nothing)
    off === nothing || push!(h, "x-ms-source-range" => len === nothing ? "bytes=$(off)-" : _range_header(off, len))
    md = get(o, :source_content_md5, nothing)
    md === nothing || push!(h, "x-ms-source-content-md5" => base64encode(md))
    append!(h, vcat(_append_headers(o), _condition_headers(o), _source_condition_headers(o), _source_auth_header(o),
        _cpk_headers(b.config, o)))
    resp = _req(b, "PUT", _blob_path(b); query=["comp" => "appendblock"], headers=h, expected=(201,),
        timeout=_timeout(o), operation="append_block_from_url")
    return response_dict(resp)
end

"""
    seal_append_blob(blob_client; appendpos_condition=nothing, lease=nothing, <access conditions>, timeout=nothing) -> Dict

Make an append blob read-only.
"""
function seal_append_blob(b::BlobClient; kwargs...)
    o = _opts(kwargs, _COND, (:appendpos_condition, :lease, :timeout))
    h = vcat(_append_headers(o), _condition_headers(o))
    resp = _req(b, "PUT", _blob_path(b); query=["comp" => "seal"], headers=h, expected=(200,), timeout=_timeout(o),
        operation="seal_append_blob")
    return response_dict(resp)
end
