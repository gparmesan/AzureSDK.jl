# BlobServiceClient operations.

_req(c::AnyBlobClient, method, path; kw...) = storage_request(c.config, method, path; kw...)

"""
    get_account_information(client) -> Dict

SKU name, account kind and hierarchical-namespace flag of the storage
account (`"sku_name"`, `"account_kind"`, `"is_hns_enabled"`). Works on any
blob client.
"""
function get_account_information(c::AnyBlobClient; timeout = nothing)
    path = c isa BlobServiceClient ? "/" : c isa ContainerClient ? _container_path(c) : _blob_path(c)
    resp = _req(
        c, "GET", path; query = ["restype" => "account", "comp" => "properties"], timeout = timeout,
        operation = "get_account_information"
    )
    d = response_dict(resp)
    d["sku_name"] = _hdr(resp, "x-ms-sku-name")
    d["account_kind"] = _hdr(resp, "x-ms-account-kind")
    d["is_hns_enabled"] = _hbool(resp, "x-ms-is-hns-enabled")
    return d
end

"""
    list_containers(service; name_starts_with=nothing, include_metadata=false, include_deleted=false,
                    include_system=false, results_per_page=nothing, timeout=nothing) -> ItemPaged{ContainerProperties}

Lazily list the containers of the account.
"""
function list_containers(
        s::BlobServiceClient; name_starts_with = nothing, include_metadata::Bool = false,
        include_deleted::Bool = false, include_system::Bool = false, results_per_page = nothing, timeout = nothing
    )
    include = String[]
    include_metadata && push!(include, "metadata")
    include_deleted && push!(include, "deleted")
    include_system && push!(include, "system")
    fetch = function (marker)
        q = Pair{String, String}["comp" => "list"]
        name_starts_with === nothing || push!(q, "prefix" => String(name_starts_with))
        marker === nothing || push!(q, "marker" => marker)
        results_per_page === nothing || push!(q, "maxresults" => string(results_per_page))
        isempty(include) || push!(q, "include" => join(include, ","))
        resp = _req(s, "GET", "/"; query = q, timeout = timeout, operation = "list_containers")
        x = xml_parse(resp.body)
        items = [_container_properties_from_xml(e) for e in xfindall(xfind(x, "Containers"), "Container")]
        next = _xstr(x, "NextMarker")
        return items, next
    end
    return ItemPaged{ContainerProperties}(fetch)
end

"""
    create_container(service, name; metadata=nothing, public_access=nothing,
                     container_encryption_scope=nothing, timeout=nothing) -> ContainerClient

Create a container and return a client for it. Raises `ResourceExistsError`
if it already exists.
"""
function create_container(s::BlobServiceClient, name; kwargs...)
    c = get_container_client(s, name)
    create_container(c; kwargs...)
    return c
end

"""
    delete_container(service, container; lease=nothing, if_modified_since=nothing, if_unmodified_since=nothing,
                     etag=nothing, match_condition=nothing, timeout=nothing)

Mark a container for deletion. `container` is a name or `ContainerProperties`.
"""
delete_container(s::BlobServiceClient, container; kwargs...) = delete_container(get_container_client(s, container); kwargs...)

"""
    undelete_container(service, deleted_container_name, deleted_container_version; timeout=nothing) -> ContainerClient

Restore a soft-deleted container (see `list_containers(...; include_deleted=true)`).
"""
function undelete_container(
        s::BlobServiceClient, deleted_container_name::AbstractString,
        deleted_container_version::AbstractString; timeout = nothing
    )
    c = get_container_client(s, deleted_container_name)
    _req(
        s, "PUT", _container_path(c); query = ["restype" => "container", "comp" => "undelete"],
        headers = [
            "x-ms-deleted-container-name" => String(deleted_container_name),
            "x-ms-deleted-container-version" => String(deleted_container_version),
        ],
        expected = (201,), timeout = timeout, operation = "undelete_container"
    )
    return c
end

# --- service properties ---------------------------------------------------------------

_xml_retention(tag, r::RetentionPolicy) =
    xml_element(tag, [xml_element("Enabled", r.enabled), r.enabled ? xml_element("Days", r.days) : ""] .|> RawXML)
_xml_metrics(tag, m::Metrics) = xml_element(
    tag, RawXML(
        string(
            xml_element("Version", m.version),
            xml_element("Enabled", m.enabled), m.enabled ? xml_element("IncludeAPIs", something(m.include_apis, false)) : "",
            _xml_retention("RetentionPolicy", m.retention_policy)
        )
    )
)

function _parse_retention(e)
    e === nothing && return RetentionPolicy()
    d = _xint(e, "Days")
    return RetentionPolicy(enabled = something(_xbool(e, "Enabled"), false), days = d === nothing ? nothing : Int(d))
end
_parse_metrics(e) = e === nothing ? nothing :
    Metrics(
        version = something(_xstr(e, "Version"), "1.0"), enabled = something(_xbool(e, "Enabled"), false),
        include_apis = _xbool(e, "IncludeAPIs"), retention_policy = _parse_retention(xfind(e, "RetentionPolicy"))
    )
_split_csv(s) = s === nothing ? String[] : String[strip(x) for x in split(s, ',') if !isempty(strip(x))]

"""
    get_service_properties(service; timeout=nothing) -> Dict

Analytics logging, metrics, CORS, soft-delete and static website settings:
keys `"analytics_logging"`, `"hour_metrics"`, `"minute_metrics"`, `"cors"`,
`"target_version"`, `"delete_retention_policy"`, `"static_website"`.
"""
function get_service_properties(s::BlobServiceClient; timeout = nothing)
    resp = _req(
        s, "GET", "/"; query = ["restype" => "service", "comp" => "properties"], timeout = timeout,
        operation = "get_service_properties"
    )
    x = xml_parse(resp.body)
    lg = xfind(x, "Logging")
    logging = lg === nothing ? nothing :
        BlobAnalyticsLogging(
            version = something(_xstr(lg, "Version"), "1.0"),
            delete = something(_xbool(lg, "Delete"), false), read = something(_xbool(lg, "Read"), false),
            write = something(_xbool(lg, "Write"), false), retention_policy = _parse_retention(xfind(lg, "RetentionPolicy"))
        )
    cors = CorsRule[]
    for r in xfindall(xfind(x, "Cors"), "CorsRule")
        push!(
            cors, CorsRule(
                _split_csv(_xstr(r, "AllowedOrigins")), _split_csv(_xstr(r, "AllowedMethods"));
                allowed_headers = _split_csv(_xstr(r, "AllowedHeaders")), exposed_headers = _split_csv(_xstr(r, "ExposedHeaders")),
                max_age_in_seconds = something(_xint(r, "MaxAgeInSeconds"), 0)
            )
        )
    end
    sw = xfind(x, "StaticWebsite")
    static = sw === nothing ? nothing :
        StaticWebsite(
            enabled = something(_xbool(sw, "Enabled"), false), index_document = _xstr(sw, "IndexDocument"),
            error_document404_path = _xstr(sw, "ErrorDocument404Path"),
            default_index_document_path = _xstr(sw, "DefaultIndexDocumentPath")
        )
    return Dict{String, Any}(
        "analytics_logging" => logging, "hour_metrics" => _parse_metrics(xfind(x, "HourMetrics")),
        "minute_metrics" => _parse_metrics(xfind(x, "MinuteMetrics")), "cors" => cors,
        "target_version" => _xstr(x, "DefaultServiceVersion"),
        "delete_retention_policy" => _parse_retention(xfind(x, "DeleteRetentionPolicy")),
        "static_website" => static
    )
end

"""
    set_service_properties(service; analytics_logging=nothing, hour_metrics=nothing, minute_metrics=nothing,
                           cors=nothing, target_version=nothing, delete_retention_policy=nothing,
                           static_website=nothing, timeout=nothing)

Update the account's Blob service settings. Settings left as `nothing`
are not changed; `cors` replaces all CORS rules (pass `CorsRule[]` to clear).
"""
function set_service_properties(
        s::BlobServiceClient; analytics_logging = nothing, hour_metrics = nothing,
        minute_metrics = nothing, cors = nothing, target_version = nothing, delete_retention_policy = nothing,
        static_website = nothing, timeout = nothing
    )
    parts = String[]
    if analytics_logging !== nothing
        l = analytics_logging
        push!(
            parts, xml_element(
                "Logging", RawXML(
                    string(
                        xml_element("Version", l.version), xml_element("Delete", l.delete),
                        xml_element("Read", l.read), xml_element("Write", l.write), _xml_retention("RetentionPolicy", l.retention_policy)
                    )
                )
            )
        )
    end
    hour_metrics === nothing || push!(parts, _xml_metrics("HourMetrics", hour_metrics))
    minute_metrics === nothing || push!(parts, _xml_metrics("MinuteMetrics", minute_metrics))
    if cors !== nothing
        length(cors) > 5 && throw(ArgumentError("at most five CORS rules are allowed"))
        rules = join(
            xml_element(
                "CorsRule", RawXML(
                    string(
                        xml_element("AllowedOrigins", join(r.allowed_origins, ",")),
                        xml_element("AllowedMethods", join(r.allowed_methods, ",")),
                        xml_element("MaxAgeInSeconds", r.max_age_in_seconds),
                        xml_element("ExposedHeaders", join(r.exposed_headers, ",")),
                        xml_element("AllowedHeaders", join(r.allowed_headers, ","))
                    )
                )
            ) for r in cors
        )
        push!(parts, xml_element("Cors", RawXML(rules)))
    end
    target_version === nothing || push!(parts, xml_element("DefaultServiceVersion", String(target_version)))
    delete_retention_policy === nothing ||
        push!(parts, _xml_retention("DeleteRetentionPolicy", delete_retention_policy))
    if static_website !== nothing
        w = static_website
        push!(
            parts, xml_element(
                "StaticWebsite", RawXML(
                    string(
                        xml_element("Enabled", w.enabled),
                        w.enabled ? string(
                                xml_element("IndexDocument", w.index_document),
                                xml_element("ErrorDocument404Path", w.error_document404_path),
                                xml_element("DefaultIndexDocumentPath", w.default_index_document_path)
                            ) : ""
                    )
                )
            )
        )
    end
    body = xml_document("StorageServiceProperties", RawXML(join(parts)))
    _req(
        s, "PUT", "/"; query = ["restype" => "service", "comp" => "properties"],
        headers = ["Content-Type" => "application/xml"], body = body, expected = (202,), timeout = timeout,
        operation = "set_service_properties"
    )
    return nothing
end

"""
    get_service_stats(service; timeout=nothing) -> Dict

Geo-replication statistics (read-access geo-redundant accounts only; the
request goes to the account's secondary endpoint). Returns
`Dict("geo_replication" => Dict("status" => ..., "last_sync_time" => ...))`.
"""
function get_service_stats(s::BlobServiceClient; timeout = nothing)
    cfg = s.config
    sec = if cfg.path_style
        cfg
    else
        m = match(r"^([^.]+)\.(.*)$", cfg.host)
        m === nothing && throw(ArgumentError("cannot derive the secondary endpoint from $(cfg.host)"))
        host = string(m.captures[1], "-secondary.", m.captures[2])
        fields = [getfield(cfg, f) for f in fieldnames(StorageConfig)]
        fields[findfirst(==(:host), fieldnames(StorageConfig))] = host
        fields[findfirst(==(:primary_endpoint), fieldnames(StorageConfig))] =
            string(cfg.scheme, "://", host, isempty(cfg.port) ? "" : ":" * cfg.port)
        StorageConfig(fields...)
    end
    resp = storage_request(
        sec, "GET", "/"; query = ["restype" => "service", "comp" => "stats"], timeout = timeout,
        operation = "get_service_stats"
    )
    g = xfind(xml_parse(resp.body), "GeoReplication")
    return Dict{String, Any}(
        "geo_replication" => Dict{String, Any}(
            "status" => _xstr(g, "Status"),
            "last_sync_time" => _xdate(g, "LastSyncTime")
        )
    )
end

"""
    get_user_delegation_key(service, key_start_time, key_expiry_time; timeout=nothing) -> UserDelegationKey

Obtain a key for signing user delegation SAS tokens. Requires a
`TokenCredential`. Times are `DateTime`s (UTC) or `Dates.Period`s from now.
"""
function get_user_delegation_key(
        s::BlobServiceClient, key_start_time, key_expiry_time; timeout = nothing,
        delegated_user_tid = nothing
    )
    s.config.credential isa TokenCredential ||
        throw(ArgumentError("get_user_delegation_key requires a TokenCredential (Entra ID authentication)"))
    body = xml_document(
        "KeyInfo", RawXML(
            string(
                xml_element("Start", iso8601(key_start_time)),
                xml_element("Expiry", iso8601(key_expiry_time)),
                delegated_user_tid === nothing ? "" : xml_element("DelegatedUserTid", String(delegated_user_tid))
            )
        )
    )
    resp = _req(
        s, "POST", "/"; query = ["restype" => "service", "comp" => "userdelegationkey"],
        headers = ["Content-Type" => "application/xml"], body = body, expected = (200,), timeout = timeout,
        operation = "get_user_delegation_key"
    )
    x = xml_parse(resp.body)
    return UserDelegationKey(
        signed_oid = something(_xstr(x, "SignedOid"), ""), signed_tid = something(_xstr(x, "SignedTid"), ""),
        signed_start = something(_xstr(x, "SignedStart"), ""), signed_expiry = something(_xstr(x, "SignedExpiry"), ""),
        signed_service = something(_xstr(x, "SignedService"), ""), signed_version = something(_xstr(x, "SignedVersion"), ""),
        signed_delegated_user_tid = _xstr(x, "SignedDelegatedUserTid"), value = something(_xstr(x, "Value"), "")
    )
end

function _filtered_blobs(c::AnyBlobClient, path, extra_query, filter_expression, results_per_page, timeout)
    fetch = function (marker)
        q = Pair{String, String}[extra_query..., "comp" => "blobs", "where" => String(filter_expression)]
        marker === nothing || push!(q, "marker" => marker)
        results_per_page === nothing || push!(q, "maxresults" => string(results_per_page))
        resp = _req(c, "GET", path; query = q, timeout = timeout, operation = "find_blobs_by_tags")
        x = xml_parse(resp.body)
        items = [
            FilteredBlob(
                _xname(b), something(_xstr(b, "ContainerName"), c isa ContainerClient ? c.container_name : ""),
                something(_xtags(b), Dict{String, String}())
            ) for b in xfindall(xfind(x, "Blobs"), "Blob")
        ]
        return items, _xstr(x, "NextMarker")
    end
    return ItemPaged{FilteredBlob}(fetch)
end

"""
    find_blobs_by_tags(client, filter_expression; results_per_page=nothing, timeout=nothing) -> ItemPaged{FilteredBlob}

Find blobs whose tags match a SQL-like expression such as
`"\\"project\\"='alpha' AND \\"status\\"='done'"`, across the account
(`BlobServiceClient`) or within one container (`ContainerClient`).
"""
find_blobs_by_tags(s::BlobServiceClient, filter_expression::AbstractString; results_per_page = nothing, timeout = nothing) =
    _filtered_blobs(s, "/", Pair{String, String}[], filter_expression, results_per_page, timeout)
