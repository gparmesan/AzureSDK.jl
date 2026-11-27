# Shared access signatures (Python: azure.storage.blob._shared_access_signature).
#
# Tokens are signed for service version SAS_VERSION; the string-to-sign
# layouts below are the ones defined for versions 2020-12-06 and later.

const SAS_VERSION = DEFAULT_API_VERSION

_sas_sign(key_b64::AbstractString, s::AbstractString) = base64encode(SHA.hmac_sha256(base64decode(key_b64), s))
_sas_str(x) = x === nothing ? "" : string(x)
_sas_time(t) = t === nothing ? nothing : iso8601(t)

function _sas_token(fields)
    return join((string(k, "=", quote_url(string(v); safe = "/")) for (k, v) in fields if v !== nothing && v != ""), "&")
end

function _check_protocol(protocol)
    protocol === nothing && return nothing
    p = String(protocol)
    p in ("https", "https,http") || throw(ArgumentError("protocol must be \"https\" or \"https,http\""))
    return p
end

"""
    generate_account_sas(account_name, account_key, resource_types, permission, expiry;
                         start=nothing, ip=nothing, protocol=nothing, services=Services(blob=true),
                         encryption_scope=nothing) -> String

Create an account SAS token (without leading `?`). `resource_types` is a
[`ResourceTypes`](@ref) or string like `"sco"`, `permission` an
[`AccountSasPermissions`](@ref) or string like `"rl"`; `expiry`/`start` are
`DateTime`s (UTC), `Dates.Period`s from now, or ISO 8601 strings.
"""
function generate_account_sas(
        account_name::AbstractString, account_key::AbstractString, resource_types, permission,
        expiry; start = nothing, ip = nothing, protocol = nothing, services = Services(blob = true), encryption_scope = nothing
    )
    sp = string(permission isa AbstractString ? AccountSasPermissions(permission) : permission)
    srt = string(resource_types isa AbstractString ? ResourceTypes(resource_types) : resource_types)
    ss = services isa AbstractString ? String(services) : string(services)
    st, se = _sas_time(start), _sas_time(expiry)
    se === nothing && throw(ArgumentError("expiry is required"))
    spr = _check_protocol(protocol)
    to_sign = join(
        [
            account_name, sp, ss, srt, _sas_str(st), se, _sas_str(ip), _sas_str(spr), SAS_VERSION,
            _sas_str(encryption_scope),
        ], "\n"
    ) * "\n"
    sig = _sas_sign(account_key, to_sign)
    return _sas_token(
        [
            "st" => st, "se" => se, "sp" => sp, "sv" => SAS_VERSION, "ss" => ss, "srt" => srt,
            "sip" => ip, "spr" => spr, "ses" => encryption_scope, "sig" => sig,
        ]
    )
end

function _service_sas(
        account_name, path, resource; account_key = nothing, user_delegation_key = nothing, permission = nothing,
        expiry = nothing, start = nothing, policy_id = nothing, ip = nothing, protocol = nothing, timestamp = nothing,
        cache_control = nothing, content_disposition = nothing, content_encoding = nothing, content_language = nothing,
        content_type = nothing, encryption_scope = nothing, user_delegation_oid = nothing, correlation_id = nothing, perm_type
    )
    (account_key === nothing) == (user_delegation_key === nothing) &&
        throw(ArgumentError("exactly one of account_key and user_delegation_key must be given"))
    sp = permission === nothing ? nothing :
        permission isa AbstractString ? string(perm_type(permission)) : string(permission)
    if policy_id === nothing
        (sp === nothing || expiry === nothing) &&
            throw(ArgumentError("permission and expiry are required unless a stored access policy (policy_id) is used"))
    end
    user_delegation_key !== nothing && policy_id !== nothing &&
        throw(ArgumentError("stored access policies cannot be used with user delegation SAS"))
    st, se = _sas_time(start), _sas_time(expiry)
    spr = _check_protocol(protocol)
    canonical = string("/blob/", account_name, path)
    parts = String[_sas_str(sp), _sas_str(st), _sas_str(se), canonical]
    k = user_delegation_key
    if k !== nothing
        append!(
            parts, [
                k.signed_oid, k.signed_tid, k.signed_start, k.signed_expiry, k.signed_service, k.signed_version,
                _sas_str(user_delegation_oid), "", _sas_str(correlation_id),
            ]
        )
    else
        push!(parts, _sas_str(policy_id))
    end
    append!(
        parts, [
            _sas_str(ip), _sas_str(spr), SAS_VERSION, resource, _sas_str(timestamp), _sas_str(encryption_scope),
            _sas_str(cache_control), _sas_str(content_disposition), _sas_str(content_encoding), _sas_str(content_language),
            _sas_str(content_type),
        ]
    )
    sig = _sas_sign(k === nothing ? account_key : k.value, join(parts, "\n"))
    fields = Pair{String, Any}[
        "st" => st, "se" => se, "sp" => sp, "sv" => SAS_VERSION, "sip" => ip, "spr" => spr,
        "ses" => encryption_scope, "sr" => resource, "snapshot" => timestamp, "si" => policy_id,
    ]
    if k !== nothing
        append!(
            fields, [
                "skoid" => k.signed_oid, "sktid" => k.signed_tid, "skt" => k.signed_start,
                "ske" => k.signed_expiry, "sks" => k.signed_service, "skv" => k.signed_version,
                "saoid" => user_delegation_oid, "scid" => correlation_id,
            ]
        )
    end
    append!(
        fields, [
            "rscc" => cache_control, "rscd" => content_disposition, "rsce" => content_encoding,
            "rscl" => content_language, "rsct" => content_type, "sig" => sig,
        ]
    )
    return _sas_token(fields)
end

"""
    generate_container_sas(account_name, container_name; account_key=nothing, user_delegation_key=nothing,
                           permission=nothing, expiry=nothing, start=nothing, policy_id=nothing, ip=nothing,
                           protocol=nothing, cache_control=nothing, content_disposition=nothing,
                           content_encoding=nothing, content_language=nothing, content_type=nothing,
                           encryption_scope=nothing, user_delegation_oid=nothing, correlation_id=nothing) -> String

Create a service SAS for a container, signed with the account key or a
[`UserDelegationKey`](@ref) (from [`get_user_delegation_key`](@ref)).
`permission` is a [`ContainerSasPermissions`](@ref) or string like `"rl"`.
"""
generate_container_sas(account_name::AbstractString, container_name::AbstractString; kwargs...) =
    _service_sas(account_name, "/" * container_name, "c"; perm_type = ContainerSasPermissions, kwargs...)

"""
    generate_blob_sas(account_name, container_name, blob_name; snapshot=nothing, version_id=nothing,
                      account_key=nothing, user_delegation_key=nothing, permission=nothing, expiry=nothing, ...) -> String

Create a service SAS for a blob (or one of its snapshots / versions). Other
keywords as for [`generate_container_sas`](@ref); `permission` is a
[`BlobSasPermissions`](@ref) or string like `"r"`.
"""
function generate_blob_sas(
        account_name::AbstractString, container_name::AbstractString, blob_name::AbstractString;
        snapshot = nothing, version_id = nothing, kwargs...
    )
    resource = version_id !== nothing ? "bv" : snapshot !== nothing ? "bs" : "b"
    ts = something(_snapshot_id(snapshot), version_id, Some(nothing))
    return _service_sas(
        account_name, string("/", container_name, "/", blob_name), resource;
        perm_type = BlobSasPermissions, timestamp = ts, kwargs...
    )
end
