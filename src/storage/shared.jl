# Shared Storage infrastructure (Python: azure.storage.*._shared): account
# URLs and connection strings, credentials, Shared Key request signing, the
# HTTP pipeline with retries, and error mapping.

const DEFAULT_API_VERSION = "2024-08-04"
const STORAGE_OAUTH_SCOPE = "https://storage.azure.com/.default"
const DEV_ACCOUNT_NAME = "devstoreaccount1"
# Azurite's publicly documented development key (not a secret).
const DEV_ACCOUNT_KEY = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
const DEV_PORTS = Dict("blob" => 10000, "queue" => 10001, "table" => 10002, "dfs" => 10000)
const RETRY_STATUS = (408, 429, 500, 502, 503, 504)

const _RFC1123_FMT = Dates.dateformat"e, dd u yyyy HH:MM:SS"

# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------

rfc1123(dt::DateTime) = Dates.format(dt, _RFC1123_FMT) * " GMT"

"Parse an RFC 1123 or ISO 8601 timestamp (UTC) into a `DateTime`; `nothing` if absent/invalid."
function parse_datetime(s)
    s isa AbstractString || return nothing
    t = strip(s)
    isempty(t) && return nothing
    if occursin(',', t)
        return tryparse(DateTime, replace(t, r"\s*GMT$" => ""), Dates.dateformat"e, d u y H:M:S")
    end
    m = match(r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(?::\d{2})?)(?:\.(\d+))?Z?$", t)
    m === nothing && return nothing
    base = m.captures[1]
    count(==(':'), base) == 1 && (base *= ":00")
    dt = tryparse(DateTime, base, Dates.dateformat"yyyy-mm-ddTHH:MM:SS")
    dt === nothing && return nothing
    frac = m.captures[2]
    if frac !== nothing
        ms = parse(Int, rpad(frac[1:min(3, length(frac))], 3, '0'))
        dt += Dates.Millisecond(ms)
    end
    return dt
end

"""
Normalise a time argument to a UTC `DateTime`: a `DateTime` (taken as UTC),
a `Dates.Period` (relative to now) or a string (ISO 8601 / RFC 1123).
"""
function to_utc(t)
    t isa DateTime && return t
    t isa Dates.Date && return DateTime(t)
    t isa Dates.Period && return Dates.now(Dates.UTC) + t
    if t isa AbstractString
        dt = parse_datetime(t)
        dt === nothing && throw(ArgumentError("invalid date/time $(repr(t))"))
        return dt
    end
    throw(ArgumentError("expected a DateTime, a Dates.Period or a date string, got $(typeof(t))"))
end

"`yyyy-mm-ddTHH:MM:SSZ` (the format used by SAS tokens and key-info requests)."
iso8601(t) = t isa AbstractString ? String(t) : Dates.format(to_utc(t), Dates.dateformat"yyyy-mm-ddTHH:MM:SS") * "Z"

const _UNRESERVED = Set(UInt8.(collect("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~")))

"Percent-encode `s`, leaving unreserved characters and those in `safe` untouched (Python's `urllib.parse.quote`)."
function quote_url(s::AbstractString; safe::AbstractString = "/")
    safeb = Set(codeunits(String(safe)))
    io = IOBuffer()
    for b in codeunits(String(s))
        if b in _UNRESERVED || b in safeb
            write(io, b)
        else
            write(io, '%', uppercase(string(b; base = 16, pad = 2)))
        end
    end
    return String(take!(io))
end

"Decode percent-escapes (no `+` handling)."
unquote_url(s::AbstractString) = URIs.unescapeuri(String(s))

encode_query(q) = join((string(quote_url(k; safe = ""), "=", quote_url(v; safe = "")) for (k, v) in q), "&")

"Parse a query string into ordered pairs (values unescaped)."
function query_pairs(q::AbstractString)
    out = Pair{String, String}[]
    for part in split(q, '&'; keepempty = false)
        i = findfirst('=', part)
        k, v = i === nothing ? (part, "") : (part[1:(i - 1)], part[(i + 1):end])
        push!(out, unquote_url(k) => unquote_url(v))
    end
    return out
end

"Whether a string looks like a shared access signature (has `sig=` plus `se=`/`sv=`...)."
function is_sas_token(s::AbstractString)
    keys_ = Set(lowercase(first(p)) for p in query_pairs(lstrip(s, '?')))
    return "sig" in keys_ && ("se" in keys_ || "sv" in keys_ || "si" in keys_)
end

const libcrypto = OpenSSL_jll.libcrypto

"MD5 digest (for transactional content validation only; not a security primitive)."
function md5(data::AbstractVector{UInt8})
    out = Vector{UInt8}(undef, 16)
    md = ccall((:EVP_md5, libcrypto), Ptr{Cvoid}, ())
    buf = data isa Vector{UInt8} ? data : Vector{UInt8}(data)
    ok = GC.@preserve buf ccall(
        (:EVP_Digest, libcrypto), Cint,
        (Ptr{UInt8}, Csize_t, Ptr{UInt8}, Ptr{Cuint}, Ptr{Cvoid}, Ptr{Cvoid}), buf, length(buf), out, C_NULL, md, C_NULL
    )
    ok == 1 || error("MD5 computation failed")
    return out
end

# ---------------------------------------------------------------------------
# Account URLs, connection strings and credentials
# ---------------------------------------------------------------------------

"""
Parsed storage URL. `path_style` is true for emulator/IP style URLs
(`http://127.0.0.1:10000/devstoreaccount1/...`) where the account name is
the first path segment.
"""
struct StorageURL
    scheme::String
    host::String
    port::String
    path_style::Bool
    account_name::Union{Nothing, String}
    segments::Vector{String}           # decoded path segments after the account (path style) / host
    query::String
end

function _is_ip(host::AbstractString)
    h = strip(host, ['[', ']'])
    return occursin(r"^\d{1,3}(\.\d{1,3}){3}$", h) || occursin(':', h)
end

function parse_storage_url(url::AbstractString, service::AbstractString)
    s = strip(url)
    occursin("://", s) || (s = "https://" * s)
    u = URIs.URI(s)
    scheme = lowercase(u.scheme)
    scheme in ("https", "http") || throw(ArgumentError("invalid account URL: scheme must be https"))
    isempty(u.host) && throw(ArgumentError("invalid account URL $(repr(AzureCore.redact_url(s)))"))
    if scheme == "http" && !AzureCore.is_dev_host(u.host)
        throw(ArgumentError("account URL must use https (http is only allowed for local emulators)"))
    end
    host = lowercase(u.host)
    segs = [unquote_url(x) for x in split(u.path, '/'; keepempty = false)]
    path_style = _is_ip(host) || AzureCore.is_dev_host(host) && !occursin(".$(service).", host)
    account = nothing
    if path_style
        isempty(segs) || (account = popfirst!(segs))
    else
        m = match(Regex("^([^.]+)\\.$(service)(?:-secondary)?\\.core\\."), host)
        account = m === nothing ? String(first(split(host, '.'))) : String(m.captures[1])
    end
    return StorageURL(scheme, host, u.port, path_style, account, segs, String(u.query))
end

function endpoint_of(u::StorageURL)
    base = string(u.scheme, "://", occursin(':', u.host) ? "[$(u.host)]" : u.host, isempty(u.port) ? "" : ":" * u.port)
    return u.path_style && u.account_name !== nothing ? string(base, "/", quote_url(u.account_name)) : base
end

"""
    parse_storage_connection_string(conn_str, service="blob") -> (endpoint, credential)

Python: `azure.storage.blob._shared.base_client.parse_connection_str`.
Supports `UseDevelopmentStorage=true`, `AccountName`/`AccountKey`,
`SharedAccessSignature`, `DefaultEndpointsProtocol`, `EndpointSuffix` and
`<Service>Endpoint`.
"""
function parse_storage_connection_string(conn_str::AbstractString, service::AbstractString = "blob")
    d = parse_connection_string_pairs(conn_str)
    if lowercase(get(d, "usedevelopmentstorage", "false")) == "true"
        proxy = get(d, "developmentstorageproxyuri", "http://127.0.0.1")
        endpoint = "$(rstrip(proxy, '/')):$(DEV_PORTS[service])/$(DEV_ACCOUNT_NAME)"
        return endpoint, AzureNamedKeyCredential(DEV_ACCOUNT_NAME, DEV_ACCOUNT_KEY)
    end
    account = get(d, "accountname", nothing)
    key = get(d, "accountkey", nothing)
    sas = get(d, "sharedaccesssignature", nothing)
    endpoint = get(d, lowercase(service) * "endpoint", nothing)
    if endpoint === nothing
        account === nothing && throw(ArgumentError("Connection string missing required connection details."))
        proto = get(d, "defaultendpointsprotocol", "https")
        suffix = get(d, "endpointsuffix", "core.windows.net")
        endpoint = "$(proto)://$(account).$(service).$(suffix)"
    end
    cred = if key !== nothing
        account === nothing && throw(ArgumentError("Connection string missing AccountName for AccountKey."))
        AzureNamedKeyCredential(account, key)
    elseif sas !== nothing
        AzureSasCredential(lstrip(sas, '?'))
    else
        nothing
    end
    return String(rstrip(endpoint, '/')), cred
end

"""
Normalise the `credential` argument (Python semantics): `nothing`, a SAS
token string, an account key string, an `AzureNamedKeyCredential`, a
dict/named tuple with `account_name` and `account_key`, an
`AzureSasCredential` or a `TokenCredential`.
"""
function normalize_credential(credential, account_name)
    credential === nothing && return nothing
    if credential isa AbstractString
        is_sas_token(credential) && return AzureSasCredential(lstrip(credential, '?'))
        account_name === nothing &&
            throw(ArgumentError("an account key credential requires the account name (use AzureNamedKeyCredential)"))
        return AzureNamedKeyCredential(account_name, credential)
    end
    if credential isa AbstractDict || credential isa NamedTuple
        g(k) = credential isa AbstractDict ? get(credential, k, get(credential, Symbol(k), nothing)) : get(credential, Symbol(k), nothing)
        n, k = g("account_name"), g("account_key")
        (n === nothing || k === nothing) && throw(ArgumentError("shared key credential needs account_name and account_key"))
        return AzureNamedKeyCredential(string(n), string(k))
    end
    credential isa Union{AzureNamedKeyCredential, AzureSasCredential, TokenCredential} && return credential
    throw(ArgumentError("unsupported credential type $(typeof(credential))"))
end

# ---------------------------------------------------------------------------
# Shared Key signing
# ---------------------------------------------------------------------------

function _hget(headers, name)
    ln = lowercase(name)
    for (k, v) in headers
        lowercase(k) == ln && return String(v)
    end
    return ""
end

"""
    shared_key_signature(account_name, account_key, method, url, headers) -> "SharedKey account:signature"

Authorization header value for the Storage Shared Key scheme (API version
2009-09-19 and later). `url` must be exactly the URL sent.
"""
function shared_key_signature(
        account_name::AbstractString, account_key::AbstractString, method::AbstractString,
        url::AbstractString, headers
    )
    clen = _hget(headers, "Content-Length")
    clen == "0" && (clen = "")
    canon = sort!(
        [(lowercase(String(k)), strip(String(v))) for (k, v) in headers if startswith(lowercase(k), "x-ms-")];
        by = first
    )
    u = URIs.URI(url)
    io = IOBuffer()
    for part in (
            uppercase(method), _hget(headers, "Content-Encoding"), _hget(headers, "Content-Language"), clen,
            _hget(headers, "Content-MD5"), _hget(headers, "Content-Type"), "", _hget(headers, "If-Modified-Since"),
            _hget(headers, "If-Match"), _hget(headers, "If-None-Match"), _hget(headers, "If-Unmodified-Since"),
            _hget(headers, "Range"),
        )
        print(io, part, '\n')
    end
    for (k, v) in canon
        print(io, k, ':', v, '\n')
    end
    print(io, '/', account_name, isempty(u.path) ? "/" : u.path)
    params = Dict{String, Vector{String}}()
    for (k, v) in query_pairs(u.query)
        push!(get!(params, lowercase(k), String[]), v)
    end
    for k in sort!(collect(keys(params)))
        print(io, '\n', k, ':', join(sort!(params[k]), ","))
    end
    sig = base64encode(SHA.hmac_sha256(base64decode(account_key), take!(io)))
    return "SharedKey $(account_name):$(sig)"
end

# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------

"""
    StorageErrorCode

Well-known error codes returned by the Storage services (Python:
`azure.storage.blob.StorageErrorCode`). Compare with an error's
`error_code` field, e.g. `err.error_code == StorageErrorCode.blob_not_found`.
"""
module StorageErrorCode
    for (name, code) in (
            # general
            :account_already_exists => "AccountAlreadyExists", :account_being_created => "AccountBeingCreated",
            :account_is_disabled => "AccountIsDisabled", :authentication_failed => "AuthenticationFailed",
            :authorization_failure => "AuthorizationFailure", :condition_headers_not_supported => "ConditionHeadersNotSupported",
            :condition_not_met => "ConditionNotMet", :empty_metadata_key => "EmptyMetadataKey",
            :insufficient_account_permissions => "InsufficientAccountPermissions", :internal_error => "InternalError",
            :invalid_authentication_info => "InvalidAuthenticationInfo", :invalid_header_value => "InvalidHeaderValue",
            :invalid_http_verb => "InvalidHttpVerb", :invalid_input => "InvalidInput", :invalid_md5 => "InvalidMd5",
            :invalid_metadata => "InvalidMetadata", :invalid_query_parameter_value => "InvalidQueryParameterValue",
            :invalid_range => "InvalidRange", :invalid_resource_name => "InvalidResourceName", :invalid_uri => "InvalidUri",
            :invalid_xml_document => "InvalidXmlDocument", :invalid_xml_node_value => "InvalidXmlNodeValue",
            :md5_mismatch => "Md5Mismatch", :metadata_too_large => "MetadataTooLarge",
            :missing_content_length_header => "MissingContentLengthHeader",
            :missing_required_query_parameter => "MissingRequiredQueryParameter",
            :missing_required_header => "MissingRequiredHeader", :missing_required_xml_node => "MissingRequiredXmlNode",
            :multiple_condition_headers_not_supported => "MultipleConditionHeadersNotSupported",
            :operation_timed_out => "OperationTimedOut", :out_of_range_input => "OutOfRangeInput",
            :out_of_range_query_parameter_value => "OutOfRangeQueryParameterValue", :request_body_too_large => "RequestBodyTooLarge",
            :resource_type_mismatch => "ResourceTypeMismatch", :request_url_failed_to_parse => "RequestUrlFailedToParse",
            :resource_already_exists => "ResourceAlreadyExists", :resource_not_found => "ResourceNotFound",
            :server_busy => "ServerBusy", :unsupported_header => "UnsupportedHeader",
            :unsupported_xml_node => "UnsupportedXmlNode", :unsupported_query_parameter => "UnsupportedQueryParameter",
            :unsupported_http_verb => "UnsupportedHttpVerb",
            # blob
            :append_position_condition_not_met => "AppendPositionConditionNotMet", :blob_already_exists => "BlobAlreadyExists",
            :blob_immutable_due_to_policy => "BlobImmutableDueToPolicy", :blob_not_found => "BlobNotFound",
            :blob_overwritten => "BlobOverwritten", :blob_tier_inadequate_for_content_length => "BlobTierInadequateForContentLength",
            :blob_uses_customer_specified_encryption => "BlobUsesCustomerSpecifiedEncryption",
            :block_count_exceeds_limit => "BlockCountExceedsLimit", :block_list_too_long => "BlockListTooLong",
            :cannot_change_to_lower_tier => "CannotChangeToLowerTier",
            :cannot_verify_copy_source => "CannotVerifyCopySource", :container_already_exists => "ContainerAlreadyExists",
            :container_being_deleted => "ContainerBeingDeleted", :container_disabled => "ContainerDisabled",
            :container_not_found => "ContainerNotFound", :content_length_larger_than_tier_limit => "ContentLengthLargerThanTierLimit",
            :copy_across_accounts_not_supported => "CopyAcrossAccountsNotSupported", :copy_id_mismatch => "CopyIdMismatch",
            :feature_version_mismatch => "FeatureVersionMismatch", :incremental_copy_blob_mismatch => "IncrementalCopyBlobMismatch",
            :incremental_copy_of_earlier_version_snapshot_not_allowed => "IncrementalCopyOfEarlierVersionSnapshotNotAllowed",
            :incremental_copy_source_must_be_snapshot => "IncrementalCopySourceMustBeSnapshot",
            :infinite_lease_duration_required => "InfiniteLeaseDurationRequired", :invalid_blob_or_block => "InvalidBlobOrBlock",
            :invalid_blob_tier => "InvalidBlobTier", :invalid_blob_type => "InvalidBlobType", :invalid_block_id => "InvalidBlockId",
            :invalid_block_list => "InvalidBlockList", :invalid_operation => "InvalidOperation", :invalid_page_range => "InvalidPageRange",
            :invalid_source_blob_type => "InvalidSourceBlobType", :invalid_source_blob_url => "InvalidSourceBlobUrl",
            :invalid_version_for_page_blob_operation => "InvalidVersionForPageBlobOperation",
            :lease_already_present => "LeaseAlreadyPresent", :lease_already_broken => "LeaseAlreadyBroken",
            :lease_id_mismatch_with_blob_operation => "LeaseIdMismatchWithBlobOperation",
            :lease_id_mismatch_with_container_operation => "LeaseIdMismatchWithContainerOperation",
            :lease_id_mismatch_with_lease_operation => "LeaseIdMismatchWithLeaseOperation", :lease_id_missing => "LeaseIdMissing",
            :lease_is_breaking_and_cannot_be_acquired => "LeaseIsBreakingAndCannotBeAcquired",
            :lease_is_breaking_and_cannot_be_changed => "LeaseIsBreakingAndCannotBeChanged",
            :lease_is_broken_and_cannot_be_renewed => "LeaseIsBrokenAndCannotBeRenewed", :lease_lost => "LeaseLost",
            :lease_not_present_with_blob_operation => "LeaseNotPresentWithBlobOperation",
            :lease_not_present_with_container_operation => "LeaseNotPresentWithContainerOperation",
            :lease_not_present_with_lease_operation => "LeaseNotPresentWithLeaseOperation",
            :max_blob_size_condition_not_met => "MaxBlobSizeConditionNotMet", :no_pending_copy_operation => "NoPendingCopyOperation",
            :operation_not_allowed_on_incremental_copy_blob => "OperationNotAllowedOnIncrementalCopyBlob",
            :pending_copy_operation => "PendingCopyOperation", :previous_snapshot_cannot_be_newer => "PreviousSnapshotCannotBeNewer",
            :previous_snapshot_not_found => "PreviousSnapshotNotFound",
            :previous_snapshot_operation_not_supported => "PreviousSnapshotOperationNotSupported",
            :sequence_number_condition_not_met => "SequenceNumberConditionNotMet",
            :sequence_number_increment_too_large => "SequenceNumberIncrementTooLarge", :snapshot_count_exceeded => "SnapshotCountExceeded",
            :snapshots_present => "SnapshotsPresent", :source_condition_not_met => "SourceConditionNotMet",
            :system_in_use => "SystemInUse", :target_condition_not_met => "TargetConditionNotMet",
            :unauthorized_blob_overwrite => "UnauthorizedBlobOverwrite", :blob_being_rehydrated => "BlobBeingRehydrated",
            :blob_archived => "BlobArchived", :blob_not_archived => "BlobNotArchived",
            :authorization_source_ip_mismatch => "AuthorizationSourceIPMismatch",
            :authorization_protocol_mismatch => "AuthorizationProtocolMismatch",
            :authorization_permission_mismatch => "AuthorizationPermissionMismatch",
            :authorization_service_mismatch => "AuthorizationServiceMismatch",
            :authorization_resource_type_mismatch => "AuthorizationResourceTypeMismatch",
            :no_authentication_information => "NoAuthenticationInformation",
            :blob_access_tier_not_supported_for_account_type => "BlobAccessTierNotSupportedForAccountType",
            :blob_tags_not_supported => "BlobTagsNotSupported",
        )
        @eval const $name = $code
    end
end

AzureCore.@azure_error ContentValidationError
@doc """
    ContentValidationError(message)

Transferred data failed an integrity check (`validate_content=true`
MD5 mismatch). Python raises a plain `AzureError` here.
""" ContentValidationError

const _EXISTS_CODES = Set(
    [
        "AccountAlreadyExists", "AccountBeingCreated", "ResourceAlreadyExists",
        "ResourceTypeMismatch", "BlobAlreadyExists", "QueueAlreadyExists", "ContainerAlreadyExists",
        "ContainerBeingDeleted", "QueueBeingDeleted", "ShareAlreadyExists", "ShareBeingDeleted",
    ]
)
const _NOT_FOUND_CODES = Set(
    [
        "ResourceNotFound", "BlobNotFound", "QueueNotFound", "ContainerNotFound",
        "ParentNotFound", "ShareNotFound",
    ]
)
const _MODIFIED_CODES = Set(
    [
        "ConditionNotMet", "SourceConditionNotMet", "TargetConditionNotMet",
        "AppendPositionConditionNotMet", "MaxBlobSizeConditionNotMet", "SequenceNumberConditionNotMet",
    ]
)
const _AUTH_CODES = Set(
    [
        "InvalidAuthenticationInfo", "AuthenticationFailed", "NoAuthenticationInformation",
        "AuthorizationFailure", "AuthorizationSourceIPMismatch", "AuthorizationProtocolMismatch",
        "AuthorizationPermissionMismatch", "AuthorizationServiceMismatch", "AuthorizationResourceTypeMismatch",
    ]
)

"""
    storage_error(resp; operation="") -> AbstractHttpResponseError

Build the exception for an unsuccessful Storage response, mapping the
service error code the way the Python SDK does.
"""
function storage_error(resp; operation::AbstractString = "")
    body = resp.body isa AbstractVector{UInt8} ? String(copy(resp.body)) : ""
    code = HTTP.header(resp, "x-ms-error-code", "")
    msg = ""
    if !isempty(body) && occursin('<', body)
        try
            x = xml_parse(body)
            c = xchildtext(x, "Code")
            (c !== nothing && isempty(code)) && (code = c)
            msg = something(xchildtext(x, "Message"), "")
            detail = xchildtext(x, "AuthenticationErrorDetail")
            detail === nothing || (msg *= "\n" * detail)
        catch
        end
    end
    isempty(msg) && (msg = "Operation returned an invalid status $(resp.status) '$(AzureCore.status_text(resp))'")
    msg = first(split(msg, "\nRequestId:"))
    isempty(operation) || (msg = "$(operation): $(msg)")
    isempty(code) || (msg *= "\nErrorCode:" * code)
    length(body) > 4096 && (body = body[1:prevind(body, 4096)] * "…")
    # Service error codes decide (as in the Python SDK); the status is only a fallback.
    T = if code in _MODIFIED_CODES
        ResourceModifiedError
    elseif code in _AUTH_CODES
        ClientAuthenticationError
    elseif code in _NOT_FOUND_CODES
        ResourceNotFoundError
    elseif code in _EXISTS_CODES
        ResourceExistsError
    elseif !isempty(code)
        HttpResponseError
    elseif resp.status == 304
        ResourceNotModifiedError
    elseif resp.status in (401, 403)
        ClientAuthenticationError
    elseif resp.status == 404
        ResourceNotFoundError
    elseif resp.status == 409
        ResourceExistsError
    elseif resp.status == 412
        ResourceModifiedError
    else
        HttpResponseError
    end
    return T(
        msg; status_code = resp.status, reason = AzureCore.status_text(resp), response_body = body,
        error_code = isempty(code) ? nothing : code
    )
end

# ---------------------------------------------------------------------------
# Client configuration and the request pipeline
# ---------------------------------------------------------------------------

"""
Connection, authentication, retry and transfer settings shared by all
clients derived from one service client.
"""
struct StorageConfig
    service::String                  # "blob"
    scheme::String
    host::String
    port::String
    path_style::Bool
    account_name::Union{Nothing, String}
    primary_endpoint::String         # scheme://host[:port][/account]
    credential::Any                  # nothing | AzureNamedKeyCredential | AzureSasCredential | TokenCredential
    sas::Union{Nothing, String}       # static SAS from the URL
    scope::String
    tenant::Base.RefValue{Union{Nothing, String}}   # learned from a bearer challenge
    api_version::String
    retry::RetryOptions
    connection_timeout::Float64
    read_timeout::Float64
    http_client::Any
    user_agent::String
    transfer::Dict{Symbol, Int}
end

Base.show(io::IO, c::StorageConfig) = print(io, "StorageConfig(", c.primary_endpoint, ")")

const TRANSFER_DEFAULTS = Dict{Symbol, Int}(
    :max_single_put_size => 64 * 1024 * 1024,
    :max_block_size => 4 * 1024 * 1024,
    :min_large_block_upload_threshold => 4 * 1024 * 1024 + 1,
    :max_page_size => 4 * 1024 * 1024,
    :max_single_get_size => 32 * 1024 * 1024,
    :max_chunk_get_size => 4 * 1024 * 1024
)

"""
    StorageConfig(url, credential, service; kwargs...) -> (config, parsed_url)

Keyword arguments (Python names): `api_version`, `audience`,
`connection_timeout` (20 s), `read_timeout` (60 s), `retry_total` (3),
`retry_backoff_factor`, `retry_backoff_max`, `retry_mode`,
`connection_verify`, `http_proxy`, `user_agent`, and the transfer sizes
`max_single_put_size`, `max_block_size`, `min_large_block_upload_threshold`,
`max_page_size`, `max_single_get_size`, `max_chunk_get_size`.
"""
function StorageConfig(
        url::AbstractString, credential, service::AbstractString; api_version = DEFAULT_API_VERSION,
        audience = nothing, connection_timeout::Real = 20.0, read_timeout::Real = 60.0, connection_verify = true, http_proxy = nothing,
        user_agent = nothing, retry_total::Integer = 3, retry_backoff_factor::Real = 0.8, retry_backoff_max::Real = 60.0,
        retry_mode = RetryMode.Exponential, kwargs...
    )
    u = parse_storage_url(url, service)
    transfer = copy(TRANSFER_DEFAULTS)
    for (k, v) in kwargs
        haskey(transfer, k) || throw(ArgumentError("unexpected keyword argument $(k)"))
        (v isa Integer && v > 0) || throw(ArgumentError("$(k) must be a positive integer"))
        transfer[k] = Int(v)
    end
    cred = normalize_credential(credential, u.account_name)
    # Keep the SAS part of the URL's query (snapshot/version ids are tracked separately).
    sas = join(
        (
            p for p in split(u.query, '&'; keepempty = false)
                if !(lowercase(first(split(p, '='))) in ("snapshot", "versionid", "sharesnapshot"))
        ), "&"
    )
    (isempty(sas) || !is_sas_token(sas)) && (sas = nothing)
    if cred isa AzureSasCredential && sas !== nothing
        throw(ArgumentError("You cannot use AzureSasCredential when the resource URI also contains a Shared Access Signature."))
    end
    (cred isa TokenCredential && u.scheme != "https" && !AzureCore.is_local_host(u.host)) &&
        throw(ArgumentError("Token credential authentication is not permitted for non-TLS protected (non-https) URLs."))
    _plaintext_credential_ok(u.scheme, u.host, cred, sas) || throw(ArgumentError(_PLAINTEXT_KEY_MSG))
    scope = audience === nothing ? STORAGE_OAUTH_SCOPE :
        endswith(String(audience), "/.default") ? String(audience) : string(rstrip(audience, '/'), "/.default")
    connection_verify === false &&
        @warn "TLS certificate verification is disabled (connection_verify=false); never do this in production"
    ua = AzureCore.user_agent("storage-$(service)")
    user_agent === nothing || (ua = string(user_agent, " ", ua))
    retry = AzureCore.retry_options(; retry_total, retry_backoff_factor, retry_backoff_max, retry_mode)
    cfg = StorageConfig(
        String(service), u.scheme, u.host, u.port, u.path_style, u.account_name, endpoint_of(u), cred,
        sas, scope, Ref{Union{Nothing, String}}(nothing), String(api_version), retry, Float64(connection_timeout),
        Float64(read_timeout), AzureCore.http_client(connection_verify, http_proxy), ua, transfer
    )
    return cfg, u
end

"Account name to use for signing (credential name wins, as in the Python SDK)."
function signing_account(cfg::StorageConfig)
    cfg.credential isa AzureNamedKeyCredential && return named_key(cfg.credential).name
    return something(cfg.account_name, "")
end

_sas_query(cfg::StorageConfig) = cfg.credential isa AzureSasCredential ? lstrip(AzureCore.signature(cfg.credential), '?') :
    cfg.sas

"Full URL for `path` (already percent-encoded, starting with `/` or empty) plus query and SAS."
function build_url(cfg::StorageConfig, path::AbstractString, query = Pair{String, String}[]; with_sas::Bool = true)
    sas = with_sas ? _sas_query(cfg) : nothing
    if sas !== nothing && occursin(r"(^|&)snapshot=", sas)
        # A snapshot SAS already names the snapshot (Python SDK token layout).
        query = [p for p in query if first(p) != "snapshot"]
    end
    q = encode_query(query)
    if sas !== nothing && !isempty(sas)
        q = isempty(q) ? String(sas) : string(q, "&", sas)
    end
    return string(cfg.primary_endpoint, path, isempty(q) ? "" : "?" * q)
end

function _parse_bearer_challenge(resp)
    h = HTTP.header(resp, "WWW-Authenticate", "")
    startswith(lowercase(h), "bearer") || return nothing
    m = match(r"authorization_uri=\"?https://[^/\s\"]+/([^/\s\"]+)", h)
    m === nothing && return nothing
    tenant = String(m.captures[1])
    occursin(r"^[A-Za-z0-9.\-]+$", tenant) || return nothing
    return tenant
end

"""
Whether `cred` / a URL SAS may be sent over `scheme` to `host`. Over plain HTTP
to anything but a loopback host (single-label emulator names, which may resolve
through the DNS search domain to any machine) only anonymous access and
Azurite's public development key are allowed, so a real account key or SAS
never travels in clear text.
"""
function _plaintext_credential_ok(scheme, host, cred, sas)
    (scheme == "https" || AzureCore.is_local_host(host)) && return true
    sas === nothing || return false
    return cred === nothing || cred isa AzureNamedKeyCredential && named_key(cred).key == DEV_ACCOUNT_KEY
end

const _PLAINTEXT_KEY_MSG = "plain HTTP to a host other than a loopback host (e.g. a docker-compose service name) \
    is only allowed anonymously or with Azurite's public development key; use https for real credentials"

function _authorize!(cfg::StorageConfig, method, url, hdrs::Vector{Pair{String, String}})
    cred = cfg.credential
    # re-checked per request: a named key credential may have been rotated with update!
    _plaintext_credential_ok(cfg.scheme, cfg.host, cred, cfg.sas) || throw(ArgumentError(_PLAINTEXT_KEY_MSG))
    if cred isa AzureNamedKeyCredential
        nk = named_key(cred)
        push!(hdrs, "Authorization" => shared_key_signature(nk.name, nk.key, method, url, hdrs))
    elseif cred isa TokenCredential
        tok = get_token(cred, cfg.scope; tenant_id = cfg.tenant[])
        push!(hdrs, "Authorization" => "Bearer " * tok.token)
    end
    return hdrs
end

_retriable_exception(err) = err isa Base.IOError || err isa EOFError || err isa HTTP.TimeoutError ||
    nameof(typeof(err)) in (
    :ConnectError, :RequestError, :DNSError, :OpError,
    :DeadlineExceededError, :ProtocolError, :ParseError, :RequestRetryError,
) &&
    !occursin(r"certificate|x509|verif"i, sprint(showerror, err))

"""
    storage_request(cfg, method, path; query=[], headers=[], body=nothing, expected=nothing,
                    timeout=nothing, operation="", raise=true, url=nothing) -> HTTP.Response

Send a signed request with retries. `expected` lists the success statuses
(default: any 2xx). Unexpected statuses raise [`storage_error`](@ref) unless
`raise=false`. `timeout` is the server-side operation timeout in seconds.
The body must be a byte vector/string (replayable) or `nothing`.
"""
function storage_request(
        cfg::StorageConfig, method::AbstractString, path::AbstractString;
        query = Pair{String, String}[], headers = Pair{String, String}[], body = nothing, expected = nothing,
        timeout = nothing, operation::AbstractString = "", raise::Bool = true
    )
    q = Pair{String, String}[String(k) => String(v) for (k, v) in query]
    timeout === nothing || push!(q, "timeout" => string(max(1, round(Int, timeout))))
    url = build_url(cfg, path, q)
    bodybytes = body === nothing ? nothing : body isa AbstractString ? codeunits(String(body)) : body
    attempt = 0
    challenged = false
    while true
        hdrs = Pair{String, String}[
            "x-ms-version" => cfg.api_version, "x-ms-date" => rfc1123(Dates.now(Dates.UTC)),
            "x-ms-client-request-id" => string(uuid4()), "User-Agent" => cfg.user_agent,
        ]
        for (k, v) in headers
            v === nothing || push!(hdrs, String(k) => String(v))
        end
        if bodybytes !== nothing || method in ("PUT", "POST", "MERGE")
            push!(hdrs, "Content-Length" => string(bodybytes === nothing ? 0 : length(bodybytes)))
        end
        _authorize!(cfg, method, url, hdrs)
        resp = try
            kw = (;
                retry = false, status_exception = false, redirect = false, cookies = false, decompress = false,
                connect_timeout = cfg.connection_timeout, read_idle_timeout = cfg.read_timeout,
            )
            b = bodybytes === nothing ? UInt8[] : bodybytes
            cfg.http_client === nothing ? HTTP.request(method, url, hdrs, b; kw...) :
                HTTP.request(method, url, hdrs, b; client = cfg.http_client, kw...)
        catch err
            err isa InterruptException && rethrow()
            attempt += 1
            if !_retriable_exception(err) || attempt > cfg.retry.retry_total
                throw(
                    ServiceRequestError(
                        "$(method) $(AzureCore.redact_url(url)) failed: " *
                            first(split(sprint(showerror, err; context = :compact => true), '\n'));
                        error = err
                    )
                )
            end
            sleep(AzureCore.backoff_delay(cfg.retry, attempt))
            continue
        end
        if resp.status == 401 && cfg.credential isa TokenCredential && !challenged
            tenant = _parse_bearer_challenge(resp)
            if tenant !== nothing && tenant != cfg.tenant[]
                challenged = true
                cfg.tenant[] = tenant
                continue
            end
        end
        if resp.status in RETRY_STATUS && attempt < cfg.retry.retry_total
            attempt += 1
            ra = AzureCore._retry_after_seconds(resp)
            sleep(ra === nothing ? AzureCore.backoff_delay(cfg.retry, attempt) : min(ra, 60.0))
            continue
        end
        ok = expected === nothing ? 200 <= resp.status < 300 : resp.status in expected
        (ok || !raise) && return resp
        throw(storage_error(resp; operation = operation))
    end
    return
end

# Response headers -> Python-style result dict.
const _RESPONSE_FIELDS = (
    "etag" => "ETag", "last_modified" => "Last-Modified", "content_md5" => "Content-MD5",
    "content_crc64" => "x-ms-content-crc64", "request_id" => "x-ms-request-id",
    "client_request_id" => "x-ms-client-request-id", "version" => "x-ms-version", "date" => "Date",
    "version_id" => "x-ms-version-id", "request_server_encrypted" => "x-ms-request-server-encrypted",
    "encryption_key_sha256" => "x-ms-encryption-key-sha256", "encryption_scope" => "x-ms-encryption-scope",
    "blob_sequence_number" => "x-ms-blob-sequence-number", "blob_committed_block_count" => "x-ms-blob-committed-block-count",
    "blob_append_offset" => "x-ms-blob-append-offset", "lease_id" => "x-ms-lease-id", "lease_time" => "x-ms-lease-time",
    "copy_id" => "x-ms-copy-id", "copy_status" => "x-ms-copy-status", "snapshot" => "x-ms-snapshot",
    "is_sealed" => "x-ms-blob-sealed", "structured_body" => "x-ms-structured-body",
)

"Python-style result dictionary built from well-known response headers."
function response_dict(resp)
    d = Dict{String, Any}()
    for (k, h) in _RESPONSE_FIELDS
        v = HTTP.header(resp, h, "")
        isempty(v) && continue
        d[k] = if k in ("last_modified", "date")
            parse_datetime(v)
        elseif k in ("content_md5",)
            try
                base64decode(v)
            catch
                v
            end
        elseif k in ("request_server_encrypted", "is_sealed")
            lowercase(v) == "true"
        elseif k in ("blob_sequence_number", "blob_committed_block_count", "blob_append_offset", "lease_time")
            something(tryparse(Int64, v), v)
        else
            v
        end
    end
    return d
end
