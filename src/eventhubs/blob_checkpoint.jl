# BlobCheckpointStore: checkpoints and partition ownership in Azure Blob
# Storage, compatible with the layout used by the Python/.NET/Java SDKs
# (azure-eventhub-checkpointstoreblob), so consumers written in different
# languages can share a consumer group.

const BLOB_API_VERSION = "2021-08-06"
const STORAGE_SCOPE = "https://storage.azure.com/.default"
const AZURITE_ACCOUNT = "devstoreaccount1"
# Azurite's publicly documented development key (not a secret).
const AZURITE_KEY = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="

"""
    BlobCheckpointStore(blob_account_url, container_name; credential=nothing)
    from_connection_string(BlobCheckpointStore, conn_str, container_name)

A [`CheckpointStore`](@ref) backed by an Azure Storage blob container (which
must exist). `credential` may be a `TokenCredential` (Entra ID; needs the
*Storage Blob Data Contributor* role), an `AzureNamedKeyCredential`
(account name + key), an `AzureSasCredential` (container SAS), or `nothing`
when `blob_account_url` already carries a SAS query string.

Blob layout (shared with the other Azure SDKs):
`<namespace>/<eventhub>/<consumer group>/ownership/<partition>` and
`.../checkpoint/<partition>`, with the data in blob metadata.
"""
struct BlobCheckpointStore <: CheckpointStore
    account_url::String          # scheme://host[:port][/account for path-style]
    account_name::String
    container::String
    credential::Any
    sas::Union{Nothing,String}
    retry::RetryOptions
end

function BlobCheckpointStore(blob_account_url::AbstractString, container_name::AbstractString; credential=nothing,
    retry_total::Integer=3)
    occursin(r"^[a-z0-9](?:[a-z0-9]|-(?=[a-z0-9])){2,62}$", container_name) ||
        throw(ArgumentError("invalid blob container name $(repr(container_name))"))
    u = URIs.URI(blob_account_url)
    (u.scheme in ("https", "http") && !isempty(u.host)) || throw(ArgumentError("invalid blob_account_url"))
    if u.scheme == "http" && !(u.host in ("127.0.0.1", "localhost") || endswith(u.host, ".localhost"))
        throw(ArgumentError("blob_account_url must use https (http is only allowed for local emulators)"))
    end
    sas = isempty(u.query) ? nothing : String(u.query)
    path = rstrip(u.path, '/')
    base = string(u.scheme, "://", u.host, isempty(u.port) ? "" : ":" * u.port, path)
    account = isempty(path) ? String(first(split(u.host, '.'))) : String(split(strip(path, '/'), '/')[1])
    if credential isa AzureSasCredential
        sas = lstrip(AzureCore.signature(credential), '?')
        credential = nothing
    end
    credential === nothing || credential isa TokenCredential || credential isa AzureNamedKeyCredential ||
        throw(ArgumentError("unsupported credential type $(typeof(credential))"))
    return BlobCheckpointStore(base, account, String(container_name), credential, sas,
        RetryOptions(retry_total=retry_total))
end

function from_connection_string(::Type{BlobCheckpointStore}, conn_str::AbstractString, container_name::AbstractString;
    kwargs...)
    d = parse_connection_string_pairs(conn_str)
    if lowercase(get(d, "usedevelopmentstorage", "false")) == "true"
        return BlobCheckpointStore("http://127.0.0.1:10000/$(AZURITE_ACCOUNT)", container_name;
            credential=AzureNamedKeyCredential(AZURITE_ACCOUNT, AZURITE_KEY), kwargs...)
    end
    account = get(d, "accountname", nothing)
    key = get(d, "accountkey", nothing)
    sas = get(d, "sharedaccesssignature", nothing)
    endpoint = get(d, "blobendpoint", nothing)
    if endpoint === nothing
        account === nothing && throw(ArgumentError("connection string needs AccountName or BlobEndpoint"))
        proto = get(d, "defaultendpointsprotocol", "https")
        suffix = get(d, "endpointsuffix", "core.windows.net")
        endpoint = "$(proto)://$(account).blob.$(suffix)"
    end
    cred = key !== nothing ? AzureNamedKeyCredential(something(account, ""), key) :
           sas !== nothing ? AzureSasCredential(sas) : nothing
    return BlobCheckpointStore(endpoint, container_name; credential=cred, kwargs...)
end

Base.show(io::IO, s::BlobCheckpointStore) =
    print(io, "BlobCheckpointStore(", s.account_url, "/", s.container, ")")

_prefix(fqns, eh, cg, kind) = string(lowercase(fqns), "/", lowercase(eh), "/", lowercase(cg), "/", kind, "/")

# --- request signing -------------------------------------------------------------

function _shared_key_signature(s::BlobCheckpointStore, method, url, headers::Vector{Pair{String,String}}, key_name, key)
    hget(n) = (i = findfirst(p -> lowercase(p.first) == lowercase(n), headers); i === nothing ? "" : headers[i].second)
    clen = hget("Content-Length")
    clen == "0" && (clen = "")
    canon_headers = sort([(lowercase(k), strip(v)) for (k, v) in headers if startswith(lowercase(k), "x-ms-")])
    u = URIs.URI(url)
    resource = string("/", key_name, u.path)
    params = Dict{String,Vector{String}}()
    for (k, v) in URIs.queryparampairs(u.query)
        push!(get!(params, lowercase(k), String[]), v)
    end
    for k in sort(collect(keys(params)))
        resource *= string("\n", k, ":", join(sort(params[k]), ","))
    end
    to_sign = join([method, hget("Content-Encoding"), hget("Content-Language"), clen, hget("Content-MD5"),
            hget("Content-Type"), "", hget("If-Modified-Since"), hget("If-Match"), hget("If-None-Match"),
            hget("If-Unmodified-Since"), hget("Range")], "\n") * "\n" *
              join((string(k, ":", v, "\n") for (k, v) in canon_headers), "") * resource
    sig = base64encode(SHA.hmac_sha256(base64decode(key), Vector{UInt8}(codeunits(to_sign))))
    return "SharedKey $(key_name):$(sig)"
end

function _blob_request(s::BlobCheckpointStore, method::String, path::String; query::Vector{Pair{String,String}}=Pair{String,String}[],
    headers::Vector{Pair{String,String}}=Pair{String,String}[], ok=(200, 201))
    q = join((string(URIs.escapeuri(k), "=", URIs.escapeuri(v)) for (k, v) in query), "&")
    s.sas === nothing || (q = isempty(q) ? s.sas : q * "&" * s.sas)
    url = string(s.account_url, "/", s.container, isempty(path) ? "" : "/" * join(URIs.escapeuri.(split(path, '/')), "/"),
        isempty(q) ? "" : "?" * q)
    hdrs = vcat(Pair{String,String}["x-ms-version" => BLOB_API_VERSION,
            "x-ms-date" => Dates.format(Dates.now(Dates.UTC), Dates.dateformat"e, dd u yyyy HH:MM:SS") * " GMT",
            "User-Agent" => AzureCore.user_agent("eventhub-checkpointstoreblob")], headers)
    method == "PUT" && push!(hdrs, "Content-Length" => "0")
    cred = s.credential
    if cred isa AzureNamedKeyCredential
        nk = named_key(cred)
        push!(hdrs, "Authorization" => _shared_key_signature(s, method, url, hdrs, nk.name, nk.key))
    elseif cred isa TokenCredential
        push!(hdrs, "Authorization" => "Bearer " * get_token(cred, STORAGE_SCOPE).token)
    end
    resp = AzureCore.send_request(method, url; headers=hdrs, body=method == "PUT" ? UInt8[] : nothing, retry=s.retry,
        timeout=30.0)
    return resp
end

_quote_etag(e::AbstractString) = isempty(e) || startswith(e, '"') ? String(e) : string('"', e, '"')

_xml_unescape(s) = replace(s, "&lt;" => "<", "&gt;" => ">", "&quot;" => "\"", "&apos;" => "'", "&amp;" => "&")
_xml_tag(xml, tag) = (m = match(Regex("<$(tag)>(.*?)</$(tag)>", "s"), xml); m === nothing ? nothing : _xml_unescape(m.captures[1]))

function _parse_http_date(s)
    s === nothing && return nothing
    dt = tryparse(Dates.DateTime, replace(strip(s), r"\s*GMT$" => ""), Dates.dateformat"e, d u y H:M:S")
    return dt === nothing ? nothing : Dates.datetime2unix(dt)
end

"List blobs under `prefix` with metadata: Vector of (name, etag, last_modified, metadata)."
function _list_blobs(s::BlobCheckpointStore, prefix::String)
    out = Tuple{String,String,Float64,Dict{String,String}}[]
    marker = ""
    while true
        query = ["restype" => "container", "comp" => "list", "prefix" => prefix, "include" => "metadata"]
        isempty(marker) || push!(query, "marker" => marker)
        resp = _blob_request(s, "GET", ""; query=query)
        resp.status == 200 || AzureCore.raise_for_status(resp; message="listing checkpoint blobs failed")
        xml = String(copy(resp.body))
        for m in eachmatch(r"<Blob>(.*?)</Blob>"s, xml)
            b = m.captures[1]
            name = _xml_tag(b, "Name")
            name === nothing && continue
            etag = _quote_etag(something(_xml_tag(b, "Etag"), ""))
            lm = something(_parse_http_date(_xml_tag(b, "Last-Modified")), 0.0)
            meta = Dict{String,String}()
            md = _xml_tag(b, "Metadata")
            if md !== nothing
                for mm in eachmatch(r"<([A-Za-z0-9_]+)>(.*?)</\1>"s, md)
                    meta[lowercase(mm.captures[1])] = _xml_unescape(mm.captures[2])
                end
            end
            push!(out, (name, etag, lm, meta))
        end
        marker = something(_xml_tag(xml, "NextMarker"), "")
        isempty(marker) && break
    end
    return out
end

function list_ownership(s::BlobCheckpointStore, fully_qualified_namespace, eventhub_name, consumer_group)
    prefix = _prefix(fully_qualified_namespace, eventhub_name, consumer_group, "ownership")
    return [Dict{String,Any}("fully_qualified_namespace" => fully_qualified_namespace, "eventhub_name" => eventhub_name,
        "consumer_group" => consumer_group, "partition_id" => name[length(prefix)+1:end],
        "owner_id" => get(meta, "ownerid", ""), "etag" => etag, "last_modified_time" => lm)
            for (name, etag, lm, meta) in _list_blobs(s, prefix)]
end

function claim_ownership(s::BlobCheckpointStore, ownerships::AbstractVector)
    claimed = Dict{String,Any}[]
    for o in ownerships
        name = _prefix(o["fully_qualified_namespace"], o["eventhub_name"], o["consumer_group"], "ownership") *
               string(o["partition_id"])
        etag = get(o, "etag", nothing)
        headers = Pair{String,String}["x-ms-blob-type" => "BlockBlob", "x-ms-meta-ownerid" => string(o["owner_id"])]
        push!(headers, etag === nothing ? ("If-None-Match" => "*") : ("If-Match" => string(etag)))
        resp = try
            _blob_request(s, "PUT", name; headers=headers)
        catch err
            @debug "claiming partition ownership failed" partition = o["partition_id"] exception = err
            continue
        end
        if resp.status in (200, 201)
            new = Dict{String,Any}(o)
            new["etag"] = _quote_etag(HTTP.header(resp, "ETag", ""))
            new["last_modified_time"] = something(_parse_http_date(HTTP.header(resp, "Last-Modified", "")), time())
            push!(claimed, new)
        elseif !(resp.status in (409, 412))
            @debug "unexpected status claiming partition ownership" status = resp.status
        end
    end
    return claimed
end

function update_checkpoint(s::BlobCheckpointStore, checkpoint::AbstractDict)
    name = _prefix(checkpoint["fully_qualified_namespace"], checkpoint["eventhub_name"], checkpoint["consumer_group"],
        "checkpoint") * string(checkpoint["partition_id"])
    headers = Pair{String,String}["x-ms-blob-type" => "BlockBlob"]
    off = get(checkpoint, "offset", nothing)
    off === nothing || push!(headers, "x-ms-meta-offset" => string(off))
    sn = get(checkpoint, "sequence_number", nothing)
    sn === nothing || push!(headers, "x-ms-meta-sequencenumber" => string(sn))
    resp = _blob_request(s, "PUT", name; headers=headers)
    resp.status in (200, 201) || AzureCore.raise_for_status(resp; message="updating checkpoint failed")
    return nothing
end

function list_checkpoints(s::BlobCheckpointStore, fully_qualified_namespace, eventhub_name, consumer_group)
    prefix = _prefix(fully_qualified_namespace, eventhub_name, consumer_group, "checkpoint")
    out = Dict{String,Any}[]
    for (name, _, _, meta) in _list_blobs(s, prefix)
        sn = tryparse(Int64, get(meta, "sequencenumber", ""))
        push!(out, Dict{String,Any}("fully_qualified_namespace" => fully_qualified_namespace,
            "eventhub_name" => eventhub_name, "consumer_group" => consumer_group,
            "partition_id" => name[length(prefix)+1:end], "offset" => get(meta, "offset", nothing),
            "sequence_number" => sn))
    end
    return out
end
