# upload_blob and download_blob (StorageStreamDownloader).

# --- data sources -------------------------------------------------------------------------

# Yields successive chunks of the input without copying byte vectors.
mutable struct _Source
    data::Any                      # AbstractVector{UInt8} or IO
    pos::Int64                     # bytes produced so far
    total::Union{Nothing, Int64}    # known total length
end

function _Source(data, length)
    if data isa AbstractString
        data = codeunits(String(data))
    end
    if data isa AbstractVector{UInt8}
        n = length === nothing ? Base.length(data) : Int(length)
        n > Base.length(data) && throw(ArgumentError("length exceeds the size of the data"))
        return _Source(data, 0, n)
    elseif data isa IO
        total = length === nothing ? nothing : Int64(length)
        if total === nothing && data isa IOStream
            try
                total = filesize(data) - position(data)
            catch
            end
        elseif total === nothing && data isa IOBuffer
            total = bytesavailable(data)
        end
        return _Source(data, 0, total)
    end
    throw(ArgumentError("unsupported data type $(typeof(data)); expected bytes, a string or an IO"))
end

"Next chunk of at most `n` bytes (empty when exhausted)."
function _next_chunk!(s::_Source, n::Integer)
    if s.data isa AbstractVector{UInt8}
        stop = min(s.pos + n, s.total)
        chunk = view(s.data, (s.pos + 1):stop)
        s.pos = stop
        return chunk
    end
    want = s.total === nothing ? Int(n) : Int(min(n, s.total - s.pos))
    want <= 0 && return UInt8[]
    buf = read(s.data, want)
    s.total !== nothing && length(buf) < want && throw(ArgumentError("the stream ended before `length` bytes were read"))
    s.pos += length(buf)
    return buf
end

_exhausted(s::_Source) = s.total !== nothing ? s.pos >= s.total : s.data isa IO && eof(s.data)

# Run `f(item)` for items produced by `produce(put)` with at most `n` concurrent tasks.
# The first error stops production and is rethrown.
function _parallel_foreach(f, produce, n::Int)
    if n <= 1
        produce(f)
        return nothing
    end
    ch = Channel{Any}(n)
    failed = Ref{Any}(nothing)
    lk = ReentrantLock()
    workers = map(1:n) do _
        Threads.@spawn begin
            for item in ch
                @lock lk (failed[] === nothing || continue)
                try
                    f(item)
                catch err
                    @lock lk (failed[] === nothing && (failed[] = err))
                end
            end
        end
    end
    try
        produce() do item
            @lock lk (failed[] === nothing || return nothing)
            put!(ch, item)
            return nothing
        end
    catch err
        @lock lk (failed[] === nothing && (failed[] = err))
    finally
        close(ch)
    end
    foreach(wait, workers)
    failed[] === nothing || throw(failed[])
    return nothing
end

"Apply `f` to `items` with at most `n` concurrent tasks; results in order. The first error is rethrown as is."
function _parallel_map(f, items::AbstractVector, n::Int)
    results = Vector{Any}(undef, length(items))
    _parallel_foreach(i -> (results[i] = f(items[i])), put -> foreach(put, eachindex(items)), n)
    return results
end

function _progress(hook, lk, counter::Ref{Int64}, n, total)
    hook === nothing && return nothing
    cur = @lock lk (counter[] += n)
    hook(cur, total)
    return nothing
end

# --- upload -----------------------------------------------------------------------------------

const _UPLOAD_OPTS = (
    :blob_type, :length, :metadata, :overwrite, :content_settings, :validate_content, :max_concurrency,
    :tags, :standard_blob_tier, :premium_page_blob_tier, :maxsize_condition, :lease, :immutability_policy,
    :legal_hold, :progress_hook, :encoding, :timeout,
)

"""
    upload_blob(blob_client, data; blob_type=BlobType.BLOCKBLOB, length=nothing, metadata=nothing, overwrite=false,
                content_settings=nothing, validate_content=false, max_concurrency=1, tags=nothing,
                standard_blob_tier=nothing, premium_page_blob_tier=nothing, maxsize_condition=nothing,
                lease=nothing, etag=nothing, match_condition=nothing, if_modified_since=nothing,
                if_unmodified_since=nothing, if_tags_match_condition=nothing, cpk=nothing, encryption_scope=nothing,
                immutability_policy=nothing, legal_hold=nothing, progress_hook=nothing, timeout=nothing) -> Dict

Upload `data` (bytes, a string or any `IO`, e.g. an open file) as a block,
page or append blob.

- Block blobs up to `max_single_put_size` (64 MiB) are uploaded with a
  single request; larger or unknown-length data is split into blocks of
  `max_block_size` (4 MiB) that are uploaded `max_concurrency` at a time and
  then committed.
- Page blobs need a known length that is a multiple of 512; all-zero pages
  are skipped.
- Append blobs are created when missing (or replaced with `overwrite=true`)
  and the data is appended.

Without `overwrite=true` an existing blob raises `ResourceExistsError`
(block and page blobs). `progress_hook(current, total)` is called as data is
sent. Returns `etag`, `last_modified` and the other response properties.
"""
function upload_blob(b::BlobClient, data; kwargs...)
    o = _opts(kwargs, _COND, _CPK, _UPLOAD_OPTS)
    enc = get(o, :encoding, "UTF-8")
    uppercase(replace(String(enc), "-" => "")) == "UTF8" || throw(ArgumentError("only UTF-8 encoding is supported"))
    bt = get(o, :blob_type, BlobType.BLOCKBLOB)
    bt isa AbstractString && (bt = something(enum_parse(BlobType.T, bt), bt))
    bt isa BlobType.T || throw(ArgumentError("invalid blob_type $(repr(bt))"))
    src = _Source(data, get(o, :length, nothing))
    if bt == BlobType.BLOCKBLOB
        return _upload_block_blob(b, src, o)
    elseif bt == BlobType.PAGEBLOB
        return _upload_page_blob(b, src, o)
    else
        return _upload_append_blob(b, src, o)
    end
end

function _overwrite_conditions(o)
    get(o, :overwrite, false) && return o
    haskey(o, :match_condition) && return o
    o2 = Dict{Symbol, Any}(o)
    o2[:match_condition] = MatchConditions.IfMissing
    return o2
end

function _upload_block_blob(b::BlobClient, src::_Source, o)
    cfg = b.config
    validate = get(o, :validate_content, false)
    hook = get(o, :progress_hook, nothing)
    cond = _overwrite_conditions(o)
    common = vcat(
        _content_settings_headers(get(o, :content_settings, nothing)), _metadata_headers(get(o, :metadata, nothing)),
        _tags_header(get(o, :tags, nothing)), _condition_headers(cond), _cpk_headers(cfg, o), _immutability_headers(o)
    )
    tier = get(o, :standard_blob_tier, nothing)
    tier === nothing || push!(common, "x-ms-access-tier" => enum_value(tier))
    single = src.total !== nothing && src.total <= cfg.transfer[:max_single_put_size]
    first = nothing
    if !single
        # Unknown length: if the whole stream fits in one block, upload it in one request.
        first = _next_chunk!(src, cfg.transfer[:max_block_size])
        single = _exhausted(src)
    end
    if single
        body = first === nothing ? _next_chunk!(src, src.total) : first
        h = vcat(Pair{String, String}["x-ms-blob-type" => "BlockBlob"], common, _md5_header(validate, body))
        resp = _req(
            b, "PUT", _blob_path(b); headers = h, body = body, expected = (201,), timeout = _timeout(o),
            operation = "upload_blob"
        )
        _check_md5_response(validate, body, resp)
        hook === nothing || hook(length(body), length(body))
        return response_dict(resp)
    end
    prefix = replace(string(uuid4()), "-" => "")
    ids = String[]
    lk = ReentrantLock()
    counter = Ref{Int64}(0)
    stage_opts = Dict{Symbol, Any}(k => v for (k, v) in o if k in (:lease, :cpk, :encryption_scope, :validate_content, :timeout))
    produce = function (put)
        i = 0
        chunk = first
        while !isempty(chunk)
            i += 1
            id = string(prefix, lpad(i, 8, '0'))
            push!(ids, id)
            put((id, chunk))
            chunk = _next_chunk!(src, cfg.transfer[:max_block_size])
        end
        return
    end
    _parallel_foreach(produce, get(o, :max_concurrency, 1)) do (id, chunk)
        stage_block(b, id, chunk; stage_opts...)
        _progress(hook, lk, counter, length(chunk), src.total)
    end
    commit_opts = Dict{Symbol, Any}(
        k => v for (k, v) in cond if k in (
                _COND..., _CPK..., :content_settings, :metadata,
                :tags, :standard_blob_tier, :immutability_policy, :legal_hold, :lease, :timeout,
            )
    )
    return commit_block_list(b, ids; commit_opts...)
end

function _upload_page_blob(b::BlobClient, src::_Source, o)
    cfg = b.config
    src.total === nothing && throw(ArgumentError("Please specify the length of a page blob upload (length=...)"))
    total = src.total
    total % 512 == 0 || throw(ArgumentError("page blob size must be a multiple of 512, got $(total)"))
    create_opts = Dict{Symbol, Any}(
        k => v for (k, v) in _overwrite_conditions(o) if k in (
                _COND..., _CPK...,
                :content_settings, :metadata, :premium_page_blob_tier, :tags, :immutability_policy, :legal_hold, :lease, :timeout,
            )
    )
    result = create_page_blob(b, total; create_opts...)
    hook = get(o, :progress_hook, nothing)
    lk = ReentrantLock()
    counter = Ref{Int64}(0)
    page_opts = Dict{Symbol, Any}(k => v for (k, v) in o if k in (:lease, :cpk, :encryption_scope, :validate_content, :timeout))
    last = Ref{Any}(result)
    produce = function (put)
        offset = 0
        while offset < total
            chunk = _next_chunk!(src, cfg.transfer[:max_page_size])
            isempty(chunk) && break
            put((offset, chunk))
            offset += length(chunk)
        end
        return
    end
    _parallel_foreach(produce, get(o, :max_concurrency, 1)) do (offset, chunk)
        if any(!iszero, chunk)
            r = upload_page(b, chunk, offset, length(chunk); page_opts...)
            @lock lk (last[] = r)
        end
        _progress(hook, lk, counter, length(chunk), total)
    end
    return last[]
end

function _upload_append_blob(b::BlobClient, src::_Source, o)
    cfg = b.config
    create_opts = Dict{Symbol, Any}(
        k => v for (k, v) in o if k in (
                _COND..., _CPK..., :content_settings, :metadata,
                :tags, :immutability_policy, :legal_hold, :lease, :timeout,
            )
    )
    append_opts = Dict{Symbol, Any}(
        k => v for (k, v) in o if k in (
                _COND..., _CPK..., :validate_content,
                :maxsize_condition, :lease, :timeout,
            )
    )
    overwrite = get(o, :overwrite, false)
    result = overwrite ? create_append_blob(b; create_opts...) : nothing
    hook = get(o, :progress_hook, nothing)
    chunk = _next_chunk!(src, cfg.transfer[:max_block_size])
    if isempty(chunk)
        # Nothing to append: just make sure the blob exists.
        if !overwrite
            try
                result = create_append_blob(b; merge(create_opts, Dict{Symbol, Any}(:match_condition => MatchConditions.IfMissing))...)
            catch err
                err isa Union{ResourceExistsError, ResourceModifiedError} || rethrow()
                result = Dict{String, Any}()
            end
        end
        return result
    end
    sent = 0
    position = nothing
    while !isempty(chunk)
        opts = position === nothing ? append_opts : merge(append_opts, Dict{Symbol, Any}(:appendpos_condition => position))
        result = try
            append_block(b, chunk; opts...)
        catch err
            # Create the blob on first use (Python semantics) and retry.
            (position === nothing && err isa ResourceNotFoundError && err.error_code == "BlobNotFound") || rethrow()
            create_append_blob(b; create_opts...)
            append_block(b, chunk; opts...)
        end
        position = something(tryparse(Int64, string(get(result, "blob_append_offset", ""))), 0) + length(chunk)
        sent += length(chunk)
        hook === nothing || hook(sent, src.total)
        chunk = _next_chunk!(src, cfg.transfer[:max_block_size])
    end
    return result
end

# --- download -----------------------------------------------------------------------------------

"""
    StorageStreamDownloader

Returned by [`download_blob`](@ref) (Python: `StorageStreamDownloader`).
Read the content with [`readall`](@ref), [`readinto`](@ref),
[`chunks`](@ref) or [`content_as_text`](@ref), or use it as a Julia `IO`
(`read(d)`, `read(d, String)`, `readline(d)`, `read(d, n)`, ...), which
streams the blob chunk by chunk. Fields: `name`, `container`, `properties`
(`BlobProperties`), `size` (bytes to download).
"""
mutable struct StorageStreamDownloader <: IO
    client::BlobClient
    name::String
    container::String
    properties::BlobProperties
    size::Int64
    start::Int64                   # blob offset of the first byte to download
    headers::Vector{Pair{String, String}}   # per-chunk request headers (lease, conditions, cpk)
    query::Vector{Pair{String, String}}
    validate::Bool
    max_concurrency::Int
    encoding::Union{Nothing, String}
    progress_hook::Any
    timeout::Any
    buffer::Vector{UInt8}
    bufpos::Int                    # next unread index in buffer
    next_offset::Int64             # next offset (relative to start) not yet fetched
    consumed::Int64                # bytes handed to the caller
    lock::ReentrantLock
end

Base.show(io::IO, d::StorageStreamDownloader) =
    print(io, "StorageStreamDownloader(", repr(d.name), ", size=", d.size, ", consumed=", d.consumed, ")")

_download_headers(b::BlobClient, o) = vcat(_condition_headers(o), _cpk_headers(b.config, o))

function _parse_content_range_total(s)
    s === nothing && return nothing
    m = match(r"/(\d+)\s*$", s)
    return m === nothing ? nothing : parse(Int64, m.captures[1])
end

"""
    download_blob(blob_client; offset=nothing, length=nothing, version_id=nothing, validate_content=false,
                  max_concurrency=1, encoding=nothing, lease=nothing, etag=nothing, match_condition=nothing,
                  if_modified_since=nothing, if_unmodified_since=nothing, if_tags_match_condition=nothing,
                  cpk=nothing, progress_hook=nothing, timeout=nothing) -> StorageStreamDownloader

Start downloading the blob (or the byte range `offset`, `length`). The
first `max_single_get_size` bytes (32 MiB) are fetched immediately; the
rest in `max_chunk_get_size` chunks (4 MiB), `max_concurrency` at a time,
pinned to the blob's ETag so a concurrent modification is detected
(`ResourceModifiedError`) instead of producing a torn read. With
`validate_content=true` every chunk's MD5 is checked. With `encoding`
(`"UTF-8"`) [`readall`](@ref) returns a `String`.
"""
function download_blob(b::BlobClient; kwargs...)
    o = _opts(
        kwargs, _COND, _CPK, (
            :offset, :length, :version_id, :validate_content, :max_concurrency, :encoding,
            :lease, :progress_hook, :timeout,
        )
    )
    cfg = b.config
    offset = get(o, :offset, nothing)
    len = get(o, :length, nothing)
    (len !== nothing && offset === nothing) && throw(ArgumentError("Offset value must not be None if length is set."))
    (offset !== nothing && offset < 0) && throw(ArgumentError("offset must be >= 0"))
    (len !== nothing && len <= 0) && throw(ArgumentError("length must be > 0"))
    validate = get(o, :validate_content, false)
    enc = get(o, :encoding, nothing)
    q = _blob_query(b)
    v = get(o, :version_id, nothing)
    v === nothing || (filter!(p -> p.first != "versionid", q); push!(q, "versionid" => String(v)))
    headers = _download_headers(b, o)
    start = something(offset, 0)
    first_size = validate ? min(cfg.transfer[:max_chunk_get_size], 4 * 1024 * 1024) : cfg.transfer[:max_single_get_size]
    len === nothing || (first_size = min(first_size, len))
    h = vcat(headers, ["x-ms-range" => _range_header(start, first_size)])
    validate && push!(h, "x-ms-range-get-content-md5" => "true")
    resp = _req(b, "GET", _blob_path(b); query = q, headers = h, timeout = _timeout(o), raise = false)
    if resp.status == 416 && offset === nothing
        # Empty blob: a ranged GET is not satisfiable.
        resp = _req(b, "GET", _blob_path(b); query = q, headers = headers, timeout = _timeout(o), raise = false)
    end
    200 <= resp.status < 300 || throw(storage_error(resp; operation = "download_blob"))
    body = resp.body
    validate && _verify_md5(resp, body)
    props = _blob_properties_from_headers(resp, b)
    blob_size = something(_parse_content_range_total(_hdr(resp, "Content-Range")), Int64(length(body)))
    size = len === nothing ? max(0, blob_size - start) : min(Int64(len), max(0, blob_size - start))
    length(body) > size && (body = body[1:size])
    props.size = size
    if len !== nothing || offset !== nothing
        props.content_range = size == 0 ? nothing : "bytes $(start)-$(start + size - 1)/$(blob_size)"
    end
    # Later chunks must come from the same version of the blob.
    chunk_headers = copy(headers)
    if !haskey(o, :etag) && props.etag !== nothing
        push!(chunk_headers, "If-Match" => props.etag)
    end
    d = StorageStreamDownloader(
        b, b.blob_name, b.container_name, props, size, start, chunk_headers, q, validate,
        max(1, get(o, :max_concurrency, 1)), enc === nothing ? nothing : String(enc), get(o, :progress_hook, nothing),
        _timeout(o), body, 1, length(body), 0, ReentrantLock()
    )
    d.progress_hook === nothing || d.progress_hook(length(body), size)
    return d
end

function _verify_md5(resp, body)
    got = _hdr(resp, "Content-MD5")
    got === nothing && throw(ContentValidationError("validate_content=true but the response carried no Content-MD5"))
    return base64decode(got) == md5(body) ||
        throw(ContentValidationError("MD5 mismatch: the downloaded content was corrupted in transit"))
end

"Fetch relative range [rel, rel+n) of the download."
function _fetch_range(d::StorageStreamDownloader, rel::Int64, n::Int64)
    b = d.client
    h = vcat(d.headers, ["x-ms-range" => _range_header(d.start + rel, n)])
    d.validate && push!(h, "x-ms-range-get-content-md5" => "true")
    attempt = 0
    while true
        resp = _req(b, "GET", _blob_path(b); query = d.query, headers = h, timeout = d.timeout, operation = "download_blob")
        body = resp.body
        if length(body) == n
            d.validate && _verify_md5(resp, body)
            return body
        end
        attempt += 1
        attempt > b.config.retry.retry_total &&
            throw(IncompleteReadError("received $(length(body)) of $(n) bytes for a blob range"))
        sleep(AzureCore.backoff_delay(b.config.retry, attempt))
    end
    return
end

_chunk_size(d::StorageStreamDownloader) = Int64(
    d.validate ? min(d.client.config.transfer[:max_chunk_get_size], 4 * 1024 * 1024) :
        d.client.config.transfer[:max_chunk_get_size]
)

function _remaining_ranges(d::StorageStreamDownloader)
    cs = _chunk_size(d)
    return [(r, min(cs, d.size - r)) for r in d.next_offset:cs:(d.size - 1)]
end

"""
    readall(downloader) -> Vector{UInt8} (or String when an `encoding` was given)

Download and return everything not yet read.
"""
function readall(d::StorageStreamDownloader)
    data = @lock d.lock begin
        rest = d.size - d.consumed
        out = Vector{UInt8}(undef, rest)
        nbuf = length(d.buffer) - d.bufpos + 1
        copyto!(out, 1, d.buffer, d.bufpos, nbuf)
        base = d.next_offset - nbuf        # relative offset corresponding to out[1]
        ranges = _remaining_ranges(d)
        counter = Ref{Int64}(d.next_offset)
        plk = ReentrantLock()
        _parallel_map(ranges, d.max_concurrency) do (rel, n)
            chunk = _fetch_range(d, rel, n)
            copyto!(out, rel - base + 1, chunk, 1, n)
            _progress(d.progress_hook, plk, counter, n, d.size)
            nothing
        end
        d.buffer = UInt8[]
        d.bufpos = 1
        d.next_offset = d.size
        d.consumed = d.size
        out
    end
    return d.encoding === nothing ? data : String(data)
end

"""
    readinto(downloader, stream::IO) -> Int

Download everything not yet read into `stream`; returns the number of
bytes written. Chunks are fetched `max_concurrency` at a time and written in order.
"""
function readinto(d::StorageStreamDownloader, stream::IO)
    @lock d.lock begin
        written = 0
        if d.bufpos <= length(d.buffer)
            written += write(stream, view(d.buffer, d.bufpos:length(d.buffer)))
        end
        ranges = _remaining_ranges(d)
        counter = Ref{Int64}(d.next_offset)
        plk = ReentrantLock()
        for window in Iterators.partition(ranges, d.max_concurrency)
            chunks = _parallel_map(r -> _fetch_range(d, r[1], r[2]), collect(window), d.max_concurrency)
            for (c, (_, n)) in zip(chunks, window)
                written += write(stream, c)
                _progress(d.progress_hook, plk, counter, n, d.size)
            end
        end
        d.buffer = UInt8[]
        d.bufpos = 1
        d.next_offset = d.size
        d.consumed = d.size
        return written
    end
end

"""
    content_as_text(downloader; encoding="UTF-8") -> String
    content_as_bytes(downloader) -> Vector{UInt8}
"""
function content_as_text(d::StorageStreamDownloader; encoding::AbstractString = "UTF-8")
    uppercase(replace(String(encoding), "-" => "")) == "UTF8" || throw(ArgumentError("only UTF-8 is supported"))
    data = readall(d)
    return data isa String ? data : String(data)
end
function content_as_bytes(d::StorageStreamDownloader)
    data = readall(d)
    return data isa String ? Vector{UInt8}(data) : data
end

# Refill the internal buffer with the next chunk; false at the end.
function _fill!(d::StorageStreamDownloader)
    d.bufpos <= length(d.buffer) && return true
    d.next_offset >= d.size && return false
    n = min(_chunk_size(d), d.size - d.next_offset)
    d.buffer = _fetch_range(d, d.next_offset, n)
    d.bufpos = 1
    d.next_offset += n
    d.progress_hook === nothing || d.progress_hook(d.next_offset, d.size)
    return true
end

"""
    chunks(downloader)

Iterate over the remaining content as byte vectors (the buffered first
chunk, then one vector per `max_chunk_get_size` range).
"""
chunks(d::StorageStreamDownloader) = _ChunkIterator(d)
struct _ChunkIterator
    d::StorageStreamDownloader
end
Base.IteratorSize(::Type{_ChunkIterator}) = Base.SizeUnknown()
Base.eltype(::Type{_ChunkIterator}) = Vector{UInt8}
function Base.iterate(it::_ChunkIterator, _ = nothing)
    d = it.d
    @lock d.lock begin
        _fill!(d) || return nothing
        c = d.bufpos == 1 ? d.buffer : d.buffer[d.bufpos:end]
        d.consumed += length(c)
        d.buffer = UInt8[]
        d.bufpos = 1
        return c, nothing
    end
end

# --- IO interface --------------------------------------------------------------------------------

Base.isopen(d::StorageStreamDownloader) = true
Base.close(::StorageStreamDownloader) = nothing
Base.isreadable(::StorageStreamDownloader) = true
Base.iswritable(::StorageStreamDownloader) = false
Base.eof(d::StorageStreamDownloader) = @lock d.lock (d.consumed >= d.size)
Base.bytesavailable(d::StorageStreamDownloader) = @lock d.lock (length(d.buffer) - d.bufpos + 1)
Base.position(d::StorageStreamDownloader) = d.consumed

function Base.read(d::StorageStreamDownloader, ::Type{UInt8})
    @lock d.lock begin
        _fill!(d) || throw(EOFError())
        x = d.buffer[d.bufpos]
        d.bufpos += 1
        d.consumed += 1
        return x
    end
end

function Base.readbytes!(d::StorageStreamDownloader, b::AbstractVector{UInt8}, nb::Integer = length(b))
    n = 0
    @lock d.lock begin
        while n < nb && _fill!(d)
            k = min(nb - n, length(d.buffer) - d.bufpos + 1)
            n + k > length(b) && resize!(b, n + k)
            copyto!(b, n + 1, d.buffer, d.bufpos, k)
            d.bufpos += k
            d.consumed += k
            n += k
        end
    end
    return n
end

function Base.unsafe_read(d::StorageStreamDownloader, p::Ptr{UInt8}, nb::UInt)
    tmp = Vector{UInt8}(undef, nb)
    n = readbytes!(d, tmp, Int(nb))
    n < nb && throw(EOFError())
    GC.@preserve tmp unsafe_copyto!(p, pointer(tmp), nb)
    return nothing
end

# Scans the buffered chunk instead of reading byte by byte (used by readline/eachline).
function Base.readuntil(d::StorageStreamDownloader, delim::UInt8; keep::Bool = false)
    out = UInt8[]
    @lock d.lock begin
        while _fill!(d)
            i = findnext(==(delim), d.buffer, d.bufpos)
            stop = i === nothing ? length(d.buffer) : i
            append!(out, view(d.buffer, d.bufpos:(keep || i === nothing ? stop : stop - 1)))
            d.consumed += stop - d.bufpos + 1
            d.bufpos = stop + 1
            i === nothing || break
        end
    end
    return out
end

function Base.readavailable(d::StorageStreamDownloader)
    @lock d.lock begin
        _fill!(d) || return UInt8[]
        c = d.buffer[d.bufpos:end]
        d.consumed += length(c)
        d.buffer = UInt8[]
        d.bufpos = 1
        return c
    end
end

Base.read(d::StorageStreamDownloader) = (x = readall(d); x isa String ? Vector{UInt8}(x) : x)
Base.read(d::StorageStreamDownloader, ::Type{String}) = String(read(d))
function Base.read(d::StorageStreamDownloader, nb::Integer)
    b = Vector{UInt8}(undef, min(nb, max(0, d.size - d.consumed)))
    n = readbytes!(d, b, length(b))
    return resize!(b, n)
end
