# BlobLeaseClient (Python: azure.storage.blob.BlobLeaseClient).

"""
    BlobLeaseClient(client; lease_id=nothing)

Manage the lease of a blob or container (`client` is a `BlobClient` or
`ContainerClient`). `lease_id` defaults to a new UUID. Operations:
[`acquire`](@ref), [`renew`](@ref), [`release`](@ref), [`change`](@ref) and
[`break_lease`](@ref). Pass the lease client (or its `id`) as `lease=` to
operations on a leased resource. Fields: `id`, `etag`, `last_modified`.
"""
mutable struct BlobLeaseClient
    client::Union{BlobClient, ContainerClient}
    id::String
    etag::Union{Nothing, String}
    last_modified::Union{Nothing, DateTime}
end
BlobLeaseClient(client::Union{BlobClient, ContainerClient}; lease_id = nothing) =
    BlobLeaseClient(client, lease_id === nothing ? string(uuid4()) : String(lease_id), nothing, nothing)
Base.show(io::IO, l::BlobLeaseClient) = print(io, "BlobLeaseClient(", repr(l.id), ")")

function _lease_op(l::BlobLeaseClient, action::String, headers, kwargs; expected)
    c = l.client
    blob = c isa BlobClient
    o = _opts(kwargs, blob ? _COND : (:if_modified_since, :if_unmodified_since), (:timeout,))
    path, q = blob ? (_blob_path(c), Pair{String, String}["comp" => "lease"]) :
        (_container_path(c), Pair{String, String}[_CONTAINER_Q, "comp" => "lease"])
    h = vcat(Pair{String, String}["x-ms-lease-action" => action], headers, _condition_headers(o; lease = false, tags = blob))
    resp = _req(c, "PUT", path; query = q, headers = h, expected = expected, timeout = _timeout(o), operation = "$(action)_lease")
    l.etag = _hdr(resp, "ETag")
    l.last_modified = _hdate(resp, "Last-Modified")
    return resp
end

"""
    acquire(lease_client; lease_duration=-1, <access conditions>, timeout=nothing)

Acquire the lease for `lease_duration` seconds (15–60, or -1 for infinite).
"""
function acquire(l::BlobLeaseClient; lease_duration::Integer = -1, kwargs...)
    (lease_duration == -1 || 15 <= lease_duration <= 60) ||
        throw(ArgumentError("lease_duration must be -1 (infinite) or between 15 and 60 seconds"))
    resp = _lease_op(
        l, "acquire", ["x-ms-lease-duration" => string(lease_duration), "x-ms-proposed-lease-id" => l.id],
        kwargs; expected = (201,)
    )
    l.id = something(_hdr(resp, "x-ms-lease-id"), l.id)
    return nothing
end

"""
    renew(lease_client; <access conditions>, timeout=nothing)

Renew the lease (restarting its duration).
"""
renew(l::BlobLeaseClient; kwargs...) = (_lease_op(l, "renew", ["x-ms-lease-id" => l.id], kwargs; expected = (200,)); nothing)

"""
    release(lease_client; <access conditions>, timeout=nothing)
"""
release(l::BlobLeaseClient; kwargs...) = (_lease_op(l, "release", ["x-ms-lease-id" => l.id], kwargs; expected = (200,)); nothing)

"""
    change(lease_client, proposed_lease_id; <access conditions>, timeout=nothing)

Change the lease id of an active lease.
"""
function change(l::BlobLeaseClient, proposed_lease_id::AbstractString; kwargs...)
    resp = _lease_op(
        l, "change", ["x-ms-lease-id" => l.id, "x-ms-proposed-lease-id" => String(proposed_lease_id)],
        kwargs; expected = (200,)
    )
    l.id = something(_hdr(resp, "x-ms-lease-id"), String(proposed_lease_id))
    return nothing
end

"""
    break_lease(lease_client; lease_break_period=nothing, <access conditions>, timeout=nothing) -> Int

Break the lease; returns the seconds remaining until it can be re-acquired.
"""
function break_lease(l::BlobLeaseClient; lease_break_period = nothing, kwargs...)
    h = lease_break_period === nothing ? Pair{String, String}[] : ["x-ms-lease-break-period" => string(lease_break_period)]
    resp = _lease_op(l, "break", h, kwargs; expected = (202,))
    return Int(something(_hint(resp, "x-ms-lease-time"), 0))
end

"""
    acquire_lease(client; lease_duration=-1, lease_id=nothing, <access conditions>, timeout=nothing) -> BlobLeaseClient

Acquire a lease on a blob or container.
"""
function acquire_lease(c::Union{BlobClient, ContainerClient}; lease_duration::Integer = -1, lease_id = nothing, kwargs...)
    l = BlobLeaseClient(c; lease_id = lease_id)
    acquire(l; lease_duration = lease_duration, kwargs...)
    return l
end
