# Lazily paged results (azure.core.paging.ItemPaged) and match conditions
# (azure.core.MatchConditions).

"""
    ItemPaged{T}

A lazy, iterable collection of `T` fetched from the service one page at a
time (Python: `azure.core.paging.ItemPaged`). Iterate it directly, `collect`
it, or iterate pages with [`by_page`](@ref). Iterating again starts over
with fresh requests.
"""
struct ItemPaged{T}
    fetch::Function        # continuation token (or nothing) -> (Vector{T}, next token or nothing)
    continuation_token::Union{Nothing, String}
end
ItemPaged{T}(fetch::Function) where {T} = ItemPaged{T}(fetch, nothing)

Base.IteratorSize(::Type{<:ItemPaged}) = Base.SizeUnknown()
Base.eltype(::Type{ItemPaged{T}}) where {T} = T
Base.show(io::IO, ::ItemPaged{T}) where {T} = print(io, "ItemPaged{", T, "}(…)")

function Base.iterate(p::ItemPaged{T}, state = (T[], 1, p.continuation_token, true)) where {T}
    items, i, token, first = state
    while i > length(items)
        (!first && token === nothing) && return nothing
        items, token = p.fetch(token)
        items = convert(Vector{T}, items)
        i = 1
        first = false
    end
    return items[i], (items, i + 1, token, first)
end

"""
    PageIterator{T}

Iterator over the pages (`Vector{T}`) of an [`ItemPaged`](@ref). Its
`continuation_token` field holds the token for the page after the last one
returned (or `nothing` when there are no more pages), so a listing can be
resumed later with `by_page(paged; continuation_token=token)`.
"""
mutable struct PageIterator{T}
    fetch::Function
    continuation_token::Union{Nothing, String}
    started::Bool
end
Base.IteratorSize(::Type{<:PageIterator}) = Base.SizeUnknown()
Base.eltype(::Type{PageIterator{T}}) where {T} = Vector{T}

"""
    by_page(paged::ItemPaged; continuation_token=nothing) -> PageIterator

Iterate `paged` one page (`Vector`) at a time, optionally resuming from a
continuation token returned by an earlier iteration.
"""
by_page(p::ItemPaged{T}; continuation_token = p.continuation_token) where {T} =
    PageIterator{T}(p.fetch, continuation_token === nothing ? nothing : String(continuation_token), false)

function Base.iterate(p::PageIterator{T}, _ = nothing) where {T}
    (p.started && p.continuation_token === nothing) && return nothing
    items, token = p.fetch(p.continuation_token)
    p.started = true
    p.continuation_token = token
    return convert(Vector{T}, items), nothing
end

"""
    MatchConditions.Unconditionally / IfNotModified / IfModified / IfPresent / IfMissing

How an `etag` passed with a request is used (Python: `azure.core.MatchConditions`):
`IfNotModified` sends `If-Match: <etag>`, `IfModified` sends
`If-None-Match: <etag>`, `IfPresent` sends `If-Match: *` and `IfMissing`
sends `If-None-Match: *`.
"""
module MatchConditions
    @enum T Unconditionally IfNotModified IfModified IfPresent IfMissing
end

"""
    from_connection_string(ClientType, conn_str, args...; kwargs...)

Create a client (e.g. `ServiceBusClient`, `EventHubProducerClient`,
`BlobServiceClient`, `ContainerClient`, `BlobClient`) from a connection string.
"""
function from_connection_string end
