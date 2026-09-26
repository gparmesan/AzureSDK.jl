# azure.core.utils.CaseInsensitiveDict

"""
    CaseInsensitiveDict{V}()
    CaseInsensitiveDict(pairs)

A `String`-keyed dictionary whose lookups ignore ASCII case, while keys keep
the spelling they were stored with (Python: `azure.core.utils.CaseInsensitiveDict`).
Used for blob and container metadata, whose names Azure treats
case-insensitively. Comparison with another dictionary also ignores key case.
"""
struct CaseInsensitiveDict{V} <: AbstractDict{String,V}
    d::Dict{String,Pair{String,V}}          # lowercase key => (original key => value)
    CaseInsensitiveDict{V}() where {V} = new{V}(Dict{String,Pair{String,V}}())
end
function CaseInsensitiveDict{V}(itr) where {V}
    d = CaseInsensitiveDict{V}()
    for (k, v) in itr
        d[k] = v
    end
    return d
end
CaseInsensitiveDict(itr::AbstractDict{K,V}) where {K,V} = CaseInsensitiveDict{V}(itr)
CaseInsensitiveDict(itr) = CaseInsensitiveDict{Any}(itr)
CaseInsensitiveDict(ps::Pair...) = CaseInsensitiveDict{Any}(ps)
CaseInsensitiveDict(ps::Pair{<:Any,V}...) where {V} = CaseInsensitiveDict{V}(ps)

_ci(k) = lowercase(string(k))
Base.length(d::CaseInsensitiveDict) = length(d.d)
function Base.iterate(d::CaseInsensitiveDict, st...)
    r = iterate(d.d, st...)
    r === nothing && return nothing
    return r[1].second, r[2]
end
function Base.get(d::CaseInsensitiveDict, k, default)
    p = get(d.d, _ci(k), nothing)
    return p === nothing ? default : p.second
end
function Base.getindex(d::CaseInsensitiveDict, k)
    p = get(d.d, _ci(k), nothing)
    p === nothing && throw(KeyError(k))
    return p.second
end
Base.haskey(d::CaseInsensitiveDict, k) = haskey(d.d, _ci(k))
Base.setindex!(d::CaseInsensitiveDict{V}, v, k) where {V} = (d.d[_ci(k)] = String(string(k)) => convert(V, v); d)
Base.delete!(d::CaseInsensitiveDict, k) = (delete!(d.d, _ci(k)); d)
Base.pop!(d::CaseInsensitiveDict, k) = pop!(d.d, _ci(k)).second
Base.pop!(d::CaseInsensitiveDict, k, default) = haskey(d, k) ? pop!(d, k) : default
Base.empty!(d::CaseInsensitiveDict) = (empty!(d.d); d)
Base.copy(d::CaseInsensitiveDict{V}) where {V} = CaseInsensitiveDict{V}(d)
Base.empty(::CaseInsensitiveDict, ::Type{K}, ::Type{V}) where {K,V} = CaseInsensitiveDict{V}()

function _ci_equal(a::AbstractDict, b::AbstractDict)
    length(a) == length(b) || return false
    for (k, v) in a
        haskey(b, k) || return false
        isequal(b[k], v) || return false
    end
    return true
end
Base.:(==)(a::CaseInsensitiveDict, b::CaseInsensitiveDict) = _ci_equal(a, b)
Base.:(==)(a::CaseInsensitiveDict, b::AbstractDict) = _ci_equal(b, a)
Base.:(==)(a::AbstractDict, b::CaseInsensitiveDict) = _ci_equal(a, b)
