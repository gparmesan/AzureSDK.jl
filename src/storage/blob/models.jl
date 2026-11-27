# Models (Python: azure.storage.blob._models).

"""
    @str_enum Name A="a" B="b" ...

Define a module `Name` holding `@enum T A B ...` whose members map to the
service's string values (Python's `str`-valued enums). `enum_value(x)`
returns the string.
"""
macro str_enum(name, args...)
    syms = Symbol[]
    vals = String[]
    for a in args
        (a isa Expr && a.head === :(=)) || error("@str_enum expects NAME=\"value\" pairs")
        push!(syms, a.args[1])
        push!(vals, a.args[2])
    end
    pairs = [:($s => $v) for (s, v) in zip(syms, vals)]
    body = quote
        @enum T $(syms...)
        const VALUES = Dict{T, String}($(pairs...))
    end
    return esc(
        Expr(
            :toplevel, Expr(:module, true, name, body),
            :(enum_value(x::$name.T) = $name.VALUES[x]),
            :(_enum_type(::Type{$name.T}) = $name)
        )
    )
end

enum_value(x::AbstractString) = String(x)
enum_value(::Nothing) = nothing

"Parse a service string into enum type `E` (case-insensitive); unknown values give `nothing`."
function enum_parse(::Type{E}, s) where {E <: Enum}
    s isa AbstractString || return nothing
    ls = lowercase(s)
    for (k, v) in _enum_type(E).VALUES
        lowercase(v) == ls && return k
    end
    return nothing
end

@str_enum BlobType BLOCKBLOB = "BlockBlob" PAGEBLOB = "PageBlob" APPENDBLOB = "AppendBlob"
@str_enum StandardBlobTier ARCHIVE = "Archive" COLD = "Cold" COOL = "Cool" HOT = "Hot"
@str_enum PremiumPageBlobTier P4 = "P4" P6 = "P6" P10 = "P10" P15 = "P15" P20 = "P20" P30 = "P30" P40 = "P40" P50 = "P50" P60 = "P60"
@str_enum RehydratePriority HIGH = "High" STANDARD = "Standard"
@str_enum PublicAccess OFF = "off" BLOB = "blob" CONTAINER = "container"
@str_enum SequenceNumberAction MAX = "max" UPDATE = "update" INCREMENT = "increment"
@str_enum BlockState COMMITTED = "Committed" UNCOMMITTED = "Uncommitted" LATEST = "Latest"
@str_enum BlobImmutabilityPolicyMode MUTABLE = "Mutable" UNLOCKED = "Unlocked" LOCKED = "Locked"

@doc "`BlobType.BLOCKBLOB`, `BlobType.PAGEBLOB`, `BlobType.APPENDBLOB`." BlobType
@doc "Access tiers for block blobs: `StandardBlobTier.HOT`, `COOL`, `COLD`, `ARCHIVE`." StandardBlobTier
@doc "Tiers for page blobs in premium accounts: `PremiumPageBlobTier.P4` … `P60`." PremiumPageBlobTier
@doc "Priority for rehydrating an archived blob: `RehydratePriority.HIGH`, `STANDARD`." RehydratePriority
@doc "Anonymous access level of a container: `PublicAccess.OFF`, `BLOB`, `CONTAINER`." PublicAccess
@doc "How `set_sequence_number` changes a page blob's sequence number: `MAX`, `UPDATE`, `INCREMENT`." SequenceNumberAction
@doc "Block list entries: `BlockState.COMMITTED`, `UNCOMMITTED`, `LATEST`." BlockState
@doc "Immutability policy modes: `MUTABLE`, `UNLOCKED`, `LOCKED`." BlobImmutabilityPolicyMode

"""
    ContentSettings(; content_type=nothing, content_encoding=nothing, content_language=nothing,
                    content_disposition=nothing, cache_control=nothing, content_md5=nothing)

HTTP content headers stored with a blob. `content_md5` is the raw 16-byte digest.
"""
Base.@kwdef mutable struct ContentSettings
    content_type::Union{Nothing, String} = nothing
    content_encoding::Union{Nothing, String} = nothing
    content_language::Union{Nothing, String} = nothing
    content_disposition::Union{Nothing, String} = nothing
    cache_control::Union{Nothing, String} = nothing
    content_md5::Union{Nothing, Vector{UInt8}} = nothing
end

"Lease status (`\"locked\"`/`\"unlocked\"`), state (`\"available\"`, `\"leased\"`, …) and duration (`\"infinite\"`/`\"fixed\"`)."
Base.@kwdef mutable struct LeaseProperties
    status::Union{Nothing, String} = nothing
    state::Union{Nothing, String} = nothing
    duration::Union{Nothing, String} = nothing
end

"Properties of the last copy operation into a blob."
Base.@kwdef mutable struct CopyProperties
    id::Union{Nothing, String} = nothing
    source::Union{Nothing, String} = nothing
    status::Union{Nothing, String} = nothing
    progress::Union{Nothing, String} = nothing
    completion_time::Union{Nothing, DateTime} = nothing
    status_description::Union{Nothing, String} = nothing
    incremental_copy::Union{Nothing, Bool} = nothing
    destination_snapshot::Union{Nothing, String} = nothing
end

"""
    ImmutabilityPolicy(; expiry_time=nothing, policy_mode=nothing)

A blob's time-based retention policy (`policy_mode` is a
[`BlobImmutabilityPolicyMode`](@ref) value).
"""
Base.@kwdef mutable struct ImmutabilityPolicy
    expiry_time::Union{Nothing, DateTime} = nothing
    policy_mode::Any = nothing
end

"""
    BlobProperties

Properties of a blob (Python: `azure.storage.blob.BlobProperties`). Returned
by `get_blob_properties`, `list_blobs` and a downloader's `properties`.
"""
Base.@kwdef mutable struct BlobProperties
    name::String = ""
    container::String = ""
    snapshot::Union{Nothing, String} = nothing
    version_id::Union{Nothing, String} = nothing
    is_current_version::Union{Nothing, Bool} = nothing
    blob_type::Union{Nothing, BlobType.T} = nothing
    metadata::CaseInsensitiveDict{String} = CaseInsensitiveDict{String}()
    last_modified::Union{Nothing, DateTime} = nothing
    etag::Union{Nothing, String} = nothing
    size::Int64 = 0
    content_range::Union{Nothing, String} = nothing
    append_blob_committed_block_count::Union{Nothing, Int} = nothing
    is_append_blob_sealed::Union{Nothing, Bool} = nothing
    page_blob_sequence_number::Union{Nothing, Int64} = nothing
    server_encrypted::Union{Nothing, Bool} = nothing
    copy::CopyProperties = CopyProperties()
    content_settings::ContentSettings = ContentSettings()
    lease::LeaseProperties = LeaseProperties()
    blob_tier::Union{Nothing, String} = nothing
    rehydrate_priority::Union{Nothing, String} = nothing
    blob_tier_change_time::Union{Nothing, DateTime} = nothing
    blob_tier_inferred::Union{Nothing, Bool} = nothing
    deleted::Bool = false
    deleted_time::Union{Nothing, DateTime} = nothing
    remaining_retention_days::Union{Nothing, Int} = nothing
    creation_time::Union{Nothing, DateTime} = nothing
    archive_status::Union{Nothing, String} = nothing
    encryption_key_sha256::Union{Nothing, String} = nothing
    encryption_scope::Union{Nothing, String} = nothing
    request_server_encrypted::Union{Nothing, Bool} = nothing
    last_accessed_on::Union{Nothing, DateTime} = nothing
    tag_count::Union{Nothing, Int} = nothing
    tags::Union{Nothing, Dict{String, String}} = nothing
    immutability_policy::ImmutabilityPolicy = ImmutabilityPolicy()
    has_legal_hold::Union{Nothing, Bool} = nothing
    has_versions_only::Union{Nothing, Bool} = nothing
end
Base.show(io::IO, p::BlobProperties) = print(
    io, "BlobProperties(", repr(p.name), ", size=", p.size,
    p.blob_type === nothing ? "" : ", " * enum_value(p.blob_type), p.snapshot === nothing ? "" : ", snapshot=" * p.snapshot, ")"
)

"""
    ContainerProperties

Properties of a container (Python: `azure.storage.blob.ContainerProperties`).
"""
Base.@kwdef mutable struct ContainerProperties
    name::String = ""
    last_modified::Union{Nothing, DateTime} = nothing
    etag::Union{Nothing, String} = nothing
    lease::LeaseProperties = LeaseProperties()
    public_access::Union{Nothing, PublicAccess.T} = nothing
    has_immutability_policy::Union{Nothing, Bool} = nothing
    has_legal_hold::Union{Nothing, Bool} = nothing
    deleted::Bool = false
    deleted_time::Union{Nothing, DateTime} = nothing
    remaining_retention_days::Union{Nothing, Int} = nothing
    version::Union{Nothing, String} = nothing
    metadata::CaseInsensitiveDict{String} = CaseInsensitiveDict{String}()
    default_encryption_scope::Union{Nothing, String} = nothing
    prevent_encryption_scope_override::Union{Nothing, Bool} = nothing
    immutable_storage_with_versioning_enabled::Union{Nothing, Bool} = nothing
end
Base.show(io::IO, p::ContainerProperties) = print(io, "ContainerProperties(", repr(p.name), p.deleted ? ", deleted" : "", ")")

"""
    BlobBlock(block_id, state=BlockState.LATEST)

A block of a block blob, used with `commit_block_list` and returned by
`get_block_list` (with `size`).
"""
mutable struct BlobBlock
    id::String
    state::BlockState.T
    size::Union{Nothing, Int64}
end
BlobBlock(block_id::AbstractString, state::BlockState.T = BlockState.LATEST) = BlobBlock(String(block_id), state, nothing)
Base.show(io::IO, b::BlobBlock) = print(
    io, "BlobBlock(", repr(b.id), ", ", enum_value(b.state),
    b.size === nothing ? "" : ", size=$(b.size)", ")"
)

"""
    PageRange(start, stop; cleared=false)

An inclusive byte range `start:stop` of a page blob (Python's `PageRange.end`
is `stop`). `cleared` marks ranges cleared since a previous snapshot.
"""
struct PageRange
    start::Int64
    stop::Int64
    cleared::Bool
end
PageRange(start::Integer, stop::Integer; cleared::Bool = false) = PageRange(Int64(start), Int64(stop), cleared)

"""
    AccessPolicy(; permission=nothing, expiry=nothing, start=nothing)

A stored access policy for a container. `permission` is a string such as
`"rl"` or a [`ContainerSasPermissions`](@ref); times are `DateTime`s (UTC).
"""
Base.@kwdef mutable struct AccessPolicy
    permission::Any = nothing
    expiry::Any = nothing
    start::Any = nothing
end

"A container access policy together with its identifier."
struct SignedIdentifier
    id::String
    access_policy::Union{Nothing, AccessPolicy}
end

"""
    CustomerProvidedEncryptionKey(key_value, key_hash)

An AES-256 key supplied with each request (both base64 encoded: the key and
its SHA-256). Requires HTTPS. See also [`CustomerProvidedEncryptionKey(key::Vector{UInt8})`](@ref).
"""
struct CustomerProvidedEncryptionKey
    key_value::String
    key_hash::String
    algorithm::String
end
CustomerProvidedEncryptionKey(key_value::AbstractString, key_hash::AbstractString) =
    CustomerProvidedEncryptionKey(String(key_value), String(key_hash), "AES256")
"Build a customer-provided key from 32 raw key bytes."
function CustomerProvidedEncryptionKey(key::AbstractVector{UInt8})
    length(key) == 32 || throw(ArgumentError("customer-provided keys must be 32 bytes (AES-256)"))
    return CustomerProvidedEncryptionKey(base64encode(key), base64encode(SHA.sha256(Vector{UInt8}(key))))
end
Base.show(io::IO, k::CustomerProvidedEncryptionKey) = print(io, "CustomerProvidedEncryptionKey(key_hash=", k.key_hash, ")")

"""
    UserDelegationKey

A key obtained with [`get_user_delegation_key`](@ref) (Entra ID auth) for
signing user delegation SAS tokens. The `value` is never printed.
"""
Base.@kwdef mutable struct UserDelegationKey
    signed_oid::String = ""
    signed_tid::String = ""
    signed_start::String = ""
    signed_expiry::String = ""
    signed_service::String = ""
    signed_version::String = ""
    signed_delegated_user_tid::Union{Nothing, String} = nothing
    value::String = ""
end
Base.show(io::IO, k::UserDelegationKey) = print(
    io, "UserDelegationKey(signed_oid=", repr(k.signed_oid),
    ", signed_expiry=", repr(k.signed_expiry), ", value=<redacted>)"
)

"A blob returned by `find_blobs_by_tags`: `name`, `container_name` and matching `tags`."
struct FilteredBlob
    name::String
    container_name::String
    tags::Dict{String, String}
end

# --- service properties ----------------------------------------------------------

"Retention policy for analytics logs/metrics or soft delete."
Base.@kwdef mutable struct RetentionPolicy
    enabled::Bool = false
    days::Union{Nothing, Int} = nothing
end

"Azure Analytics logging settings."
Base.@kwdef mutable struct BlobAnalyticsLogging
    version::String = "1.0"
    delete::Bool = false
    read::Bool = false
    write::Bool = false
    retention_policy::RetentionPolicy = RetentionPolicy()
end

"Hour/minute metrics settings."
Base.@kwdef mutable struct Metrics
    version::String = "1.0"
    enabled::Bool = false
    include_apis::Union{Nothing, Bool} = nothing
    retention_policy::RetentionPolicy = RetentionPolicy()
end

"""
    CorsRule(allowed_origins, allowed_methods; allowed_headers=String[], exposed_headers=String[], max_age_in_seconds=0)
"""
mutable struct CorsRule
    allowed_origins::Vector{String}
    allowed_methods::Vector{String}
    allowed_headers::Vector{String}
    exposed_headers::Vector{String}
    max_age_in_seconds::Int
end
CorsRule(allowed_origins, allowed_methods; allowed_headers = String[], exposed_headers = String[], max_age_in_seconds::Integer = 0) =
    CorsRule(
    String.(collect(allowed_origins)), String.(collect(allowed_methods)), String.(collect(allowed_headers)),
    String.(collect(exposed_headers)), Int(max_age_in_seconds)
)

"Static website hosting settings."
Base.@kwdef mutable struct StaticWebsite
    enabled::Bool = false
    index_document::Union{Nothing, String} = nothing
    error_document404_path::Union{Nothing, String} = nothing
    default_index_document_path::Union{Nothing, String} = nothing
end

# --- SAS permissions -------------------------------------------------------------

# (letter, field) in the order the service expects.
const _BLOB_PERMS = (
    ('r', :read), ('a', :add), ('c', :create), ('w', :write), ('d', :delete),
    ('x', :delete_previous_version), ('y', :permanent_delete), ('t', :tag), ('m', :move), ('e', :execute),
    ('i', :set_immutability_policy),
)
const _CONTAINER_PERMS = (
    ('r', :read), ('a', :add), ('c', :create), ('w', :write), ('d', :delete),
    ('x', :delete_previous_version), ('y', :permanent_delete), ('l', :list), ('t', :tag), ('f', :filter_by_tags),
    ('m', :move), ('e', :execute), ('i', :set_immutability_policy),
)
const _ACCOUNT_PERMS = (
    ('r', :read), ('w', :write), ('d', :delete), ('x', :delete_previous_version),
    ('y', :permanent_delete), ('l', :list), ('a', :add), ('c', :create), ('u', :update), ('p', :process),
    ('t', :tag), ('f', :filter_by_tags), ('i', :set_immutability_policy),
)

for (T, table, doc) in (
        (:BlobSasPermissions, :_BLOB_PERMS, "a blob SAS"),
        (:ContainerSasPermissions, :_CONTAINER_PERMS, "a container SAS"),
        (:AccountSasPermissions, :_ACCOUNT_PERMS, "an account SAS"),
    )
    fields = [f for (_, f) in eval(table)]
    @eval begin
        """
            $($(string(T)))(; $($(join(string.(fields), "=false, ")))=false)
            $($(string(T)))(permission_string)

        Permissions for $($doc). `string(p)` gives the permission string
        in the order the service expects.
        """
        struct $T
            $([:($f::Bool) for f in fields]...)
        end
        $T(; $([Expr(:kw, f, false) for f in fields]...)) = $T($(fields...))
        function $T(s::AbstractString)
            letters = Set(collect(s))
            known = Set(first.($table))
            for c in letters
                c in known || throw(ArgumentError("invalid permission character '$(c)'"))
            end
            return $T((l in letters for (l, _) in $table)...)
        end
        Base.string(p::$T) = String([l for (l, f) in $table if getfield(p, f)])
        Base.show(io::IO, p::$T) = print(io, $(string(T)), "(\"", string(p), "\")")
    end
end

"""
    ResourceTypes(; service=false, container=false, object=false)
    ResourceTypes("sco")

Resource types accessible with an account SAS.
"""
struct ResourceTypes
    service::Bool
    container::Bool
    object::Bool
end
ResourceTypes(; service::Bool = false, container::Bool = false, object::Bool = false) = ResourceTypes(service, container, object)
ResourceTypes(s::AbstractString) = (
    all(in("sco"), s) || throw(ArgumentError("invalid resource types $(repr(s))"));
    ResourceTypes('s' in s, 'c' in s, 'o' in s)
)
Base.string(r::ResourceTypes) = string(r.service ? "s" : "", r.container ? "c" : "", r.object ? "o" : "")

"""
    Services(; blob=false, queue=false, table=false, fileshare=false)

Services accessible with an account SAS.
"""
struct Services
    blob::Bool
    queue::Bool
    table::Bool
    fileshare::Bool
end
Services(; blob::Bool = false, queue::Bool = false, table::Bool = false, fileshare::Bool = false) = Services(blob, queue, table, fileshare)
Base.string(s::Services) = string(s.blob ? "b" : "", s.queue ? "q" : "", s.table ? "t" : "", s.fileshare ? "f" : "")

_perm_string(p) = p === nothing ? nothing : p isa AbstractString ? String(p) : string(p)
