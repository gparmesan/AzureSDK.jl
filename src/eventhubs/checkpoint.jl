# Checkpoint stores (azure.eventhub.CheckpointStore) and partition context.

"""
    CheckpointStore

Abstract type for stores that persist partition ownership and checkpoints so
several consumers can share the work and resume where they left off. Subtypes
implement (all dictionaries use the Python SDK's key names):

- `list_ownership(store, fully_qualified_namespace, eventhub_name, consumer_group) -> Vector{Dict{String,Any}}`
- `claim_ownership(store, ownerships::Vector{Dict{String,Any}}) -> Vector{Dict{String,Any}}`
- `update_checkpoint(store, checkpoint::Dict{String,Any})`
- `list_checkpoints(store, fully_qualified_namespace, eventhub_name, consumer_group) -> Vector{Dict{String,Any}}`

Ownership keys: `fully_qualified_namespace`, `eventhub_name`, `consumer_group`,
`partition_id`, `owner_id`, `last_modified_time` (Unix seconds), `etag`.
Checkpoint keys: `fully_qualified_namespace`, `eventhub_name`,
`consumer_group`, `partition_id`, `offset`, `sequence_number`.
"""
abstract type CheckpointStore end

function list_ownership end
function claim_ownership end
function update_checkpoint end
function list_checkpoints end

"""
    InMemoryCheckpointStore()

Thread-safe checkpoint store kept in process memory. Useful for tests and for
balancing several consumers inside one process.
"""
mutable struct InMemoryCheckpointStore <: CheckpointStore
    lock::ReentrantLock
    ownership::Dict{NTuple{4,String},Dict{String,Any}}
    checkpoints::Dict{NTuple{4,String},Dict{String,Any}}
end
InMemoryCheckpointStore() = InMemoryCheckpointStore(ReentrantLock(), Dict{NTuple{4,String},Dict{String,Any}}(),
    Dict{NTuple{4,String},Dict{String,Any}}())

_key(d) = (lowercase(string(d["fully_qualified_namespace"])), lowercase(string(d["eventhub_name"])),
    lowercase(string(d["consumer_group"])), string(d["partition_id"]))
_matches(k, fqns, eh, cg) = k[1] == lowercase(fqns) && k[2] == lowercase(eh) && k[3] == lowercase(cg)

function list_ownership(s::InMemoryCheckpointStore, fully_qualified_namespace, eventhub_name, consumer_group)
    @lock s.lock [copy(v) for (k, v) in s.ownership if _matches(k, fully_qualified_namespace, eventhub_name, consumer_group)]
end

function claim_ownership(s::InMemoryCheckpointStore, ownerships::AbstractVector)
    claimed = Dict{String,Any}[]
    @lock s.lock for o in ownerships
        k = _key(o)
        current = get(s.ownership, k, nothing)
        etag = get(o, "etag", nothing)
        if current === nothing ? etag === nothing : current["etag"] == etag
            new = Dict{String,Any}(o)
            new["etag"] = string(uuid4())
            new["last_modified_time"] = time()
            s.ownership[k] = new
            push!(claimed, copy(new))
        end
    end
    return claimed
end

function update_checkpoint(s::InMemoryCheckpointStore, checkpoint::AbstractDict)
    @lock s.lock s.checkpoints[_key(checkpoint)] = Dict{String,Any}(checkpoint)
    return nothing
end

function list_checkpoints(s::InMemoryCheckpointStore, fully_qualified_namespace, eventhub_name, consumer_group)
    @lock s.lock [copy(v) for (k, v) in s.checkpoints if _matches(k, fully_qualified_namespace, eventhub_name, consumer_group)]
end

Base.show(io::IO, s::InMemoryCheckpointStore) =
    print(io, "InMemoryCheckpointStore(", length(s.ownership), " ownerships, ", length(s.checkpoints), " checkpoints)")

"""
    PartitionContext

Passed to the consumer callbacks. Fields: `fully_qualified_namespace`,
`eventhub_name`, `consumer_group`, `partition_id`, `owner_id`,
`last_enqueued_event_properties` (populated when
`track_last_enqueued_event_properties=true`).

Call [`update_checkpoint`](@ref)`(context, event)` to record progress.
"""
mutable struct PartitionContext
    fully_qualified_namespace::String
    eventhub_name::String
    consumer_group::String
    partition_id::String
    owner_id::String
    last_enqueued_event_properties::Union{Nothing,Dict{String,Any}}
    checkpoint_store::Union{Nothing,CheckpointStore}
    memory_store::InMemoryCheckpointStore
end

Base.show(io::IO, c::PartitionContext) = print(io, "PartitionContext(", c.eventhub_name, "/", c.consumer_group,
    ", partition_id=", repr(c.partition_id), ")")

"""
    update_checkpoint(context::PartitionContext, event=nothing)

Record that processing of `event` (and all earlier events of the partition)
is complete. Without a checkpoint store the checkpoint is kept in memory.
"""
function update_checkpoint(ctx::PartitionContext, event::Union{Nothing,EventData}=nothing)
    event === nothing && return nothing
    cp = Dict{String,Any}(
        "fully_qualified_namespace" => ctx.fully_qualified_namespace,
        "eventhub_name" => ctx.eventhub_name,
        "consumer_group" => ctx.consumer_group,
        "partition_id" => ctx.partition_id,
        "offset" => event.offset,
        "sequence_number" => event.sequence_number,
    )
    store = ctx.checkpoint_store === nothing ? ctx.memory_store : ctx.checkpoint_store
    update_checkpoint(store, cp)
    return nothing
end
