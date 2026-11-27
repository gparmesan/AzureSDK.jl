# EventHubConsumerClient: partition pumps, event positions and load balancing.

const SELECTOR_FILTER = Symbol("apache.org:selector-filter:string")
const EPOCH_PROPERTY = Symbol("com.microsoft:epoch")
const RUNTIME_METRIC_CAPABILITY = Symbol("com.microsoft:enable-receiver-runtime-metric")

"""
    EventHubConsumerClient(fully_qualified_namespace, eventhub_name, consumer_group, credential; kwargs...)
    EventHubConsumerClient(f::Function, args...; kwargs...)   # do-block

Receives events from an event hub (`azure.eventhub.EventHubConsumerClient`).

Keyword arguments (in addition to the transport/retry options of
[`EventHubProducerClient`](@ref)):

- `checkpoint_store`: a [`CheckpointStore`](@ref) to persist checkpoints and
  balance partitions across several consumers.
- `load_balancing_interval` (30 s), `partition_ownership_expiration_interval`
  (6 × `load_balancing_interval`), `load_balancing_strategy`
  (`LoadBalancingStrategy.GREEDY` or `BALANCED`, default `GREEDY`).
- `prefetch` (300): events buffered per partition.

Only `TransportType.Amqp` and `TransportType.AmqpOverWebsocket` can receive.
"""
mutable struct EventHubConsumerClient <: EventHubClient
    config::ClientConfig
    eventhub_name::String
    consumer_group::String
    conn::ClientConnection
    checkpoint_store::Union{Nothing, CheckpointStore}
    memory_store::InMemoryCheckpointStore
    load_balancing_interval::Float64
    ownership_expiration::Float64
    strategy::LoadBalancingStrategy.T
    owner_id::String
    prefetch::Int
    lock::ReentrantLock
    cond::Threads.Condition
    receiving::Bool
    closed::Bool
    pumps::Dict{String, Any}
    mgmt::Ref{Any}
    mgmt_lock::ReentrantLock
end

function EventHubConsumerClient(
        fully_qualified_namespace::AbstractString, eventhub_name::AbstractString,
        consumer_group::AbstractString, credential; checkpoint_store::Union{Nothing, CheckpointStore} = nothing,
        load_balancing_interval::Real = 30.0, partition_ownership_expiration_interval::Union{Nothing, Real} = nothing,
        load_balancing_strategy = LoadBalancingStrategy.GREEDY, prefetch::Integer = 300,
        use_development_emulator::Bool = false, kwargs...
    )
    cfg = ClientConfig(
        fully_qualified_namespace, credential, EVENTHUBS_SCOPE, "eventhub";
        use_development_emulator = use_development_emulator, kwargs...
    )
    cfg.transport_type == TransportType.Http &&
        throw(ArgumentError("EventHubConsumerClient requires TransportType.Amqp or TransportType.AmqpOverWebsocket"))
    isempty(consumer_group) && throw(ArgumentError("consumer_group must not be empty"))
    prefetch < 1 && throw(ArgumentError("prefetch must be at least 1"))
    strategy = load_balancing_strategy isa LoadBalancingStrategy.T ? load_balancing_strategy :
        lowercase(string(load_balancing_strategy)) == "balanced" ? LoadBalancingStrategy.BALANCED :
        LoadBalancingStrategy.GREEDY
    lk = ReentrantLock()
    return EventHubConsumerClient(
        cfg, _check_hub_name(eventhub_name), String(consumer_group),
        Messaging.make_connection(cfg), checkpoint_store, InMemoryCheckpointStore(), Float64(load_balancing_interval),
        Float64(something(partition_ownership_expiration_interval, 6 * load_balancing_interval)), strategy,
        string(uuid4()), Int(prefetch), lk, Threads.Condition(lk), false, false, Dict{String, Any}(),
        Ref{Any}(nothing), ReentrantLock()
    )
end

function EventHubConsumerClient(f::Function, args...; kwargs...)
    client = EventHubConsumerClient(args...; kwargs...)
    try
        return f(client)
    finally
        close(client)
    end
end

function from_connection_string(
        ::Type{EventHubConsumerClient}, conn_str::AbstractString; consumer_group::AbstractString,
        eventhub_name = nothing, kwargs...
    )
    p, name, cred = _from_connection_string_args(conn_str, eventhub_name)
    host = p.port === nothing ? p.fully_qualified_namespace : "$(p.fully_qualified_namespace):$(p.port)"
    return EventHubConsumerClient(
        host, name, consumer_group, cred; use_development_emulator = p.use_development_emulator,
        kwargs...
    )
end
function from_connection_string(f::Function, T::Type{EventHubConsumerClient}, conn_str::AbstractString; kwargs...)
    client = from_connection_string(T, conn_str; kwargs...)
    try
        return f(client)
    finally
        close(client)
    end
end

function Base.show(io::IO, c::EventHubConsumerClient)
    return print(
        io, "EventHubConsumerClient(", c.config.fully_qualified_namespace, "/", c.eventhub_name, ", consumer_group=",
        repr(c.consumer_group), c.closed ? ", closed" : "", ")"
    )
end

# ---------------------------------------------------------------------------
# Event positions
# ---------------------------------------------------------------------------

"""
Selector filter expression for a starting position:

- `"-1"`: beginning of the partition, `"@latest"`: only new events,
- other strings: an offset,
- `Integer`: a sequence number,
- `DateTime` (UTC): enqueued time.
"""
function selector_expression(pos, inclusive::Bool)
    op = inclusive ? ">=" : ">"
    if pos isa AbstractString
        s = String(pos)
        if s == "@latest"
            return "amqp.annotation.x-opt-offset > '@latest'"
        end
        # offsets are numeric; refuse anything else (the value is embedded in a filter expression)
        occursin(r"^-?\d+$", s) || throw(ArgumentError("invalid offset $(repr(s)); use \"-1\", \"@latest\" or a numeric offset"))
        return "amqp.annotation.x-opt-offset $op '$s'"
    elseif pos isa Integer
        return "amqp.annotation.x-opt-sequence-number $op $(Int64(pos))"
    elseif pos isa Dates.DateTime
        return "amqp.annotation.x-opt-enqueued-time $op $(AMQP.datetime_to_ms(pos))"
    end
    throw(ArgumentError("invalid starting position $(repr(pos)); use an offset string, a sequence number or a DateTime"))
end

_per_partition(x::AbstractDict, pid, default) = get(x, pid, get(x, tryparse(Int, pid), default))
_per_partition(x, pid, default) = x === nothing ? default : x

function _open_receiver(c::EventHubConsumerClient, pid::String, pos, inclusive::Bool, owner_level, track::Bool)
    conn = c.conn
    AMQP.authorize!(conn, Messaging.resource_uri(c.config, c.eventhub_name))
    _, session, _ = AMQP.acquire!(conn)
    expr = selector_expression(pos, inclusive)
    source = AMQP.Source(
        address = "$(c.eventhub_name)/ConsumerGroups/$(c.consumer_group)/Partitions/$pid",
        filter = Dict{Any, Any}(SELECTOR_FILTER => AMQPDescribed(SELECTOR_FILTER, expr))
    )
    props = owner_level === nothing ? nothing : Dict{Symbol, Any}(EPOCH_PROPERTY => Int64(owner_level))
    caps = track ? [RUNTIME_METRIC_CAPABILITY] : nothing
    return AMQP.attach_receiver(
        session, "EHReceiver-$(uuid4())", source;
        prefetch = c.prefetch, snd_settle_mode = AMQP.SND_SETTLED, rcv_settle_mode = AMQP.RCV_FIRST,
        properties = props, desired_capabilities = caps, timeout = c.config.auth_timeout
    )
end

# ---------------------------------------------------------------------------
# Partition pumps
# ---------------------------------------------------------------------------

mutable struct PartitionPump
    partition_id::String
    stop::Threads.Atomic{Bool}
    link::Any
    task::Union{Nothing, Task}
end

Base.@kwdef struct ReceiveOptions
    on_event::Function
    batch::Bool = false
    max_batch_size::Int = 300
    max_wait_time::Union{Nothing, Float64} = nothing
    on_error::Union{Nothing, Function} = nothing
    on_partition_initialize::Union{Nothing, Function} = nothing
    on_partition_close::Union{Nothing, Function} = nothing
    owner_level::Union{Nothing, Int} = nothing
    track_last_enqueued_event_properties::Bool = false
    starting_position::Any = nothing
    starting_position_inclusive::Any = false
end

function _call_error_handler(opts::ReceiveOptions, ctx, err)
    if opts.on_error === nothing
        @warn "EventHubConsumerClient error" partition = ctx === nothing ? nothing : ctx.partition_id exception = err
        return
    end
    return try
        opts.on_error(ctx, err)
    catch cb_err
        @error "on_error callback raised an exception" exception = (cb_err, catch_backtrace())
    end
end

function _update_last_enqueued!(ctx::PartitionContext, d)
    m = d.raw_amqp_message
    da = m === nothing ? nothing : m.delivery_annotations
    da === nothing && return
    props = Dict{String, Any}()
    v = get(da, PROP_LAST_ENQUEUED_SEQUENCE_NUMBER, nothing); v === nothing || (props["sequence_number"] = v)
    v = get(da, PROP_LAST_ENQUEUED_OFFSET, nothing); v === nothing || (props["offset"] = string(v))
    v = get(da, PROP_LAST_ENQUEUED_TIME_UTC, nothing); v === nothing || (props["enqueued_time"] = v)
    v = get(da, PROP_RUNTIME_INFO_RETRIEVAL_TIME_UTC, nothing); v === nothing || (props["retrieval_time"] = v)
    return isempty(props) || (ctx.last_enqueued_event_properties = props)
end

function _starting_position(c::EventHubConsumerClient, pid::String, opts::ReceiveOptions)
    store = c.checkpoint_store === nothing ? c.memory_store : c.checkpoint_store
    for cp in list_checkpoints(store, c.config.fully_qualified_namespace, c.eventhub_name, c.consumer_group)
        if string(cp["partition_id"]) == pid
            off = get(cp, "offset", nothing)
            off !== nothing && return (string(off), false)
            sn = get(cp, "sequence_number", nothing)
            sn !== nothing && return (Int64(sn), false)
        end
    end
    pos = _per_partition(opts.starting_position, pid, "@latest")
    pos === nothing && (pos = "@latest")
    inclusive = _per_partition(opts.starting_position_inclusive, pid, false)
    return (pos, Bool(something(inclusive, false)))
end

function _run_pump(c::EventHubConsumerClient, pump::PartitionPump, opts::ReceiveOptions)
    pid = pump.partition_id
    ctx = PartitionContext(
        c.config.fully_qualified_namespace, c.eventhub_name, c.consumer_group, pid, c.owner_id,
        nothing, c.checkpoint_store, c.memory_store
    )
    reason = CloseReason.SHUTDOWN
    initialized = false
    attempt = 0
    pos, inclusive = try
        _starting_position(c, pid, opts)
    catch err
        _call_error_handler(opts, ctx, map_eventhub_error(err))
        ("@latest", false)
    end
    try
        if opts.on_partition_initialize !== nothing
            try
                opts.on_partition_initialize(ctx)
            catch err
                _call_error_handler(opts, ctx, err)
            end
        end
        initialized = true
        while !pump.stop[] && !c.closed
            link = nothing
            try
                link = _open_receiver(c, pid, pos, inclusive, opts.owner_level, opts.track_last_enqueued_event_properties)
                pump.link = link
                attempt = 0
                timeout = opts.max_wait_time === nothing ? 0.0 : opts.max_wait_time
                while !pump.stop[] && !c.closed
                    first = AMQP.receive_delivery(link; timeout = timeout)
                    if first === nothing
                        (pump.stop[] || c.closed) && break
                        opts.max_wait_time === nothing && continue
                        _deliver(opts, ctx, EventData[])
                        continue
                    end
                    deliveries = opts.batch ? vcat([first], AMQP.take_queued!(link, opts.max_batch_size - 1)) : [first]
                    events = EventData[EventData(AMQP.decode_delivery(d)) for d in deliveries]
                    last = events[end]
                    if last.offset !== nothing
                        pos, inclusive = last.offset, false
                    elseif last.sequence_number !== nothing
                        pos, inclusive = last.sequence_number, false
                    end
                    opts.track_last_enqueued_event_properties && _update_last_enqueued!(ctx, last)
                    _deliver(opts, ctx, events)
                end
            catch err
                (pump.stop[] || c.closed) && break
                mapped = map_eventhub_error(err)
                if mapped isa OwnershipLostError
                    reason = CloseReason.OWNERSHIP_LOST
                    _call_error_handler(opts, ctx, mapped)
                    break
                end
                _call_error_handler(opts, ctx, mapped)
                attempt += 1
                delay = Messaging.is_retriable(err) ? AzureCore.backoff_delay(c.config.retry, min(attempt, 8)) :
                    c.load_balancing_interval
                _interruptible_sleep(c, pump, delay)
            finally
                pump.link = nothing
                link === nothing || @async try
                    AMQP.detach(link; timeout = 5.0)
                catch end
            end
        end
    finally
        if initialized && opts.on_partition_close !== nothing
            try
                opts.on_partition_close(ctx, reason)
            catch err
                _call_error_handler(opts, ctx, err)
            end
        end
    end
    return reason
end

function _deliver(opts::ReceiveOptions, ctx::PartitionContext, events::Vector{EventData})
    return try
        if opts.batch
            opts.on_event(ctx, events)
        elseif isempty(events)
            opts.on_event(ctx, nothing)
        else
            for e in events
                opts.on_event(ctx, e)
            end
        end
    catch err
        err isa InterruptException && rethrow()
        _call_error_handler(opts, ctx, err)
    end
end

function _interruptible_sleep(c::EventHubConsumerClient, pump, seconds::Real)
    deadline = time() + seconds
    while time() < deadline && !c.closed && (pump === nothing || !pump.stop[])
        sleep(min(0.25, deadline - time()))
    end
    return
end

function _start_pump!(c::EventHubConsumerClient, pid::String, opts::ReceiveOptions)
    pump = PartitionPump(pid, Threads.Atomic{Bool}(false), nothing, nothing)
    pump.task = Threads.@spawn _run_pump(c, pump, opts)
    @lock c.lock c.pumps[pid] = pump
    return pump
end

function _stop_pump!(c::EventHubConsumerClient, pid::String; wait_for::Bool = true)
    pump = @lock c.lock pop!(c.pumps, pid, nothing)
    pump === nothing && return
    pump.stop[] = true
    link = pump.link
    link === nothing || @async try
        AMQP.detach(link; timeout = 5.0)
    catch end
    return if wait_for && pump.task !== nothing
        try
            wait(pump.task)
        catch
        end
    end
end

# ---------------------------------------------------------------------------
# Load balancing
# ---------------------------------------------------------------------------

function _ownership_dict(c::EventHubConsumerClient, pid::String, etag)
    return Dict{String, Any}(
        "fully_qualified_namespace" => c.config.fully_qualified_namespace,
        "eventhub_name" => c.eventhub_name, "consumer_group" => c.consumer_group, "partition_id" => pid,
        "owner_id" => c.owner_id, "etag" => etag
    )
end

"""
One load-balancing cycle: renew our ownerships and claim (or steal) partitions
so every active consumer owns ⌊n/k⌋ or ⌈n/k⌉ of the n partitions. Returns the
set of partitions we own afterwards.
"""
function _balance!(c::EventHubConsumerClient, partition_ids::Vector{String})
    store = c.checkpoint_store::CheckpointStore
    now = time()
    ownerships = list_ownership(store, c.config.fully_qualified_namespace, c.eventhub_name, c.consumer_group)
    by_pid = Dict{String, Dict{String, Any}}(string(o["partition_id"]) => o for o in ownerships)
    is_active(o) = !isempty(string(something(get(o, "owner_id", ""), ""))) &&
        now - Float64(something(get(o, "last_modified_time", 0.0), 0.0)) < c.ownership_expiration
    active = [o for o in ownerships if is_active(o) && string(o["partition_id"]) in partition_ids]
    counts = Dict{String, Int}(c.owner_id => 0)
    for o in active
        counts[string(o["owner_id"])] = get(counts, string(o["owner_id"]), 0) + 1
    end
    per, extra = divrem(length(partition_ids), length(counts))
    mine = [o for o in active if o["owner_id"] == c.owner_id]
    my_count = length(mine)
    others_over = count(((k, v),) -> k != c.owner_id && v > per, counts)
    target = my_count < per ? per : (my_count == per && extra > 0 && others_over < extra ? per + 1 : my_count)
    need = max(0, target - my_count)
    to_claim = Dict{String, Any}[]
    if need > 0
        n = c.strategy == LoadBalancingStrategy.GREEDY ? need : 1
        active_pids = Set(string(o["partition_id"]) for o in active)
        unclaimed = shuffle!([p for p in partition_ids if !(p in active_pids)])
        for pid in unclaimed[1:min(n, length(unclaimed))]
            o = get(by_pid, pid, nothing)
            push!(to_claim, _ownership_dict(c, pid, o === nothing ? nothing : get(o, "etag", nothing)))
        end
        remaining = n - length(to_claim)
        while remaining > 0 && my_count + length(to_claim) < per
            victims = [(k, v) for (k, v) in counts if k != c.owner_id && v > per]
            isempty(victims) && break
            victim = first(sort(victims; by = x -> -x[2]))[1]
            candidates = [
                o for o in active if o["owner_id"] == victim &&
                    !any(t -> t["partition_id"] == o["partition_id"], to_claim)
            ]
            isempty(candidates) && break
            o = rand(candidates)
            push!(to_claim, _ownership_dict(c, string(o["partition_id"]), get(o, "etag", nothing)))
            counts[victim] -= 1
            remaining -= 1
        end
    end
    for o in mine
        push!(to_claim, _ownership_dict(c, string(o["partition_id"]), get(o, "etag", nothing)))
    end
    isempty(to_claim) && return Set{String}()
    claimed = claim_ownership(store, to_claim)
    return Set{String}(string(o["partition_id"]) for o in claimed if o["owner_id"] == c.owner_id)
end

function _relinquish!(c::EventHubConsumerClient, pids)
    store = c.checkpoint_store
    (store === nothing || isempty(pids)) && return
    return try
        current = list_ownership(store, c.config.fully_qualified_namespace, c.eventhub_name, c.consumer_group)
        release = [
            begin
                d = Dict{String, Any}(o)
                d["owner_id"] = ""
                d
            end for o in current if o["owner_id"] == c.owner_id && string(o["partition_id"]) in pids
        ]
        isempty(release) || claim_ownership(store, release)
    catch err
        @debug "failed to relinquish partition ownership" exception = err
    end
end

# ---------------------------------------------------------------------------
# Public receive API
# ---------------------------------------------------------------------------

"""
    receive(consumer, on_event; partition_id=nothing, starting_position=nothing,
            starting_position_inclusive=false, owner_level=nothing, max_wait_time=nothing,
            track_last_enqueued_event_properties=false, on_error=nothing,
            on_partition_initialize=nothing, on_partition_close=nothing)

Receive events and call `on_event(partition_context, event)` for each. Blocks
until the consumer is closed (e.g. with `close(consumer)` from another task or
from inside a callback).

- `partition_id`: receive from one partition only; otherwise from all
  partitions (balanced with other consumers when a checkpoint store is set).
- `starting_position`: where to start when there is no checkpoint:
  `"-1"` (beginning), `"@latest"` (default), an offset string, a sequence
  number, a `DateTime` (UTC), or a `Dict` mapping partition ids to positions.
- `max_wait_time`: if no event arrives within this many seconds,
  `on_event(context, nothing)` is called.
- `owner_level`: exclusive (epoch) receiver priority.
- `on_error(context, error)`, `on_partition_initialize(context)`,
  `on_partition_close(context, reason::CloseReason.T)`.

Callbacks for different partitions may run concurrently on different threads.
"""
function receive(c::EventHubConsumerClient, on_event::Function; partition_id = nothing, kwargs...)
    return _receive(c, ReceiveOptions(; on_event = on_event, batch = false, _receive_kwargs(kwargs)...), partition_id)
end

"""
    receive_batch(consumer, on_event_batch; max_batch_size=300, max_wait_time=nothing, kwargs...)

Like [`receive`](@ref) but calls `on_event_batch(partition_context, events::Vector{EventData})`
with up to `max_batch_size` events at a time. With `max_wait_time`, the callback
is called with an empty vector when no events arrived in that interval.
"""
function receive_batch(
        c::EventHubConsumerClient, on_event_batch::Function; partition_id = nothing,
        max_batch_size::Integer = 300, kwargs...
    )
    max_batch_size < 1 && throw(ArgumentError("max_batch_size must be at least 1"))
    return _receive(
        c, ReceiveOptions(;
            on_event = on_event_batch, batch = true, max_batch_size = Int(max_batch_size),
            _receive_kwargs(kwargs)...
        ), partition_id
    )
end

# do-block forms: receive(consumer; kwargs...) do ctx, event ... end
receive(on_event::Function, c::EventHubConsumerClient; kwargs...) = receive(c, on_event; kwargs...)
receive_batch(on_event_batch::Function, c::EventHubConsumerClient; kwargs...) = receive_batch(c, on_event_batch; kwargs...)

function _receive_kwargs(kwargs)
    allowed = (
        :max_wait_time, :on_error, :on_partition_initialize, :on_partition_close, :owner_level,
        :track_last_enqueued_event_properties, :starting_position, :starting_position_inclusive,
    )
    d = Dict{Symbol, Any}()
    for (k, v) in kwargs
        k in allowed || throw(ArgumentError("unsupported keyword argument $k"))
        v === nothing && continue
        d[k] = k === :max_wait_time ? Float64(v) : k === :owner_level ? Int(v) : v
    end
    return d
end

function _receive(c::EventHubConsumerClient, opts::ReceiveOptions, partition_id)
    @lock c.lock begin
        _check_open(c)
        c.receiving && throw(ArgumentError("receive is already running on this consumer"))
        c.receiving = true
    end
    try
        if partition_id !== nothing || c.checkpoint_store === nothing
            pids = partition_id !== nothing ? [string(partition_id)] : get_partition_ids(c)
            for pid in pids
                _start_pump!(c, pid, opts)
            end
            @lock c.lock while !c.closed
                wait(c.cond)
            end
        else
            pids = String[]
            while !c.closed
                try
                    pids = get_partition_ids(c)
                    owned = _balance!(c, pids)
                    running = @lock c.lock Set(keys(c.pumps))
                    for pid in setdiff(running, owned)
                        _stop_pump!(c, pid; wait_for = false)
                    end
                    for pid in setdiff(owned, running)
                        _start_pump!(c, pid, opts)
                    end
                    # restart pumps that ended on their own (e.g. ownership lost)
                    for (pid, pump) in @lock(c.lock, collect(c.pumps))
                        if pump.task !== nothing && istaskdone(pump.task) && pid in owned
                            @lock c.lock delete!(c.pumps, pid)
                        end
                    end
                catch err
                    c.closed && break
                    _call_error_handler(opts, nothing, map_eventhub_error(err))
                end
                lock(c.lock)
                try
                    c.closed || AMQP.wait_until(() -> c.closed, c.cond, AMQP.deadline_after(c.load_balancing_interval))
                finally
                    unlock(c.lock)
                end
            end
        end
    finally
        owned = @lock c.lock collect(keys(c.pumps))
        for pid in owned
            _stop_pump!(c, pid; wait_for = true)
        end
        _relinquish!(c, owned)
        @lock c.lock c.receiving = false
    end
    return nothing
end

"""
    close(consumer)

Stop receiving (any running [`receive`](@ref) call returns) and close the connection.
"""
function Base.close(c::EventHubConsumerClient)
    @lock c.lock begin
        c.closed && return nothing
        c.closed = true
        notify(c.cond)
    end
    pumps = @lock c.lock collect(values(c.pumps))
    for p in pumps
        p.stop[] = true
        l = p.link
        l === nothing || try
            AMQP.detach(l; timeout = 2.0)
        catch end
    end
    # wait (bounded) for a running receive to wind down before closing the connection,
    # unless close was called from one of the consumer's own callbacks
    in_callback = any(p -> p.task === current_task(), pumps)
    deadline = time() + 10.0
    while !in_callback && (@lock c.lock c.receiving) && time() < deadline
        sleep(0.05)
    end
    close(c.conn)
    return nothing
end
Base.isopen(c::EventHubConsumerClient) = !c.closed
