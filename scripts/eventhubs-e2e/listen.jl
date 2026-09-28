# Print the events arriving at the event hub made by resources.sh.
#
#   julia --project scripts/eventhubs-e2e/listen.jl [options]
#
# Runs until Ctrl-C (or --exit-after). By default only events sent after the
# listener has started are shown.
#
# Options:
#   -b, --from-beginning         also show the events already in the hub
#   -P, --partition-id ID        read one partition only
#   -g, --consumer-group NAME    (default $Default; Basic namespaces have no other)
#   -x, --exit-after N           exit after N events (e.g. for scripted checks)
#   -t, --idle-timeout SECONDS   exit when no event arrived for this long
#       --auth key|entra         connection string (default) or DefaultAzureCredential
#       --transport amqp|websocket   (default amqp)
#   -h, --help

include(joinpath(@__DIR__, "common.jl"))
using Dates

const SPEC = Dict(
    "-b" => (:from_beginning, :flag), "--from-beginning" => (:from_beginning, :flag),
    "-P" => (:partition_id, :value), "--partition-id" => (:partition_id, :value),
    "-g" => (:consumer_group, :value), "--consumer-group" => (:consumer_group, :value),
    "-x" => (:exit_after, :value), "--exit-after" => (:exit_after, :value),
    "-t" => (:idle_timeout, :value), "--idle-timeout" => (:idle_timeout, :value),
    "--auth" => (:auth, :value), "--transport" => (:transport, :value),
    "-h" => (:help, :flag), "--help" => (:help, :flag))

"Milliseconds from the event's \"sent_at\" property (set by send.jl) until now, or `nothing`."
function latency_ms(event)
    sent = get(event.properties, "sent_at", nothing)
    sent isa AbstractString || return nothing
    t = tryparse(DateTime, rstrip(sent, 'Z'))
    return t === nothing ? nothing : Dates.value(now(UTC) - t)
end

function show_event(ctx, event)
    println("── partition ", ctx.partition_id, "  seq ", event.sequence_number, "  offset ", event.offset,
        "  enqueued ", event.enqueued_time, " UTC")
    event.partition_key === nothing || println("   partition key: ", event.partition_key)
    event.content_type === nothing || println("   content type:  ", event.content_type)
    props = filter(p -> p.first != "sent_at", event.properties)
    isempty(props) || println("   properties:    ",
        join(("$k=$(repr(props[k]))" for k in sort!(collect(keys(props)))), ", "))
    ms = latency_ms(event)
    ms === nothing || println("   latency:       ", ms, " ms (sent_at → received)")
    println("   body:          ", body_as_str(event))
end

function main(args)
    opts, rest = parse_args(args, SPEC)
    (get(opts, :help, false) || !isempty(rest)) && usage(@__FILE__; status=isempty(rest) ? 0 : 1)
    exit_after = haskey(opts, :exit_after) ? parse(Int, opts[:exit_after]) : nothing
    idle = haskey(opts, :idle_timeout) ? parse(Float64, opts[:idle_timeout]) : nothing
    transport = parse_transport(get(opts, :transport, "amqp"))
    transport == TransportType.Http && error("receiving needs AMQP; use --transport amqp or websocket")

    consumer = make_client(EventHubConsumerClient, get(opts, :auth, "key"), transport;
        consumer_group=get(opts, :consumer_group, "\$Default"))
    received = Threads.Atomic{Int}(0)
    last_event = Threads.Atomic{Float64}(time())
    out = ReentrantLock()    # callbacks for different partitions may run concurrently

    Base.exit_on_sigint(false)
    try
        info = get_eventhub_properties(consumer)
        println("listening on ", info["eventhub_name"], " (partitions ",
            something(get(opts, :partition_id, nothing), join(info["partition_ids"], ", ")), "); Ctrl-C quits")
        receive(consumer;
            partition_id=get(opts, :partition_id, nothing),
            starting_position=get(opts, :from_beginning, false) ? "-1" : "@latest",
            max_wait_time=idle,
            on_partition_initialize=ctx -> lock(() -> println("   (reading partition ", ctx.partition_id, ")"), out),
            on_error=(ctx, err) -> @warn("receive error", partition = ctx === nothing ? nothing : ctx.partition_id, err)
        ) do ctx, event
            if event === nothing    # max_wait_time passed without an event on this partition
                if time() - last_event[] >= idle
                    lock(() -> println("no event for $(idle) s; stopping"), out)
                    close(consumer)
                end
                return
            end
            last_event[] = time()
            n = Threads.atomic_add!(received, 1) + 1
            exit_after !== nothing && n > exit_after && return
            lock(() -> show_event(ctx, event), out)
            exit_after !== nothing && n >= exit_after && close(consumer)
        end
    catch err
        err isa InterruptException || rethrow()
    finally
        close(consumer)
    end
    println("received ", min(received[], something(exit_after, typemax(Int))), " events")
end

try
    main(ARGS)
catch err
    err isa AuthenticationError && "entra" in ARGS && occursin("claim", sprint(showerror, err)) || rethrow()
    println(stderr, "error: ", sprint(showerror, err), "\n\nWith --auth entra your identity needs the ",
        "\"Azure Event Hubs Data Owner\" (or Data Receiver) role on the namespace: run ",
        "scripts/eventhubs-e2e/resources.sh grant and wait a few minutes.")
    exit(1)
end
