# Send custom events to the event hub made by resources.sh.
#
#   julia --project scripts/eventhubs-e2e/send.jl [options] [message ...]
#
# Each message argument is sent as one event. Without message arguments the
# script is interactive: every line you type is sent as an event (Ctrl-D quits).
# Messages starting with "@" are read from a file ("@data.json").
#
# Options:
#   -k, --partition-key KEY      route all events by this partition key
#   -P, --partition-id ID        send to this partition
#   -p, --property NAME=VALUE    application property (repeatable)
#   -n, --count N                send every message N times (default 1)
#   -c, --content-type TYPE      e.g. application/json
#       --auth key|entra         connection string (default) or DefaultAzureCredential
#       --transport amqp|websocket|http   (default amqp)
#   -h, --help
#
# Every event also carries the properties "sent_at" (UTC time) and "n" (a
# running number), which listen.jl uses to show the end-to-end latency.

include(joinpath(@__DIR__, "common.jl"))
using Dates

const SPEC = Dict(
    "-k" => (:partition_key, :value), "--partition-key" => (:partition_key, :value),
    "-P" => (:partition_id, :value), "--partition-id" => (:partition_id, :value),
    "-p" => (:property, :multi), "--property" => (:property, :multi),
    "-n" => (:count, :value), "--count" => (:count, :value),
    "-c" => (:content_type, :value), "--content-type" => (:content_type, :value),
    "--auth" => (:auth, :value), "--transport" => (:transport, :value),
    "-h" => (:help, :flag), "--help" => (:help, :flag)
)

"Parse `NAME=VALUE`; integers, floats and booleans keep their type."
function parse_property(p)
    occursin('=', p) || error("property $(repr(p)) is not NAME=VALUE")
    name, value = split(p, '='; limit = 2)
    v = something(tryparse(Int64, value), tryparse(Float64, value), tryparse(Bool, value), Some(String(value)))
    return String(name) => v
end

message_body(m) = startswith(m, "@") ? read(m[2:end]) : m

function main(args)
    opts, messages = parse_args(args, SPEC)
    get(opts, :help, false) && usage(@__FILE__)
    count = parse(Int, get(opts, :count, "1"))
    props = Dict{String, Any}(parse_property(p) for p in get(opts, :property, String[]))
    key, pid = get(opts, :partition_key, nothing), get(opts, :partition_id, nothing)
    transport = parse_transport(get(opts, :transport, "amqp"))

    producer = make_client(EventHubProducerClient, get(opts, :auth, "key"), transport)
    sent = Ref(0)
    function send(message)
        batch = create_batch(producer; partition_key = key, partition_id = pid)
        body = message_body(message)
        for _ in 1:count
            sent[] += 1
            p = merge(props, Dict{String, Any}("n" => sent[], "sent_at" => string(now(UTC)) * "Z"))
            add!(batch, EventData(body; properties = p, content_type = get(opts, :content_type, nothing)))
        end
        send_batch(producer, batch)
        return println(
            "sent ", count == 1 ? "event #$(sent[])" : "events #$(sent[] - count + 1)-#$(sent[])",
            " (", size_in_bytes(batch), " bytes)"
        )
    end

    return try
        info = get_eventhub_properties(producer)
        println("connected to ", info["eventhub_name"], " (partitions ", join(info["partition_ids"], ", "), ")")
        if isempty(messages)
            println("type a message and press Enter to send it; Ctrl-D quits")
            while true
                print("> ")
                line = readline(stdin; keep = true)
                isempty(line) && break          # EOF
                line = chomp(line)
                isempty(line) || send(line)
            end
            println()
        else
            foreach(send, messages)
        end
    finally
        close(producer)
    end
end

try
    main(ARGS)
catch err
    err isa AuthenticationError && "entra" in ARGS && occursin("claim", sprint(showerror, err)) || rethrow()
    println(
        stderr, "error: ", sprint(showerror, err), "\n\nWith --auth entra your identity needs the ",
        "\"Azure Event Hubs Data Owner\" (or Data Sender) role on the namespace: run ",
        "scripts/eventhubs-e2e/resources.sh grant and wait a few minutes."
    )
    exit(1)
end
