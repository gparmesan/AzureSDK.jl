# Settings and client construction shared by send.jl and listen.jl.
#
# Settings come from the environment, falling back to the .env file that
# resources.sh writes next to this file:
#   EVENTHUB_CONNECTION_STRING   used with --auth key (the default)
#   EVENTHUB_NAMESPACE           e.g. myns.servicebus.windows.net, used with --auth entra
#   EVENTHUB_NAME                the event hub

using AzureSDK.EventHubs
using AzureSDK.Identity

const ENV_FILE = joinpath(@__DIR__, ".env")

"Settings from `ENV`, with the values in `.env` as fallback."
function load_settings()
    s = Dict{String,String}()
    if isfile(ENV_FILE)
        for line in eachline(ENV_FILE)
            m = match(r"^\s*([A-Za-z_][A-Za-z0-9_]*)='?([^']*)'?\s*$", line)
            m === nothing || (s[m[1]] = m[2])
        end
    end
    for k in ("EVENTHUB_CONNECTION_STRING", "EVENTHUB_NAMESPACE", "EVENTHUB_NAME")
        v = get(ENV, k, "")
        isempty(v) || (s[k] = v)
    end
    return s
end

function setting(s, key)
    v = get(s, key, "")
    isempty(v) && error("$key is not set; run scripts/eventhubs-e2e/resources.sh create or set it in the environment")
    return v
end

function parse_transport(name)
    name == "amqp" && return TransportType.Amqp
    name == "websocket" && return TransportType.AmqpOverWebsocket
    name == "http" && return TransportType.Http
    error("unknown transport $(repr(name)); use amqp, websocket or http")
end

"""
    make_client(T, auth, transport; kwargs...)

An `EventHubProducerClient` or `EventHubConsumerClient` authenticated with the
connection string (`auth == "key"`) or with `DefaultAzureCredential` (`"entra"`).
"""
function make_client(T::Type, auth::AbstractString, transport; consumer_group="\$Default")
    s = load_settings()
    hub = setting(s, "EVENTHUB_NAME")
    extra = T === EventHubConsumerClient ? (; consumer_group) : (;)
    if auth == "key"
        return from_connection_string(T, setting(s, "EVENTHUB_CONNECTION_STRING"); eventhub_name=hub,
            transport_type=transport, extra...)
    elseif auth == "entra"
        ns, cred = setting(s, "EVENTHUB_NAMESPACE"), DefaultAzureCredential()
        return T === EventHubConsumerClient ?
               T(ns, hub, consumer_group, cred; transport_type=transport) :
               T(ns, hub, cred; transport_type=transport)
    end
    error("unknown auth $(repr(auth)); use key or entra")
end

"""
    parse_args(args, spec) -> (options, positional)

Minimal command line parsing. `spec` maps option names (`"--name"` or `"-n"`)
to `(key, kind)` with `kind` one of `:flag`, `:value` or `:multi` (repeatable).
"""
function parse_args(args, spec)
    opts = Dict{Symbol,Any}()
    rest = String[]
    i = 1
    while i <= length(args)
        a = args[i]
        if a == "--"
            append!(rest, args[i+1:end])
            break
        elseif startswith(a, "-") && length(a) > 1
            name, inline = occursin('=', a) && startswith(a, "--") ? split(a, '='; limit=2) : (a, nothing)
            haskey(spec, name) || error("unknown option $name (see --help)")
            key, kind = spec[name]
            if kind === :flag
                opts[key] = true
            else
                if inline === nothing
                    i += 1
                    i <= length(args) || error("option $name needs a value")
                    inline = args[i]
                end
                kind === :multi ? push!(get!(opts, key, String[]), String(inline)) : (opts[key] = String(inline))
            end
        else
            push!(rest, a)
        end
        i += 1
    end
    return opts, rest
end

"Print the leading comment block of `file` (the usage text) and exit."
function usage(file; status=0)
    for line in eachline(file)
        startswith(line, "#") || break
        println(replace(line, r"^# ?" => ""))
    end
    exit(status)
end
