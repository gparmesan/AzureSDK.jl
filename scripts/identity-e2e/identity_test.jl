# Managed identity / workload identity end-to-end test. Runs inside Azure
# (Container Instances, Container Apps, AKS; see resources.sh) and checks every
# credential in E2E_CREDENTIALS against Event Hubs, Service Bus and Blob Storage.
#
# Environment (set by resources.sh):
#   E2E_TARGET                 label for the results, e.g. "aci"
#   E2E_CREDENTIALS            comma-separated credentials to test:
#       mi-system        ManagedIdentityCredential()  (system-assigned identity)
#       mi-client-id     ManagedIdentityCredential(client_id=E2E_UAMI_CLIENT_ID)
#       mi-object-id     ManagedIdentityCredential(object_id=E2E_UAMI_PRINCIPAL_ID)
#       mi-resource-id   ManagedIdentityCredential(resource_id=E2E_UAMI_RESOURCE_ID)
#       mi-imds-kubelet  ManagedIdentityCredential(client_id=E2E_KUBELET_CLIENT_ID) over IMDS,
#                        with the workload identity variables hidden (AKS node identity)
#       workload         WorkloadIdentityCredential()  (AZURE_* variables of AKS workload identity)
#       default          DefaultAzureCredential()
#   E2E_EVENTHUBS_NAMESPACE, E2E_EVENTHUB, E2E_SERVICEBUS_NAMESPACE, E2E_QUEUE,
#   E2E_STORAGE_URL, E2E_RESULTS_CONTAINER
#   E2E_UAMI_CLIENT_ID, E2E_UAMI_PRINCIPAL_ID, E2E_UAMI_RESOURCE_ID, E2E_KUBELET_CLIENT_ID
#
# Prints a result table, uploads it to <E2E_RESULTS_CONTAINER>/<target>/latest.txt
# and exits with status 1 if any check failed.

using AzureSDK.Identity
using AzureSDK.EventHubs
using AzureSDK.ServiceBus
using AzureSDK.Storage.Blob
using Dates
import UUIDs

const TARGET = get(ENV, "E2E_TARGET", "local")
const EH_NS = ENV["E2E_EVENTHUBS_NAMESPACE"]
const EH = ENV["E2E_EVENTHUB"]
const SB_NS = ENV["E2E_SERVICEBUS_NAMESPACE"]
const QUEUE = ENV["E2E_QUEUE"]
const STORAGE = ENV["E2E_STORAGE_URL"]
const RESULTS = get(ENV, "E2E_RESULTS_CONTAINER", "e2e-results")
const SCOPES = (
    "https://eventhubs.azure.net/.default", "https://servicebus.azure.net/.default",
    "https://storage.azure.com/.default",
)

marker() = "e2e-$TARGET-" * string(UUIDs.uuid4())

function make_credential(name)
    name == "mi-system" && return ManagedIdentityCredential()
    name == "mi-client-id" && return ManagedIdentityCredential(client_id = ENV["E2E_UAMI_CLIENT_ID"])
    name == "mi-object-id" && return ManagedIdentityCredential(object_id = ENV["E2E_UAMI_PRINCIPAL_ID"])
    name == "mi-resource-id" && return ManagedIdentityCredential(resource_id = ENV["E2E_UAMI_RESOURCE_ID"])
    name == "mi-imds-kubelet" && return withenv("AZURE_FEDERATED_TOKEN_FILE" => nothing) do
        ManagedIdentityCredential(client_id = ENV["E2E_KUBELET_CLIENT_ID"])
    end
    name == "workload" && return WorkloadIdentityCredential()
    name == "default" && return DefaultAzureCredential()
    error("unknown credential $(repr(name))")
end

function check_tokens(cred)
    for scope in SCOPES
        tok = get_token(cred, scope)
        (isempty(tok.token) || tok.expires_on < time() + 60) && error("bad token for $scope")
    end
    return
end

"Send one event to partition 0 and read it back."
function check_eventhubs(cred, transport)
    m = marker()
    start = EventHubProducerClient(EH_NS, EH, cred; transport_type = transport) do p
        seq = get_partition_properties(p, "0")["last_enqueued_sequence_number"]
        batch = create_batch(p; partition_id = "0")
        add!(batch, EventData(m; properties = Dict("target" => TARGET)))
        send_batch(p, batch)
        seq
    end
    consumer = EventHubConsumerClient(EH_NS, EH, "\$Default", cred; transport_type = transport)
    found = Ref(false)
    task = Threads.@spawn receive(
        consumer; partition_id = "0", starting_position = start,
        starting_position_inclusive = false, max_wait_time = 5
    ) do ctx, ev
        if ev !== nothing && body_as_str(ev) == m
            found[] = true
            close(consumer)
        end
    end
    timedwait(() -> istaskdone(task), 60.0)
    istaskdone(task) || close(consumer)
    istaskfailed(task) && throw(task.exception)
    return found[] || error("event not received within 60 s")
end

"Send one message and receive it in receive-and-delete mode."
function check_servicebus(cred, transport)
    m = marker()
    return ServiceBusClient(SB_NS, cred; transport_type = transport) do client
        send_messages(get_queue_sender(client, QUEUE), ServiceBusMessage(m; application_properties = Dict("target" => TARGET)))
        r = get_queue_receiver(client, QUEUE; receive_mode = ServiceBusReceiveMode.RECEIVE_AND_DELETE, max_wait_time = 10)
        deadline = time() + 60
        while time() < deadline
            any(msg -> body_as_str(msg) == m, receive_messages(r; max_message_count = 10)) && return
        end
        error("message not received within 60 s")
    end
end

function check_blob(cred)
    cc = get_container_client(BlobServiceClient(STORAGE; credential = cred), RESULTS)
    name, m = "$TARGET/probe/$(UUIDs.uuid4()).txt", marker()
    upload_blob(cc, name, m)
    return try
        content_as_text(download_blob(cc, name)) == m || error("downloaded content differs")
    finally
        delete_blob(get_blob_client(cc, name))
    end
end

const CHECKS = [
    "tokens" => check_tokens,
    "eventhubs amqp" => c -> check_eventhubs(c, TransportType.Amqp),
    "eventhubs websocket" => c -> check_eventhubs(c, TransportType.AmqpOverWebsocket),
    "servicebus amqp" => c -> check_servicebus(c, TransportType.Amqp),
    "servicebus websocket" => c -> check_servicebus(c, TransportType.AmqpOverWebsocket),
    "servicebus http" => c -> check_servicebus(c, TransportType.Http),
    "blob" => check_blob,
]

short_error(err) = first(replace(sprint(showerror, err), r"\s+" => " "), 300)

function main()
    names = filter(!isempty, strip.(split(get(ENV, "E2E_CREDENTIALS", "default"), ',')))
    rows = Tuple{String, String, String, String}[]      # credential, check, status, detail
    passed_blob = nothing
    println("target $TARGET, credentials: ", join(names, ", "))
    for name in names
        cred = try
            make_credential(name)
        catch err
            push!(rows, (name, "construct", "FAIL", short_error(err)))
            continue
        end
        println("── $name: $(repr(cred))")
        for (check, f) in CHECKS
            t = time()
            row = try
                f(cred)
                (name, check, "PASS", "$(round(time() - t; digits = 1)) s")
            catch err
                (name, check, "FAIL", short_error(err))
            end
            println("   ", rpad(check, 22), row[3], "  ", row[4])
            push!(rows, row)
            check == "blob" && row[3] == "PASS" && passed_blob === nothing && (passed_blob = cred)
        end
    end
    failed = count(r -> r[3] == "FAIL", rows)
    report = IOBuffer()
    println(
        report, "target: $TARGET\nfinished: $(now(UTC)) UTC\nresult: ",
        failed == 0 ? "all $(length(rows)) checks passed" : "$failed of $(length(rows)) checks FAILED", "\n"
    )
    for (n, c, s, d) in rows
        println(report, rpad(n, 16), rpad(c, 22), rpad(s, 6), d)
    end
    text = String(take!(report))
    println("\n", text)
    if passed_blob === nothing
        println("no credential could write to blob storage; results are only in this log")
    else
        cc = get_container_client(BlobServiceClient(STORAGE; credential = passed_blob), RESULTS)
        upload_blob(cc, "$TARGET/$(Dates.format(now(UTC), "yyyymmdd-HHMMSS")).txt", text)
        upload_blob(cc, "$TARGET/latest.txt", text; overwrite = true)
    end
    return exit(failed == 0 ? 0 : 1)
end

main()
