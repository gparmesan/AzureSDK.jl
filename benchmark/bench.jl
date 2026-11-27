# Stress/throughput benchmark for AzureSDK.jl. The same workloads are in bench.py
# (Azure SDK for Python); benchmark/run.py runs both and compares them.
#
#   julia -t 8 --project benchmark/bench.jl [eventhubs|servicebus|blob|all] [--scale=1.0]
#
# Needs the local emulators from test/emulator (Event Hubs + Azurite, Service Bus).
# Prints one JSON object per metric on stdout.
using AzureSDK, AzureSDK.EventHubs, AzureSDK.ServiceBus, AzureSDK.Storage.Blob
using JSON, Random, Statistics, Printf

const EH_CS = get(ENV, "AZURESDK_EVENTHUBS_CONNECTION_STRING",
    "Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;")
const SB_CS = get(ENV, "AZURESDK_SERVICEBUS_CONNECTION_STRING",
    "Endpoint=sb://localhost:5673;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;")
const BLOB_CS = get(ENV, "AZURESDK_AZURITE_CONNECTION_STRING", "UseDevelopmentStorage=true")

scale = 1.0
for a in ARGS
    startswith(a, "--scale=") && (global scale = parse(Float64, a[9:end]))
end
n(x) = max(1, round(Int, x * scale))
const PAYLOAD = rand(MersenneTwister(1), UInt8, 256)

emit(scenario, metric, value, unit) =
    (println(JSON.json(Dict("sdk" => "julia", "scenario" => scenario, "metric" => metric, "value" => value, "unit" => unit))); flush(stdout))
function emit_lat(scenario, lat)
    emit(scenario, "p50", 1000 * quantile(lat, 0.5), "ms")
    emit(scenario, "p99", 1000 * quantile(lat, 0.99), "ms")
end

function bench_eventhubs(; warm=false)
    N = warm ? 200 : n(3_000)
    producer = from_connection_string(EventHubProducerClient, EH_CS; eventhub_name="eh1")
    try
        start_seq = get_partition_properties(producer, "0")["last_enqueued_sequence_number"]
        # batched send: fill batches until full
        t = @elapsed begin
            batch = create_batch(producer; partition_id="0")
            for _ in 1:N
                ev = EventData(PAYLOAD)
                if !try_add!(batch, ev)
                    send_batch(producer, batch)
                    batch = create_batch(producer; partition_id="0")
                    add!(batch, ev)
                end
            end
            length(batch) > 0 && send_batch(producer, batch)
        end
        warm || emit("eh_send_batched", "throughput", N / t, "events/s")
        # single-event send latency
        lat = [@elapsed(send_event(producer, EventData(PAYLOAD); partition_id="1")) for _ in 1:(warm ? 5 : n(300))]
        warm || emit_lat("eh_send_single", lat)
        # receive the batched events back
        consumer = from_connection_string(EventHubConsumerClient, EH_CS; eventhub_name="eh1", consumer_group="\$default", prefetch=300)
        got = Threads.Atomic{Int}(0)
        t0 = time()
        task = Threads.@spawn receive_batch(consumer, (ctx, evs) -> begin
                Threads.atomic_add!(got, length(evs))
                got[] >= N && close(consumer)
            end; partition_id="0", starting_position=start_seq, starting_position_inclusive=false,
            max_batch_size=300, max_wait_time=5)
        timedwait(() -> istaskdone(task), 300.0)
        t = time() - t0
        istaskdone(task) || close(consumer)
        got[] >= N || error("received only $(got[]) of $N events")
        warm || emit("eh_receive", "throughput", N / t, "events/s")
    finally
        close(producer)
    end
end

# The emulator delivers in bursts and can stall for longer than max_wait_time, so an
# empty receive does not mean the queue is empty: check with peek_messages.
function drain_queue(client, q)
    r = get_queue_receiver(client, q; receive_mode=ServiceBusReceiveMode.RECEIVE_AND_DELETE, max_wait_time=2)
    try
        while true
            isempty(receive_messages(r; max_message_count=100)) && isempty(peek_messages(r; max_message_count=1)) && break
        end
    finally
        close(r)
    end
end

function bench_servicebus(; warm=false)
    N = warm ? 100 : n(5_000)
    client = from_connection_string(ServiceBusClient, SB_CS)
    try
        drain_queue(client, "queue.1")
        sender = get_queue_sender(client, "queue.1")
        t = @elapsed for i in 1:100:N
            send_messages(sender, [ServiceBusMessage(PAYLOAD) for _ in i:min(i + 99, N)])
        end
        warm || emit("sb_send_batched", "throughput", N / t, "msgs/s")
        # peek-lock receive + complete
        r = get_queue_receiver(client, "queue.1"; max_wait_time=5)
        got = 0
        t0 = time()
        t = @elapsed while got < N
            msgs = receive_messages(r; max_message_count=100, max_wait_time=5)
            isempty(msgs) ? (time() - t0 > 120 && error("no messages for 120 s after $got of $N")) : (t0 = time())
            foreach(m -> complete_message(r, m), msgs)
            got += length(msgs)
        end
        close(r)
        warm || emit("sb_receive_complete", "throughput", N / t, "msgs/s")
        # receive-and-delete (no settlement round trips)
        for i in 1:100:N
            send_messages(sender, [ServiceBusMessage(PAYLOAD) for _ in i:min(i + 99, N)])
        end
        r = get_queue_receiver(client, "queue.1"; receive_mode=ServiceBusReceiveMode.RECEIVE_AND_DELETE, max_wait_time=5)
        got = 0
        t0 = time()
        t = @elapsed while got < N
            msgs = receive_messages(r; max_message_count=100, max_wait_time=5)
            isempty(msgs) ? (time() - t0 > 120 && error("no messages for 120 s after $got of $N")) : (t0 = time())
            got += length(msgs)
        end
        close(r)
        warm || emit("sb_receive_and_delete", "throughput", N / t, "msgs/s")
        lat = [@elapsed(send_messages(sender, ServiceBusMessage(PAYLOAD))) for _ in 1:(warm ? 5 : n(300))]
        warm || emit_lat("sb_send_single", lat)
        close(sender)
        drain_queue(client, "queue.1")
    finally
        close(client)
    end
end

function bench_blob(; warm=false)
    svc = from_connection_string(BlobServiceClient, BLOB_CS; max_single_put_size=8 * 1024^2)
    cname = "bench-jl-" * randstring('a':'z', 8)
    container = create_container(svc, cname)
    try
        big = rand(UInt8, warm ? 16 * 1024^2 : n(64) * 1024^2)
        mb = length(big) / 1024^2
        t = @elapsed upload_blob(container, "big.bin", big; overwrite=true, max_concurrency=8)
        warm || emit("blob_upload_64MiB_c8", "throughput", mb / t, "MiB/s")
        local back
        t = @elapsed(back = readall(download_blob(container, "big.bin"; max_concurrency=8)))
        back == big || error("download mismatch")
        warm || emit("blob_download_64MiB_c8", "throughput", mb / t, "MiB/s")

        small = rand(UInt8, 4096)
        lat = [@elapsed(upload_blob(container, "seq/$i", small; overwrite=true)) for i in 1:(warm ? 10 : n(300))]
        warm || emit_lat("blob_put_4KiB_seq", lat)
        warm || emit("blob_put_4KiB_seq", "throughput", length(lat) / sum(lat), "ops/s")

        M = warm ? 64 : n(2_000)
        t = @elapsed asyncmap(i -> upload_blob(container, "par/$i", small; overwrite=true), 1:M; ntasks=16)
        warm || emit("blob_put_4KiB_c16", "throughput", M / t, "ops/s")
        t = @elapsed asyncmap(i -> readall(download_blob(container, "par/$i")), 1:M; ntasks=16)
        warm || emit("blob_get_4KiB_c16", "throughput", M / t, "ops/s")
        local cnt
        t = @elapsed(cnt = count(_ -> true, list_blobs(container; name_starts_with="par/")))
        cnt == M || error("listed $cnt of $M blobs")
        warm || emit("blob_list_2000", "throughput", M / t, "blobs/s")
    finally
        delete_container(svc, cname)
    end
end

which = get(filter(a -> !startswith(a, "--"), ARGS), 1, "all")
benches = Dict("eventhubs" => bench_eventhubs, "servicebus" => bench_servicebus, "blob" => bench_blob)
for (name, f) in (which == "all" ? sort(collect(benches); by=first) : [which => benches[which]])
    f(; warm=true)          # compile the code paths first; cold start is measured separately
    GC.gc()
    f()
end
emit("process", "peak_rss", Sys.maxrss() / 1024^2, "MiB")
