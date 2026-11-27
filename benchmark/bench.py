"""Same workloads as bench.jl, using the Azure SDK for Python (sync APIs).

    python benchmark/bench.py [eventhubs|servicebus|blob|all] [--scale=1.0]
"""
import json, os, random, resource, statistics, string, sys, threading, time
from concurrent.futures import ThreadPoolExecutor

from azure.eventhub import EventHubProducerClient, EventHubConsumerClient, EventData
from azure.servicebus import ServiceBusClient, ServiceBusMessage, ServiceBusReceiveMode
from azure.storage.blob import BlobServiceClient

EH_CS = os.environ.get("AZURESDK_EVENTHUBS_CONNECTION_STRING",
    "Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;")
SB_CS = os.environ.get("AZURESDK_SERVICEBUS_CONNECTION_STRING",
    "Endpoint=sb://localhost:5673;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;")
BLOB_CS = os.environ.get("AZURESDK_AZURITE_CONNECTION_STRING", "UseDevelopmentStorage=true")

scale = 1.0
for a in sys.argv[1:]:
    if a.startswith("--scale="):
        scale = float(a[8:])
n = lambda x: max(1, round(x * scale))
PAYLOAD = random.Random(1).randbytes(256)


def emit(scenario, metric, value, unit):
    print(json.dumps({"sdk": "python", "scenario": scenario, "metric": metric, "value": value, "unit": unit}), flush=True)


def quantile(xs, q):  # same definition as Julia's Statistics.quantile (type 7)
    xs = sorted(xs)
    h = (len(xs) - 1) * q
    lo = int(h)
    return xs[lo] + (h - lo) * (xs[min(lo + 1, len(xs) - 1)] - xs[lo])


def emit_lat(scenario, lat):
    emit(scenario, "p50", 1000 * quantile(lat, 0.5), "ms")
    emit(scenario, "p99", 1000 * quantile(lat, 0.99), "ms")


def timed(f):
    t = time.perf_counter()
    f()
    return time.perf_counter() - t


def bench_eventhubs(warm=False):
    N = 200 if warm else n(3_000)
    producer = EventHubProducerClient.from_connection_string(EH_CS, eventhub_name="eh1")
    with producer:
        start_seq = producer.get_partition_properties("0")["last_enqueued_sequence_number"]

        def send_all():
            batch = producer.create_batch(partition_id="0")
            for _ in range(N):
                ev = EventData(PAYLOAD)
                try:
                    batch.add(ev)
                except ValueError:
                    producer.send_batch(batch)
                    batch = producer.create_batch(partition_id="0")
                    batch.add(ev)
            if len(batch) > 0:
                producer.send_batch(batch)
        t = timed(send_all)
        if not warm: emit("eh_send_batched", "throughput", N / t, "events/s")
        lat = [timed(lambda: producer.send_event(EventData(PAYLOAD), partition_id="1")) for _ in range(5 if warm else n(300))]
        if not warm: emit_lat("eh_send_single", lat)

        consumer = EventHubConsumerClient.from_connection_string(EH_CS, consumer_group="$default", eventhub_name="eh1")
        got = [0]
        done = threading.Event()

        def on_batch(ctx, evs):
            got[0] += len(evs)
            if got[0] >= N:
                done.set()
        t0 = time.perf_counter()
        th = threading.Thread(target=consumer.receive_batch, kwargs=dict(on_event_batch=on_batch, partition_id="0",
            starting_position=start_seq, starting_position_inclusive=False, max_batch_size=300, max_wait_time=5, prefetch=300))
        th.start()
        done.wait(300)
        t = time.perf_counter() - t0
        consumer.close()
        th.join()
        if got[0] < N: raise RuntimeError(f"received only {got[0]} of {N} events")
        if not warm: emit("eh_receive", "throughput", N / t, "events/s")


# The emulator delivers in bursts and can stall for longer than max_wait_time, so an
# empty receive does not mean the queue is empty: check with peek_messages.
def drain_queue(client, q):
    with client.get_queue_receiver(q, receive_mode=ServiceBusReceiveMode.RECEIVE_AND_DELETE, max_wait_time=2) as r:
        while r.receive_messages(max_message_count=100) or r.peek_messages(max_message_count=1):
            pass


def bench_servicebus(warm=False):
    N = 100 if warm else n(5_000)
    with ServiceBusClient.from_connection_string(SB_CS) as client:
        drain_queue(client, "queue.1")
        sender = client.get_queue_sender("queue.1")
        with sender:
            t = timed(lambda: [sender.send_messages([ServiceBusMessage(PAYLOAD) for _ in range(i, min(i + 100, N))])
                               for i in range(0, N, 100)])
            if not warm: emit("sb_send_batched", "throughput", N / t, "msgs/s")

            def recv_all():
                got, t0 = 0, time.perf_counter()
                with client.get_queue_receiver("queue.1", max_wait_time=5) as r:
                    while got < N:
                        msgs = r.receive_messages(max_message_count=100, max_wait_time=5)
                        if msgs: t0 = time.perf_counter()
                        elif time.perf_counter() - t0 > 120: raise RuntimeError(f"no messages for 120 s after {got} of {N}")
                        for m in msgs:
                            r.complete_message(m)
                        got += len(msgs)
            t = timed(recv_all)
            if not warm: emit("sb_receive_complete", "throughput", N / t, "msgs/s")
            # receive-and-delete (no settlement round trips)
            for i in range(0, N, 100):
                sender.send_messages([ServiceBusMessage(PAYLOAD) for _ in range(i, min(i + 100, N))])

            def recv_delete():
                got, t0 = 0, time.perf_counter()
                with client.get_queue_receiver("queue.1", receive_mode=ServiceBusReceiveMode.RECEIVE_AND_DELETE,
                                               max_wait_time=5) as r:
                    while got < N:
                        msgs = r.receive_messages(max_message_count=100, max_wait_time=5)
                        if msgs: t0 = time.perf_counter()
                        elif time.perf_counter() - t0 > 120: raise RuntimeError(f"no messages for 120 s after {got} of {N}")
                        got += len(msgs)
            t = timed(recv_delete)
            if not warm: emit("sb_receive_and_delete", "throughput", N / t, "msgs/s")
            lat = [timed(lambda: sender.send_messages(ServiceBusMessage(PAYLOAD))) for _ in range(5 if warm else n(300))]
            if not warm: emit_lat("sb_send_single", lat)
        drain_queue(client, "queue.1")


def bench_blob(warm=False):
    svc = BlobServiceClient.from_connection_string(BLOB_CS, max_single_put_size=8 * 1024**2)
    cname = "bench-py-" + "".join(random.choices(string.ascii_lowercase, k=8))
    container = svc.create_container(cname)
    try:
        big = os.urandom(16 * 1024**2 if warm else n(64) * 1024**2)
        mb = len(big) / 1024**2
        t = timed(lambda: container.upload_blob("big.bin", big, overwrite=True, max_concurrency=8))
        if not warm: emit("blob_upload_64MiB_c8", "throughput", mb / t, "MiB/s")
        back = [None]
        t = timed(lambda: back.__setitem__(0, container.download_blob("big.bin", max_concurrency=8).readall()))
        if back[0] != big: raise RuntimeError("download mismatch")
        if not warm: emit("blob_download_64MiB_c8", "throughput", mb / t, "MiB/s")

        small = os.urandom(4096)
        lat = [timed(lambda: container.upload_blob(f"seq/{i}", small, overwrite=True)) for i in range(10 if warm else n(300))]
        if not warm:
            emit_lat("blob_put_4KiB_seq", lat)
            emit("blob_put_4KiB_seq", "throughput", len(lat) / sum(lat), "ops/s")

        M = 64 if warm else n(2_000)
        with ThreadPoolExecutor(16) as pool:
            t = timed(lambda: list(pool.map(lambda i: container.upload_blob(f"par/{i}", small, overwrite=True), range(M))))
            if not warm: emit("blob_put_4KiB_c16", "throughput", M / t, "ops/s")
            t = timed(lambda: list(pool.map(lambda i: container.download_blob(f"par/{i}").readall(), range(M))))
            if not warm: emit("blob_get_4KiB_c16", "throughput", M / t, "ops/s")
        cnt = [0]
        t = timed(lambda: cnt.__setitem__(0, sum(1 for _ in container.list_blobs(name_starts_with="par/"))))
        if cnt[0] != M: raise RuntimeError(f"listed {cnt[0]} of {M} blobs")
        if not warm: emit("blob_list_2000", "throughput", M / t, "blobs/s")
    finally:
        svc.delete_container(cname)


if __name__ == "__main__":
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    which = args[0] if args else "all"
    benches = {"blob": bench_blob, "eventhubs": bench_eventhubs, "servicebus": bench_servicebus}
    for name, f in (sorted(benches.items()) if which == "all" else [(which, benches[which])]):
        f(warm=True)
        f()
    emit("process", "peak_rss", resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024, "MiB")
