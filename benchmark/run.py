"""Run bench.jl and bench.py against the local emulators and compare them.

    python benchmark/run.py [--reps=3] [--scale=1.0] [--python=python3] [--out=results.json]
                            [--services=blob,eventhubs,servicebus]

Each service is benchmarked in a fresh process per SDK and repetition; the order
of the SDKs alternates between repetitions. Reported values are medians.
"""
import json, os, statistics, subprocess, sys, time

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
opts = dict(a[2:].split("=", 1) for a in sys.argv[1:] if a.startswith("--") and "=" in a)
REPS, SCALE = int(opts.get("reps", 3)), opts.get("scale", "1.0")
PY = opts.get("python", sys.executable)
OUT = opts.get("out", os.path.join(HERE, "results.json"))
SERVICES = opts.get("services", "blob,eventhubs,servicebus").split(",")
JULIA = ["julia", "-t", "8", f"--project={ROOT}"]

COLD_JL = """using AzureSDK.Storage.Blob
svc = from_connection_string(BlobServiceClient, "UseDevelopmentStorage=true")
c = create_container(svc, "cold-jl-" * string(rand(UInt32)))
upload_blob(c, "x", "hello"); delete_container(c)"""
COLD_PY = """import random
from azure.storage.blob import BlobServiceClient
svc = BlobServiceClient.from_connection_string("UseDevelopmentStorage=true")
c = svc.create_container("cold-py-%d" % random.getrandbits(32))
c.upload_blob("x", b"hello"); c.delete_container()"""


def cmd(sdk, service):
    if sdk == "julia":
        return JULIA + [os.path.join(HERE, "bench.jl"), service, f"--scale={SCALE}"]
    return [PY, os.path.join(HERE, "bench.py"), service, f"--scale={SCALE}"]


samples = {}  # (scenario, metric, sdk) -> [values]
units = {}


def record(sdk, scenario, metric, value, unit):
    samples.setdefault((scenario, metric, sdk), []).append(value)
    units[(scenario, metric)] = unit


for rep in range(REPS):
    order = ["julia", "python"] if rep % 2 == 0 else ["python", "julia"]
    for service in SERVICES:
        for sdk in order:
            if service == "eventhubs":
                time.sleep(20)  # the Event Hubs emulator throttles (server-busy) above ~1000 ops per window
            t = time.time()
            p = subprocess.run(cmd(sdk, service), capture_output=True, text=True, cwd=ROOT)
            print(f"rep {rep + 1}/{REPS} {service:10s} {sdk:6s} {time.time() - t:6.1f} s  exit {p.returncode}", flush=True)
            if p.returncode != 0:
                print(p.stderr[-3000:], file=sys.stderr)
                continue
            for line in p.stdout.splitlines():
                if line.startswith("{"):
                    r = json.loads(line)
                    scen = r["scenario"] if r["scenario"] != "process" else f"process_{service}"
                    record(sdk, scen, r["metric"], r["value"], r["unit"])
    for sdk, script in (("julia", JULIA + ["-e", COLD_JL]), ("python", [PY, "-c", COLD_PY])):
        t = time.perf_counter()
        subprocess.run(script, check=True, cwd=ROOT, capture_output=True)
        record(sdk, "cold_start_first_upload", "wall", time.perf_counter() - t, "s")

rows = []
for (scenario, metric), unit in units.items():
    jl, py = samples.get((scenario, metric, "julia"), []), samples.get((scenario, metric, "python"), [])
    rows.append(dict(scenario=scenario, metric=metric, unit=unit,
                     julia=statistics.median(jl) if jl else None, python=statistics.median(py) if py else None,
                     julia_samples=jl, python_samples=py))
json.dump(dict(reps=REPS, scale=float(SCALE), rows=rows), open(OUT, "w"), indent=1)

higher_better = {"throughput"}
print(f"\n{'scenario':28s} {'metric':10s} {'julia':>12s} {'python':>12s}  ratio (>1 = julia better)")
for r in rows:
    if r["julia"] is None or r["python"] is None:
        continue
    ratio = r["julia"] / r["python"] if r["metric"] in higher_better else r["python"] / r["julia"]
    print(f"{r['scenario']:28s} {r['metric']:10s} {r['julia']:12.2f} {r['python']:12.2f}  {ratio:6.2f}x  {r['unit']}")
