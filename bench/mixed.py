"""Run one mixed-Python-versions benchmark: python bench/mixed.py LAYOUT WORKLOAD [--n N] [--concurrency C] ...

A 3.14t producer pre-fills N tasks, then three worker groups start at the same moment: 3.14t in thread mode
(.venv), 3.14 and 3.13 in process mode (.venv-gil, .venv-313). LAYOUT is `shared` (one queue for all groups) or
`routed` (a queue per group, tasks dealt round-robin). Completions come from each worker process's /metrics,
because the 3.13/3.14 venvs have no `redis` package. Run it with .venv/bin/python (see bench/all.sh)."""
import argparse
import concurrent.futures
import dataclasses
import json
import os
import pathlib
import re
import signal
import subprocess
import sys
import threading
import time
import urllib.request
import uuid

HERE = pathlib.Path(__file__).resolve().parent
REPO = HERE.parent
sys.path[:0] = [str(HERE), str(HERE / "systems")]

import measure  # noqa: E402
import run  # noqa: E402
import workload  # noqa: E402

# venv, worker mode, first metrics port (process-mode child i serves on base + i)
GROUPS = (
    (".venv", "thread", 19100),
    (".venv-gil", "process", 19200),
    (".venv-313", "process", 19300),
)
SETTLED = re.compile(r'^mayu_tasks_settled_total\{[^}]*outcome="success"[^}]*\} (\S+)$', re.M)
POLL_EVERY = 0.1
PRODUCERS = 8


@dataclasses.dataclass
class Group:
    name: str
    python: str
    exe: pathlib.Path
    mode: str
    procs: int
    threads: int
    layout: str
    ports: list
    queue: str = ""
    proc: subprocess.Popen = None
    peak: list = dataclasses.field(default_factory=lambda: [0, 0])  # (pss, rss) bytes


def settled_total(text: str) -> float:
    """Successful task completions in one /metrics page, summed over its label sets."""
    return sum(float(v) for v in SETTLED.findall(text))


def group_rate(series, total) -> float:
    """Steady tasks/s of one group between 5 % and 95 % of its own final count; 0 when it did too little to tell."""
    try:
        return measure.steady_rate(series, total)
    except ValueError:
        return 0.0


def finish_seconds(series, total, default) -> float:
    """Time of the first sample that reaches `total`."""
    return next((t for t, c in series if c >= total), default)


def python_version(exe) -> str:
    code = "import sys, sysconfig; print(sys.version.split()[0], sysconfig.get_config_var('Py_GIL_DISABLED') or 0)"
    version, free_threaded = subprocess.check_output([str(exe), "-c", code], text=True).split()
    return version + ("t" if free_threaded == "1" else "")


def find_groups(concurrency: int) -> list:
    groups = []
    for venv, mode, port in GROUPS:
        exe = REPO / venv / "bin" / "python"
        if not exe.exists():
            print(f"skipping the {venv} group: {exe} does not exist (run scripts/matrix.sh)", file=sys.stderr)
            continue
        py = python_version(exe)
        procs, threads = (1, concurrency) if mode == "thread" else (concurrency, 1)
        layout = f"1 process × {threads} threads" if mode == "thread" else f"{procs} processes"
        minor = ".".join(py.rstrip("t").split(".")[:2]) + ("t" if py.endswith("t") else "")
        groups.append(Group(f"{minor} {mode}", py, exe, mode, procs, threads, layout, [port + i for i in range(procs)]))
    return groups


def scrape(port):
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=1) as r:
            return settled_total(r.read().decode())
    except OSError:
        return None


def prefill(task, args, groups, layout, n) -> None:
    """Enqueues n tasks from several threads; a routed layout deals them round-robin over the groups' queues."""
    def send(k):
        for i in range(k, n, PRODUCERS):
            task.send(*args, queue=groups[i % len(groups)].queue if layout == "routed" else groups[0].queue)

    with concurrent.futures.ThreadPoolExecutor(PRODUCERS) as pool:
        list(pool.map(send, range(PRODUCERS)))


def launch(groups, env, logdir, tag) -> None:
    for g in groups:
        cmd = [str(g.exe), "-m", "mayu", "worker", "bench_mixed:app", "--queues", g.queue, "--mode", g.mode,
               "--processes", str(g.procs), "--metrics-port", str(g.ports[0]), "--prefetch-multiplier", "2"]
        if g.mode == "thread":
            cmd += ["--threads", str(g.threads)]
        with open(logdir / f"mixed-{tag}-{g.mode}-{g.python}.log", "wb") as log:
            g.proc = subprocess.Popen(cmd, env=env, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)


def sample_memory(groups, stop) -> None:
    while not stop.wait(run.MEMORY_EVERY):
        for g in groups:
            rss, pss = measure.tree_memory(g.proc.pid)
            g.peak = [max(g.peak[0], pss), max(g.peak[1], rss)]


def watch(groups, n, timeout, clock=time.monotonic, sleep=time.sleep):
    """Polls every worker's /metrics until n tasks completed. Returns [(seconds since launch, {group: done})]."""
    seen = {p: 0.0 for g in groups for p in g.ports}  # a missed scrape keeps the last count
    t0 = clock()
    samples = []
    while True:
        for p in seen:
            v = scrape(p)
            if v is not None:
                seen[p] = v
        now = clock() - t0
        done = {g.name: sum(seen[p] for p in g.ports) for g in groups}
        samples.append((now, done))
        total = sum(done.values())
        if total >= n:
            return samples
        for g in groups:
            if g.proc.poll() is not None:
                raise run.BenchError(f"{g.name} workers exited with code {g.proc.returncode} "
                                     f"after {total:.0f} of {n} tasks")
        if now > timeout:
            raise run.BenchError(f"timed out after {timeout:g}s: {total:.0f} of {n} tasks done")
        sleep(POLL_EVERY)


def main(argv=None) -> int:
    p = argparse.ArgumentParser(prog="bench/mixed.py")
    p.add_argument("layout", choices=("shared", "routed"))
    p.add_argument("workload", choices=("noop", "realistic"))
    p.add_argument("--n", type=int, default=150_000)
    p.add_argument("--concurrency", type=int, default=16, help="per group: threads, or processes")
    p.add_argument("--timeout", type=float, default=900)
    p.add_argument("--out", type=pathlib.Path, default=None, help="append the JSON line to this file")
    a = p.parse_args(argv)

    # Die through the `finally` below on SIGTERM, or the workers (their own sessions) outlive us.
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(143))

    import bench_mixed

    groups = find_groups(a.concurrency)
    if not groups:
        p.error("no worker venvs found; run scripts/matrix.sh and bench/setup.sh")
    import redis
    try:
        run.claim_db(redis.Redis.from_url(workload.URL))
    except run.BenchError as e:
        print(f"mixed {a.layout} {a.workload}: {e}", file=sys.stderr)
        return 1
    tag = uuid.uuid4().hex[:6]  # fresh queues, so a run never sees another's leftovers
    for i, g in enumerate(groups):
        g.queue = f"mixed-{tag}" if a.layout == "shared" else f"mixed-{tag}-{i}"
    task, args = (bench_mixed.noop, ()) if a.workload == "noop" else (bench_mixed.realistic, (workload.PAYLOAD,))
    prefill(task, args, groups, a.layout, a.n)

    logdir = HERE / "results" / "logs"
    logdir.mkdir(parents=True, exist_ok=True)
    env = {**os.environ,
           "PYTHONPATH": os.pathsep.join([str(HERE), str(HERE / "systems"), os.environ.get("PYTHONPATH", "")])}
    stop_memory = threading.Event()
    sampler = threading.Thread(target=sample_memory, args=(groups, stop_memory), daemon=True)
    try:
        launch(groups, env, logdir, tag)
        sampler.start()
        samples = watch(groups, a.n, a.timeout)
    except run.BenchError as e:
        print(f"mixed {a.layout} {a.workload}: {e} (logs: {logdir})", file=sys.stderr)
        return 1
    finally:
        stop_memory.set()
        for g in groups:
            if g.proc is not None:
                run.stop(g.proc)

    wall, final = samples[-1]
    executed = sum(final.values())
    total_series = [(t, sum(d.values())) for t, d in samples]
    total_rate = group_rate(total_series, a.n) or a.n / wall
    result = {
        "layout": a.layout, "workload": a.workload, "n": a.n, "executed": int(executed),
        "concurrency": a.concurrency, "rate": round(total_rate, 1), "wall_seconds": round(wall, 3), "groups": [],
    }
    for g in groups:
        series = [(t, d[g.name]) for t, d in samples]
        result["groups"].append({
            "name": g.name, "python": g.python, "mode": g.mode, "layout": g.layout, "tasks": int(final[g.name]),
            "share": round(100 * final[g.name] / executed, 1), "rate": round(group_rate(series, final[g.name]), 1),
            "finish_seconds": round(finish_seconds(series, final[g.name], wall), 3),
            "peak_pss_mb": round(g.peak[0] / 2**20, 1), "peak_rss_mb": round(g.peak[1] / 2**20, 1),
        })
    line = json.dumps(result)
    print(line)
    if a.out is not None:
        with open(a.out, "a") as f:
            f.write(line + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
