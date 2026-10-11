"""Run one benchmark: python bench/run.py SYSTEM WORKLOAD [--n N] [--concurrency C] [--out FILE].

Empties the bench Redis DB, enqueues N tasks, starts SYSTEM's workers, waits for N completions, and prints
one JSON line. Run it with the interpreter that has SYSTEM installed (see bench/all.sh)."""
import argparse
import importlib
import importlib.metadata
import json
import os
import pathlib
import signal
import subprocess
import sys
import time

HERE = pathlib.Path(__file__).resolve().parent
sys.path[:0] = [str(HERE), str(HERE / "systems")]

import redis  # noqa: E402

import measure  # noqa: E402
import workload  # noqa: E402

# system -> (adapter module, worker-command function)
SYSTEMS = {
    "mayu-thread": ("bench_mayu", "thread_cmd"),
    "mayu-process": ("bench_mayu", "process_cmd"),
    "celery": ("bench_celery", "worker_cmd"),
    "rq": ("bench_rq", "worker_cmd"),
}
OWNER_KEY = "bench:owner"
SAMPLE_EVERY = 0.005
MEMORY_EVERY = 0.25


class BenchError(Exception):
    pass


def claim_db(r) -> None:
    """Empties the bench DB. Refuses a DB that holds keys the bench did not create."""
    if r.dbsize() and not r.exists(OWNER_KEY):
        raise BenchError(f"refusing to flush {r.connection_pool.connection_kwargs}: it holds keys and has no "
                         f"{OWNER_KEY!r} marker; point MAYU_BENCH_REDIS at an empty database")
    r.flushdb()
    r.set(OWNER_KEY, "1")


def drive(proc, count, n, timeout, memory=None, clock=time.monotonic, sleep=time.sleep):
    """Samples count() until it reaches n. Returns (samples, (peak_rss, peak_pss))."""
    samples, peak = [], (0, 0)
    start = next_memory = clock()
    while True:
        now = clock()
        c = count()
        samples.append((now, c))
        if c >= n:
            return samples, peak
        if proc.poll() is not None:
            raise BenchError(f"workers exited with code {proc.returncode} after {c} of {n} tasks")
        if now - start > timeout:
            raise BenchError(f"timed out after {timeout:g}s: {c} of {n} tasks done")
        if memory is not None and now >= next_memory:
            rss, pss = memory()
            peak = (max(peak[0], rss), max(peak[1], pss))
            next_memory = now + MEMORY_EVERY
        sleep(SAMPLE_EVERY)


def stop(proc) -> None:
    """SIGTERM the workers' session, then SIGKILL whatever is left of it (orphaned children included)."""
    try:
        os.killpg(proc.pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    try:
        proc.wait(timeout=30)
    except subprocess.TimeoutExpired:
        pass
    try:
        os.killpg(proc.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    proc.wait()


def main(argv=None) -> int:
    p = argparse.ArgumentParser(prog="bench/run.py")
    p.add_argument("system", choices=sorted(SYSTEMS))
    p.add_argument("workload", choices=("noop", "realistic"))
    p.add_argument("--n", type=int, default=20_000)
    p.add_argument("--concurrency", type=int, default=64)
    p.add_argument("--timeout", type=float, default=900)
    p.add_argument("--out", type=pathlib.Path, default=None, help="append the JSON line to this file")
    a = p.parse_args(argv)
    if a.concurrency < 2 or a.concurrency % 2:
        p.error("--concurrency must be an even number >= 2")

    module, cmd_name = SYSTEMS[a.system]
    adapter = importlib.import_module(module)
    r = redis.Redis.from_url(workload.URL)
    try:
        claim_db(r)
    except BenchError as e:
        print(f"{a.system} {a.workload}: {e}", file=sys.stderr)
        return 1
    adapter.enqueue(a.workload, a.n)
    cmd, extra_env, layout = getattr(adapter, cmd_name)(a.concurrency)

    logdir = HERE / "results" / "logs"
    logdir.mkdir(parents=True, exist_ok=True)
    log_path = logdir / f"{a.system}-{a.workload}.log"
    env = {**os.environ, **extra_env,
           "PYTHONPATH": os.pathsep.join([str(HERE), str(HERE / "systems"), os.environ.get("PYTHONPATH", "")])}
    with open(log_path, "wb") as log:
        t0 = time.monotonic()
        proc = subprocess.Popen(cmd, env=env, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
        try:
            samples, (peak_rss, peak_pss) = drive(
                proc, lambda: int(r.get(workload.DONE_KEY) or 0), a.n, a.timeout,
                memory=lambda: measure.tree_memory(proc.pid))
        except BenchError as e:
            print(f"{a.system} {a.workload}: {e} (log: {log_path})", file=sys.stderr)
            return 1
        finally:
            stop(proc)

    wall = samples[-1][0] - t0
    try:
        rate, rate_kind = measure.steady_rate(samples, a.n), "steady"
    except ValueError:
        rate, rate_kind = a.n / wall, "wall"
    result = {
        "system": a.system, "workload": a.workload, "n": a.n,
        "executed": int(r.get(workload.DONE_KEY) or 0), "concurrency": a.concurrency, "layout": layout,
        "rate": round(rate, 1), "rate_kind": rate_kind, "wall_seconds": round(wall, 3),
        "peak_rss_mb": round(peak_rss / 2**20, 1), "peak_pss_mb": round(peak_pss / 2**20, 1),
        "python": sys.version.split()[0] + ("t" if workload._free_threaded_build() else ""),
        "library_version": importlib.metadata.version(a.system.split("-")[0]),
    }
    line = json.dumps(result)
    print(line)
    if a.out is not None:
        with open(a.out, "a") as f:
            f.write(line + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
