"""Render bench/results/*.jsonl as markdown: python bench/report.py FILE > bench/RESULTS.md"""
import json
import os
import pathlib
import statistics
import sys

import redis

TITLES = {"noop": "No-op task throughput", "realistic": "Realistic task throughput"}
DESCRIPTIONS = {
    "noop": "The task body is one Redis `INCR` (the completion counter every system shares).",
    "realistic": "JSON-decode a 2.6 KB payload, sleep 5 ms (simulated I/O), about 1 ms of pure-Python CPU "
                 "work, then the `INCR`.",
}

METHOD = """\
- Each run pre-fills the queue with N tasks, then starts the workers. Throughput is the steady rate between 5 % and
  95 % of completions, so worker startup and the drain tail are excluded. Wall time includes startup.
- Every system runs at concurrency 64 with its defaults; only results and retries are off. Delivery semantics
  differ: Celery acknowledges a task before running it by default, mayu after it finishes.
- mayu thread mode runs on free-threaded CPython 3.14t; everything else runs on CPython 3.14 with the GIL.
- Memory is the peak over the run of the whole worker process tree, sampled every 250 ms. PSS shares pages
  fairly between forked children; RSS counts shared pages once per process.
- RQ runs `SimpleWorker` processes. Its default worker forks a work horse per job and reconnects to Redis each
  time; at this rate that exhausts the ephemeral ports behind Docker's userland proxy and the run cannot finish.
- Each figure is the median of the repeated runs. Reproduce with `bench/setup.sh && bench/all.sh`.
"""


def _median(rows, key):
    return statistics.median(r[key] for r in rows)


def render(records, machine) -> str:
    groups = {}
    for r in records:
        groups.setdefault((r["workload"], r["system"]), []).append(r)
    out = ["# mayu benchmarks", "",
           f"{machine['cpu']}, {machine['cores']} cores, Redis {machine['redis']}.", "", "## Method", "", METHOD]
    for wl in ("noop", "realistic"):
        systems = {s: rows for (w, s), rows in groups.items() if w == wl}
        if not systems:
            continue
        celery = _median(systems["celery"], "rate") if "celery" in systems else None
        out += [f"## {TITLES[wl]}", "", DESCRIPTIONS[wl], "",
                "| system | layout | tasks/s | wall s | peak PSS MB | peak RSS MB | vs Celery |",
                "|---|---|---:|---:|---:|---:|---:|"]
        for s, rows in sorted(systems.items(), key=lambda kv: -_median(kv[1], "rate")):
            rate = _median(rows, "rate")
            ratio = f"{rate / celery:.2f}×" if celery else "—"
            v = rows[0]
            out.append(f"| {s} | {v['layout']} (Python {v['python']}, {v['library_version']}) | {rate:.0f} | "
                       f"{_median(rows, 'wall_seconds'):.1f} | {_median(rows, 'peak_pss_mb'):.0f} | "
                       f"{_median(rows, 'peak_rss_mb'):.0f} | {ratio} |")
        out.append("")
    mt, ce = groups.get(("realistic", "mayu-thread")), groups.get(("realistic", "celery"))
    if mt and ce:
        m, c = _median(mt, "peak_pss_mb"), _median(ce, "peak_pss_mb")
        out += ["## Memory at equal concurrency", "",
                "Peak PSS during the realistic workload, 64 tasks in flight.", "",
                f"- mayu, {mt[0]['layout']}: **{m:.0f} MB**",
                f"- Celery, {ce[0]['layout']}: **{c:.0f} MB**",
                f"- Celery uses {c / m:.1f}× the memory of mayu.", ""]
    return "\n".join(out)


def _machine() -> dict:
    cpu = "unknown CPU"
    try:
        for line in pathlib.Path("/proc/cpuinfo").read_text().splitlines():
            if line.startswith("model name"):
                cpu = line.split(":", 1)[1].strip()
                break
    except OSError:
        pass
    import workload
    ver = redis.Redis.from_url(workload.URL).info("server")["redis_version"]
    return {"cpu": cpu, "cores": os.cpu_count(), "redis": ver}


def main(argv=None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
    records = [json.loads(line) for line in pathlib.Path(argv[0]).read_text().splitlines() if line.strip()]
    print(render(records, _machine()))
    return 0


if __name__ == "__main__":
    sys.exit(main())
