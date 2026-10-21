"""Render bench/results/*.jsonl as markdown: python bench/report.py FILE [MIXED_FILE] > bench/RESULTS.md"""
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
- mayu is a release build (`maturin develop --release`).
- Memory is the peak over the run of the whole worker process tree, sampled every 250 ms. PSS shares pages
  fairly between forked children; RSS counts shared pages once per process.
- RQ runs `SimpleWorker` processes. Its default worker forks a work horse per job and reconnects to Redis each
  time; at this rate that exhausts the ephemeral ports behind Docker's userland proxy and the run cannot finish.
- Each figure is the median of the repeated runs. Reproduce with `bench/setup.sh && bench/all.sh`.
"""


def _median(rows, key):
    return statistics.median(r[key] for r in rows)


MIXED_DESCRIPTION = """\
One 3.14t producer pre-fills the queue, then three worker groups start at the same moment: 3.14t in thread mode,
3.14 and 3.13 (both with the GIL) in process mode. Completions are read from each worker's `/metrics`. Groups on
a shared queue split the tasks by whichever reader is blocked when a task arrives, so the queue is pre-filled
to keep every group busy; the routed layout gives each group its own queue and an equal share of the tasks.
"""


def _mixed_section(mixed) -> list:
    """Per layout: tasks/s (share of tasks) per worker group, then peak memory, medians over the repeats."""
    out = ["## Mixed Python versions", "", MIXED_DESCRIPTION]
    for layout in ("shared", "routed"):
        runs = {wl: [r for r in mixed if r["layout"] == layout and r["workload"] == wl]
                for wl in ("noop", "realistic")}
        runs = {wl: rows for wl, rows in runs.items() if rows}
        if not runs:
            continue
        names = {}  # group name -> its first record, in the order the groups first appear; absent groups get no column
        for rows in runs.values():
            for r in rows:
                for g in r["groups"]:
                    names.setdefault(g["name"], g)
        out += ["### Shared queue" if layout == "shared" else "### Queue per group", "",
                *[f"- {n}: Python {g['python']}, {g['layout']}" for n, g in names.items()], "",
                "| workload | " + " | ".join(names) + " | total tasks/s |",
                "|---|" + "---:|" * (len(names) + 1)]
        memory = []
        for wl, rows in runs.items():
            cells, mem = [], []
            for n in names:
                gs = [g for r in rows for g in r["groups"] if g["name"] == n]
                if not gs:
                    cells.append("—")
                    mem.append("—")
                    continue
                cells.append(f"{_median(gs, 'rate'):.0f} ({_median(gs, 'share'):.0f} %)")
                mem.append(f"{_median(gs, 'peak_pss_mb'):.0f} / {_median(gs, 'peak_rss_mb'):.0f}")
            out.append(f"| {wl} | " + " | ".join(cells) + f" | {_median(rows, 'rate'):.0f} |")
            memory.append(f"| peak PSS / RSS MB, {wl} | " + " | ".join(mem) + " | |")
        out += [*memory, ""]
    return out


def render(records, machine, mixed=()) -> str:
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
    if mixed:
        out += _mixed_section(mixed)
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


def _load(path) -> list:
    return [json.loads(line) for line in pathlib.Path(path).read_text().splitlines() if line.strip()]


def main(argv=None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
    records = _load(argv[0])
    print(render(records, _machine(), _load(argv[1]) if len(argv) > 1 else ()))
    return 0


if __name__ == "__main__":
    sys.exit(main())
