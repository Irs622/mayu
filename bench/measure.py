"""Throughput and memory measurements for bench/run.py."""
import psutil


def steady_rate(samples, n, lo=0.05, hi=0.95):
    """Tasks per second between the first sample at or past lo*n completions and the first at or past hi*n.

    samples: (monotonic seconds, completed count) in time order. Raises ValueError when the window has no
    two distinct points (the run was too short to sample)."""
    a = next((s for s in samples if s[1] >= lo * n), None)
    b = next((s for s in samples if s[1] >= hi * n), None)
    if a is None or b is None or b[0] <= a[0]:
        raise ValueError("not enough samples to measure a steady rate")
    return (b[1] - a[1]) / (b[0] - a[0])


def tree_memory(pid):
    """(rss, pss) in bytes, summed over pid and all its descendants; pss falls back to rss off Linux."""
    try:
        root = psutil.Process(pid)
        procs = [root, *root.children(recursive=True)]
    except psutil.NoSuchProcess:
        return (0, 0)
    rss = pss = 0
    for p in procs:
        try:
            info = p.memory_full_info()
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            continue
        rss += info.rss
        pss += getattr(info, "pss", info.rss)
    return (rss, pss)
