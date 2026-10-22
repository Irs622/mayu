import os
import sys


def rss_mb(pid: int | None = None) -> float | None:
    """Resident memory of this process (or of process `pid`, from /proc only) in MB (peak RSS where /proc is
    unavailable)."""
    try:
        with open("/proc/self/statm" if pid is None else f"/proc/{pid}/statm") as f:
            pages = int(f.read().split()[1])
        return pages * os.sysconf("SC_PAGE_SIZE") / 1_048_576
    except (OSError, ValueError, IndexError):
        pass
    if pid is not None:
        return None
    try:
        import resource
        peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    except (ImportError, OSError):
        return None
    return peak / 1_048_576 if sys.platform == "darwin" else peak / 1024
