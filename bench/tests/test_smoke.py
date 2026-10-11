"""Every system x workload end to end at small n, in the interpreter that hosts it."""
import json
import os
import pathlib
import subprocess
import sys
import sysconfig
import uuid

import psutil
import pytest

from run import SYSTEMS

RUN = pathlib.Path(__file__).resolve().parents[1] / "run.py"
FREE_THREADED = bool(sysconfig.get_config_var("Py_GIL_DISABLED"))
N = 300


def _skip_unless_hosted_here(system):
    pytest.importorskip(system.split("-")[0])
    if system == "mayu-thread" and not FREE_THREADED:
        pytest.skip("mayu-thread runs on free-threaded Python (.venv)")
    if system != "mayu-thread" and FREE_THREADED:
        pytest.skip(f"{system} runs on the GIL build (.venv-bench)")


def _survivors(tag):
    out = []
    for p in psutil.process_iter():
        try:
            if p.environ().get("BENCH_TAG") == tag:
                out.append(p.pid)
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            pass
    return out


@pytest.mark.parametrize("workload_name", ["noop", "realistic"])
@pytest.mark.parametrize("system", sorted(SYSTEMS))
def test_runs_to_completion(system, workload_name):
    _skip_unless_hosted_here(system)
    tag = uuid.uuid4().hex
    out = subprocess.run(
        [sys.executable, str(RUN), system, workload_name, "--n", str(N), "--concurrency", "8", "--timeout", "120"],
        capture_output=True, text=True, timeout=240, env={**os.environ, "BENCH_TAG": tag})
    assert out.returncode == 0, out.stderr
    r = json.loads(out.stdout.strip().splitlines()[-1])
    assert r["system"] == system and r["workload"] == workload_name
    assert r["executed"] == N
    assert r["rate"] > 0 and r["peak_rss_mb"] > 0
    assert _survivors(tag) == [], "worker processes outlived the run"
