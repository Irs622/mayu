"""Every system x workload end to end at small n, in the interpreter that hosts it."""
import json
import os
import pathlib
import signal
import subprocess
import sys
import sysconfig
import time
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


def test_sigterm_to_run_py_stops_the_workers():
    system = "mayu-thread" if FREE_THREADED else "celery"
    _skip_unless_hosted_here(system)
    tag = uuid.uuid4().hex
    proc = subprocess.Popen(
        [sys.executable, str(RUN), system, "noop", "--n", "30000", "--concurrency", "8", "--timeout", "600"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env={**os.environ, "BENCH_TAG": tag})
    try:
        deadline = time.monotonic() + 120
        # run.py carries BENCH_TAG too: wait for a worker, not just for run.py itself
        while not [p for p in _survivors(tag) if p != proc.pid] and proc.poll() is None \
                and time.monotonic() < deadline:
            time.sleep(0.2)
        assert [p for p in _survivors(tag) if p != proc.pid], "workers never started"
        time.sleep(1)  # let run.py reach its sampling loop
        proc.send_signal(signal.SIGTERM)
        proc.wait(timeout=60)
        deadline = time.monotonic() + 10
        while _survivors(tag) and time.monotonic() < deadline:
            time.sleep(0.2)
        assert _survivors(tag) == [], "SIGTERM left worker processes running"
    finally:
        if proc.poll() is None:
            proc.kill()
        for pid in _survivors(tag):
            try:
                psutil.Process(pid).kill()
            except psutil.NoSuchProcess:
                pass
