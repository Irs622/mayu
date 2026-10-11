import os
import pathlib
import signal
import subprocess
import sys
import threading
import time

import pytest

from mayu import App, TaskFailed
from mayu.recycle import rss_mb
from mayu.worker import WorkerRunner

HERE = pathlib.Path(__file__).parent


def _spawn(redis_url, queue, *flags):
    env = {**os.environ, "MAYU_BROKER": redis_url, "MAYU_DEFAULT_QUEUE": queue, "PYTHONPATH": str(HERE)}
    return subprocess.Popen([sys.executable, "-m", "mayu", "worker", "sample_app:app", *flags],
                            env=env, stderr=subprocess.DEVNULL)


def _producer(redis_url, queue):
    app = App(broker=redis_url, result_ttl=60, default_queue=queue)

    @app.task(name="sample.boot")
    def boot():
        pass

    @app.task(name="sample.sleepy")
    def sleepy():
        pass

    return boot, sleepy


def _stop(proc):
    proc.send_signal(signal.SIGTERM)
    try:
        proc.wait(timeout=20)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()
    return proc.returncode


def test_rss_mb_reports_this_process():
    rss = rss_mb()
    assert rss is not None and rss > 1


def test_run_returns_reason_after_max_tasks(app):
    @app.task
    def f():
        return 1

    w = WorkerRunner(app, threads=1, max_tasks_per_process=2)
    out = []
    th = threading.Thread(target=lambda: out.append(w.run()))
    th.start()
    assert f.send().result(timeout=5) == 1
    assert f.send().result(timeout=5) == 1
    th.join(15)
    assert out and "max_tasks_per_process" in out[0]


def test_run_returns_reason_on_memory_limit(app):
    w = WorkerRunner(app, threads=1, max_memory_per_process=1)
    out = []
    th = threading.Thread(target=lambda: out.append(w.run()))
    th.start()
    th.join(15)
    assert out and "memory" in out[0]


def test_recycling_options_validated(app):
    with pytest.raises(ValueError):
        WorkerRunner(app, threads=1, max_zombie_threads=-1)
    with pytest.raises(ValueError):
        WorkerRunner(app, threads=1, max_tasks_per_process=0)
    with pytest.raises(ValueError):
        WorkerRunner(app, threads=1, max_memory_per_process=0)


def test_max_tasks_per_process_reexecs(redis_url, queue):
    boot, _ = _producer(redis_url, queue)
    proc = _spawn(redis_url, queue, "--threads", "1", "--max-tasks-per-process", "2")
    try:
        b1 = boot.send().result(timeout=15)
        b2 = boot.send().result(timeout=15)
        b3 = boot.send().result(timeout=30)
        assert b1 == b2 != b3, "third task must run in a fresh interpreter"
        assert proc.poll() is None, "re-exec keeps the same process running"
    finally:
        assert _stop(proc) == 0


def test_zombies_trigger_reexec(redis_url, queue):
    boot, sleepy = _producer(redis_url, queue)
    proc = _spawn(redis_url, queue, "--threads", "2", "--max-zombie-threads", "1", "--shutdown-grace", "30")
    try:
        b0 = boot.send().result(timeout=15)
        t = time.monotonic()
        for h in [sleepy.send(), sleepy.send()]:
            with pytest.raises(TaskFailed, match="TimeLimitExceeded"):
                h.result(timeout=10)
        b1 = boot.send().result(timeout=20)
        assert b1 != b0, "two zombies > max 1 must recycle the worker"
        assert time.monotonic() - t < 20, "recycling must not wait out the 30s grace for zombies"
    finally:
        assert _stop(proc) == 0
