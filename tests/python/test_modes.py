import os
import pathlib
import signal
import subprocess
import sys
import time

import pytest

from mayu import App, TaskFailed
from mayu.modes import import_app_detecting_gil, resolve_mode

HERE = pathlib.Path(__file__).parent


def _env(redis_url, queue):
    return {**os.environ, "MAYU_BROKER": redis_url, "MAYU_DEFAULT_QUEUE": queue, "PYTHONPATH": str(HERE)}


def _spawn(redis_url, queue, *flags, stderr=subprocess.DEVNULL):
    return subprocess.Popen([sys.executable, "-m", "mayu", "worker", "sample_app:app", *flags],
                            env=_env(redis_url, queue), stderr=stderr, text=True)


def _producer(redis_url, queue):
    app = App(broker=redis_url, result_ttl=60, default_queue=queue)

    @app.task(name="sample.whoami")
    def whoami():
        pass

    @app.task(name="sample.sleepy")
    def sleepy():
        pass

    return whoami, sleepy


def _stop(proc, timeout=25):
    proc.send_signal(signal.SIGTERM)
    try:
        proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()
        raise
    return proc.returncode


def _alive(pid):
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


def _round(whoami, n):
    return [tuple(h.result(timeout=20)) for h in [whoami.send() for _ in range(n)]]


def test_resolve_mode():
    assert resolve_mode("auto", gil_on=False) == "thread"
    assert resolve_mode("auto", gil_on=True) == "process"
    assert resolve_mode("thread", gil_on=True) == "thread"
    assert resolve_mode("process", gil_on=False) == "process"


def test_offending_module_detected(monkeypatch):
    monkeypatch.syspath_prepend(str(HERE))
    app, offenders = import_app_detecting_gil("gil_offender_app:app")
    assert isinstance(app, App)
    assert offenders == ["fake_ext"]


def test_explicit_threads_with_process_mode_rejected(redis_url, queue):
    r = subprocess.run([sys.executable, "-m", "mayu", "worker", "sample_app:app",
                        "--mode", "process", "--threads", "4"],
                       env=_env(redis_url, queue), capture_output=True, text=True, timeout=30)
    assert r.returncode == 2
    assert "--threads" in r.stderr


def test_auto_mode_on_free_threaded_build_stays_in_process(redis_url, queue):
    if sys._is_gil_enabled():
        pytest.skip("needs a free-threaded build")
    whoami, _ = _producer(redis_url, queue)
    proc = _spawn(redis_url, queue, "--threads", "2", stderr=subprocess.PIPE)
    boot, pid = whoami.send().result(timeout=20)
    assert pid == proc.pid, "thread mode with one process runs in the CLI process itself"
    proc.send_signal(signal.SIGTERM)
    _, err = proc.communicate(timeout=25)
    assert proc.returncode == 0
    assert "process mode" not in err


def test_thread_mode_multi_process_runs_and_restarts_children(redis_url, queue):
    whoami, _ = _producer(redis_url, queue)
    # A SIGKILLed child can still be handed a task by Redis before the server notices its socket
    # closed; that task comes back only after the visibility timeout, so keep it under _round's 20 s.
    proc = _spawn(redis_url, queue, "--processes", "2", "--threads", "2", "--shutdown-grace", "5",
                  "--visibility-timeout", "3", "--heartbeat-interval", "1")
    seen = set()
    try:
        results = _round(whoami, 8)
        boots = {b for b, _ in results}
        pids = {p for _, p in results}
        seen |= pids
        assert len(boots) == 2 and len(pids) == 2 and proc.pid not in pids
        victim = next(iter(pids))
        os.kill(victim, signal.SIGKILL)
        deadline = time.monotonic() + 30
        while True:
            results = _round(whoami, 8)
            new = {p for _, p in results} - pids
            seen |= new
            if new:
                break
            assert time.monotonic() < deadline, "killed child was not replaced"
        assert proc.poll() is None
    finally:
        assert _stop(proc) == 0
    time.sleep(0.5)
    assert not any(_alive(p) for p in seen), "children must exit with the parent"


def test_process_mode_one_task_per_process_and_hard_limit_replaces_child(redis_url, queue):
    whoami, sleepy = _producer(redis_url, queue)
    proc = _spawn(redis_url, queue, "--mode", "process", "--processes", "1", "--shutdown-grace", "5",
                  "--prefetch-multiplier", "1")
    try:
        whoami.send().result(timeout=20)  # warm up: the child is running
        t = time.monotonic()
        (b1, p1), (b2, p2) = _round(whoami, 2)
        assert time.monotonic() - t >= 0.4, "one task at a time per process"
        assert b1 == b2 and p1 == p2
        with pytest.raises(TaskFailed, match="TimeLimitExceeded"):
            sleepy.send().result(timeout=15)
        b3, p3 = whoami.send().result(timeout=30)
        assert b3 != b1 and p3 != p1, "the stuck child was replaced"
        assert proc.poll() is None
    finally:
        assert _stop(proc) == 0


def test_children_exit_when_parent_killed(redis_url, queue):
    whoami, _ = _producer(redis_url, queue)
    proc = _spawn(redis_url, queue, "--processes", "2", "--threads", "1", "--shutdown-grace", "5")
    pids = {p for _, p in _round(whoami, 4)}
    assert pids
    proc.kill()
    proc.wait()
    deadline = time.monotonic() + 15
    while any(_alive(p) for p in pids):
        assert time.monotonic() < deadline, "orphaned children kept running"
        time.sleep(0.2)
