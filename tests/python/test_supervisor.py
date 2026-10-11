import os
import signal
import sys
import threading
import time

import pytest

from mayu.supervisor import RESTART_EXIT_CODE, ProcessSupervisor, watch_parent

SLEEPER = [sys.executable, "-c", "import time; time.sleep(60)"]


def _wait_for(predicate, timeout, what):
    deadline = time.monotonic() + timeout
    while not predicate():
        assert time.monotonic() < deadline, f"timed out waiting for {what}"
        time.sleep(0.05)


def _start(sup):
    out = []
    th = threading.Thread(target=lambda: out.append(sup.run()))
    th.start()
    return th, out


def _alive(pid):
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


def test_rejects_zero_processes():
    with pytest.raises(ValueError):
        ProcessSupervisor(SLEEPER, 0)


def test_keeps_children_running_and_restarts_killed_ones():
    sup = ProcessSupervisor(SLEEPER, 2, shutdown_grace=1, min_backoff=0.1)
    th, out = _start(sup)
    try:
        _wait_for(lambda: len(sup.pids()) == 2, 10, "two children")
        first = set(sup.pids())
        os.kill(next(iter(first)), signal.SIGKILL)
        _wait_for(lambda: sup.restarts >= 1 and len(sup.pids()) == 2, 10, "a replacement")
        assert set(sup.pids()) != first
    finally:
        sup.stop()
        th.join(15)
    assert out == [0]
    assert not any(_alive(p) for p in first)


def test_crash_loop_backs_off():
    crasher = [sys.executable, "-c", "import sys; sys.exit(1)"]
    sup = ProcessSupervisor(crasher, 1, min_backoff=0.2, max_backoff=0.8)
    th, _ = _start(sup)
    time.sleep(2.5)
    sup.stop()
    th.join(10)
    assert 3 <= sup.restarts <= 10, f"{sup.restarts} restarts: backoff not applied"


def test_restart_code_respawns_immediately():
    recycler = [sys.executable, "-c", f"import sys, time; time.sleep(0.2); sys.exit({RESTART_EXIT_CODE})"]
    sup = ProcessSupervisor(recycler, 1, min_backoff=10)
    th, _ = _start(sup)
    time.sleep(2.5)
    sup.stop()
    th.join(10)
    assert sup.restarts >= 3, "exit 75 must not be backed off"


def test_shutdown_forwards_sigterm(tmp_path):
    child = [sys.executable, "-c", (
        "import os, signal, sys, time, pathlib\n"
        f"d = pathlib.Path({str(tmp_path)!r})\n"
        "def bye(*_):\n"
        "    (d / str(os.getpid())).write_text(os.environ['MAYU_PARENT_PID'])\n"
        "    sys.exit(0)\n"
        "signal.signal(signal.SIGTERM, bye)\n"
        "time.sleep(60)\n"
    )]
    sup = ProcessSupervisor(child, 2, shutdown_grace=5)
    th, out = _start(sup)
    _wait_for(lambda: len(sup.pids()) == 2, 10, "two children")
    time.sleep(0.5)  # let the children install their handlers
    pids = sup.pids()
    sup.stop()
    th.join(15)
    assert out == [0]
    for pid in pids:
        assert (tmp_path / str(pid)).read_text() == str(os.getpid())


def test_shutdown_kills_children_that_ignore_sigterm():
    stubborn = [sys.executable, "-c",
                "import signal, time; signal.signal(signal.SIGTERM, signal.SIG_IGN); time.sleep(60)"]
    sup = ProcessSupervisor(stubborn, 1, shutdown_grace=0.5)
    th, _ = _start(sup)
    _wait_for(lambda: len(sup.pids()) == 1, 10, "a child")
    time.sleep(0.5)
    pid = sup.pids()[0]
    t = time.monotonic()
    sup.stop()
    th.join(20)
    assert not th.is_alive()
    assert time.monotonic() - t < 10
    assert not _alive(pid)


def test_watch_parent_fires_when_orphaned(monkeypatch):
    monkeypatch.setenv("MAYU_PARENT_PID", str(os.getppid() + 987654))
    fired = threading.Event()
    assert watch_parent(fired.set, interval=0.05) is not None
    assert fired.wait(2)


def test_watch_parent_quiet_while_parent_lives(monkeypatch):
    monkeypatch.setenv("MAYU_PARENT_PID", str(os.getppid()))
    fired = threading.Event()
    watch_parent(fired.set, interval=0.05)
    assert not fired.wait(0.4)


def test_watch_parent_noop_without_env(monkeypatch):
    monkeypatch.delenv("MAYU_PARENT_PID", raising=False)
    assert watch_parent(lambda: None) is None
