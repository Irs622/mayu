import os
import pathlib
import subprocess
import sys
import time

import pytest

from mayu.worker import WorkerRunner

HERE = pathlib.Path(__file__).parent


def _wait_for(predicate, timeout, what):
    deadline = time.monotonic() + timeout
    while not predicate():
        assert time.monotonic() < deadline, f"timed out waiting for {what}"
        time.sleep(0.05)


def test_task_of_killed_worker_is_recovered(app, running, redis_url, tmp_path):
    marker = tmp_path / "started"

    @app.task(name="sample.slow_once")
    def slow_once(path):
        if os.path.exists(path):
            return "recovered"
        open(path, "w").close()
        time.sleep(60)
        return "slow"

    env = {**os.environ, "MAYU_BROKER": redis_url, "MAYU_DEFAULT_QUEUE": app.default_queue,
           "PYTHONPATH": str(HERE)}
    proc = subprocess.Popen(
        [sys.executable, "-m", "mayu", "worker", "sample_app:app", "--mode", "thread", "--threads", "1",
         "--visibility-timeout", "1", "--heartbeat-interval", "0.3"],
        env=env, stderr=subprocess.DEVNULL,
    )
    try:
        handle = slow_once.send(str(marker))
        _wait_for(marker.exists, 15, "the first worker to start the task")
    finally:
        proc.kill()
        proc.wait()
    with running(app, threads=1, visibility_timeout=1, heartbeat_interval=0.3):
        assert handle.result(timeout=15) == "recovered"


def test_long_task_is_not_run_twice(app, running):
    runs = []

    @app.task
    def long_task():
        runs.append(1)
        time.sleep(2.5)
        return "done"

    with running(app, threads=1, visibility_timeout=1), running(app, threads=1, visibility_timeout=1):
        assert long_task.send().result(timeout=10) == "done"
        time.sleep(1.0)  # give the other worker's reclaimer a chance to misbehave
    assert len(runs) == 1


def test_heartbeat_visible_while_running_and_removed_on_stop(app, running):
    with running(app, threads=2, heartbeat_interval=0.2) as w:
        wid = w.worker_id
        assert wid
        _wait_for(lambda: app.client.worker_info(wid) is not None, 5, "heartbeat")
        info = app.client.worker_info(wid)
        assert info["queues"] == [app.default_queue]
        assert info["pid"] == os.getpid()
        assert info["concurrency"] == 2
    assert app.client.worker_info(wid) is None


def test_worker_runner_validates_reliability_options(app):
    with pytest.raises(ValueError):
        WorkerRunner(app, threads=1, visibility_timeout=0)
    with pytest.raises(ValueError):
        WorkerRunner(app, threads=1, heartbeat_interval=-1)
    with pytest.raises(ValueError):
        WorkerRunner(app, threads=1, max_deliveries=0)
    assert WorkerRunner(app, threads=1).worker_id is None
