import json
import logging
import os
import pathlib
import signal
import subprocess
import sys
import time

import pytest

from mayu import App, TaskFailed, configure_logging
from mayu.log import JsonFormatter

HERE = pathlib.Path(__file__).parent


def _run_worker(redis_url, queue, *flags, env_extra=None):
    """Runs a worker, sends one failing and one succeeding task, stops it; returns stderr."""
    env = {**os.environ, "MAYU_BROKER": redis_url, "MAYU_DEFAULT_QUEUE": queue, "PYTHONPATH": str(HERE),
           **(env_extra or {})}
    proc = subprocess.Popen([sys.executable, "-m", "mayu", "worker", "sample_app:app", "--mode", "thread",
                             "--threads", "1", *flags], env=env, stderr=subprocess.PIPE, text=True)
    try:
        producer = App(broker=redis_url, result_ttl=60, default_queue=queue)

        @producer.task(name="sample.fail")
        def fail():
            pass

        @producer.task(name="sample.add")
        def add(a, b):
            pass

        h = fail.send()
        with pytest.raises(TaskFailed):
            h.result(timeout=15)
        assert add.send(1, 2).result(timeout=15) == 3
        time.sleep(0.3)  # let the Rust log line for the dead letter flush
    finally:
        proc.send_signal(signal.SIGTERM)
        _, err = proc.communicate(timeout=30)
    assert proc.returncode == 0, err
    return h.id, err


def test_json_logs_are_one_object_per_line(redis_url, queue):
    task_id, err = _run_worker(redis_url, queue, "--log-format", "json")
    records = []
    for line in err.splitlines():
        if line.strip():
            records.append(json.loads(line))  # every line must be JSON
    for r in records:
        assert {"timestamp", "level", "message", "target"} <= r.keys(), r
    started = [r for r in records if r["message"] == "worker started"]
    assert started and started[0]["target"] == "mayu.worker" and started[0]["level"] == "INFO"
    assert started[0]["worker_id"] and started[0]["queues"] == [queue]
    dead = [r for r in records if r["message"] == "task failed; dead-lettered"]
    assert dead, records
    assert dead[0]["task_id"] == task_id and dead[0]["level"] == "WARN" and "sample failure" in dead[0]["error"]
    assert any(r["message"] == "worker stopped" for r in records)


def test_text_logs_by_default(redis_url, queue):
    task_id, err = _run_worker(redis_url, queue)
    assert "mayu: worker started" in err
    dead = [l for l in err.splitlines() if "task failed; dead-lettered" in l]
    assert dead and task_id in dead[0]
    with pytest.raises(json.JSONDecodeError):
        json.loads(err.splitlines()[0])


def test_log_level_filters_rust_and_python(redis_url, queue):
    _, err = _run_worker(redis_url, queue, "--log-level", "ERROR")
    assert "worker started" not in err
    assert "dead-lettered" not in err


def test_env_selects_json(redis_url, queue):
    _, err = _run_worker(redis_url, queue, env_extra={"MAYU_LOG_FORMAT": "json"})
    assert all(json.loads(l) for l in err.splitlines() if l.strip())


def test_json_formatter_shape():
    rec = logging.LogRecord("mayu.worker", logging.WARNING, __file__, 1, "recycling worker: %s", ("x",), None)
    rec.reason = "x"
    out = json.loads(JsonFormatter().format(rec))
    assert out["level"] == "WARN" and out["message"] == "recycling worker: x"
    assert out["target"] == "mayu.worker" and out["reason"] == "x"
    assert out["timestamp"].endswith("Z")
    try:
        raise ValueError("boom")
    except ValueError:
        rec = logging.LogRecord("mayu", logging.ERROR, __file__, 1, "bad", (), sys.exc_info())
    assert "ValueError: boom" in json.loads(JsonFormatter().format(rec))["exception"]


def test_configure_logging_validates():
    with pytest.raises(ValueError):
        configure_logging("xml")
    with pytest.raises(ValueError):
        configure_logging("text", "LOUD")
