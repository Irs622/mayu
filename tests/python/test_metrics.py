import os
import pathlib
import signal
import socket
import subprocess
import sys
import time
import urllib.request

import pytest

from mayu import App
from mayu.worker import WorkerRunner

HERE = pathlib.Path(__file__).parent


def _scrape(port: int) -> str:
    with urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=5) as r:
        assert r.status == 200
        assert r.headers["Content-Type"].startswith("text/plain; version=0.0.4")
        return r.read().decode()


def _value(body: str, series: str):
    for line in body.splitlines():
        if line.startswith(series + " "):
            return float(line.rsplit(" ", 1)[1])
    return None


def _free_ports(n: int) -> int:
    """A base port with n consecutive free ports."""
    for _ in range(50):
        with socket.socket() as s:
            s.bind(("127.0.0.1", 0))
            base = s.getsockname()[1]
        if base + n > 65535:
            continue
        socks = []
        try:
            for p in range(base, base + n):
                s = socket.socket()
                socks.append(s)
                s.bind(("127.0.0.1", p))
            return base
        except OSError:
            continue
        finally:
            for s in socks:
                s.close()
    pytest.skip("no run of free ports")


def _wait_scrape(port: int, timeout: float = 20.0) -> str:
    deadline = time.monotonic() + timeout
    while True:
        try:
            return _scrape(port)
        except OSError:
            if time.monotonic() > deadline:
                raise
            time.sleep(0.1)


def _env(redis_url, queue):
    return {**os.environ, "MAYU_BROKER": redis_url, "MAYU_DEFAULT_QUEUE": queue, "PYTHONPATH": str(HERE)}


def _producer(redis_url, queue):
    producer = App(broker=redis_url, result_ttl=60, default_queue=queue)

    @producer.task(name="sample.add")
    def add(a, b):
        pass

    return add


def test_runner_serves_task_metrics(app, running, queue):
    @app.task(name="m.add")
    def add(a, b):
        return a + b

    with running(app, threads=2, metrics_port=0, metrics_host="127.0.0.1") as w:
        assert w.metrics_port and w.metrics_port > 0
        assert [add.send(i, 1).result(timeout=5) for i in range(3)] == [1, 2, 3]
        deadline = time.monotonic() + 5
        while True:
            body = _scrape(w.metrics_port)
            if _value(body, f'mayu_tasks_settled_total{{queue="{queue}",task="m.add",outcome="success"}}') == 3:
                break
            assert time.monotonic() < deadline, body
            time.sleep(0.05)
    assert _value(body, f'mayu_tasks_received_total{{queue="{queue}",task="m.add"}}') == 3
    assert _value(body, f'mayu_task_duration_seconds_count{{queue="{queue}",task="m.add"}}') == 3
    assert _value(body, "mayu_worker_concurrency") == 2


def test_no_metrics_port_by_default(app, running):
    with running(app, threads=1) as w:
        assert w.metrics_port is None


def test_metrics_options_validated(app):
    with pytest.raises(ValueError, match="metrics_port"):
        WorkerRunner(app, threads=1, metrics_port=70000)
    with pytest.raises(ValueError, match="metrics_port"):
        WorkerRunner(app, threads=1, metrics_port=-1)


def test_cli_multi_process_serves_one_port_per_process(redis_url, queue):
    base = _free_ports(2)
    proc = subprocess.Popen(
        [sys.executable, "-m", "mayu", "worker", "sample_app:app", "--mode", "thread", "--threads", "1",
         "--processes", "2", "--metrics-port", str(base), "--metrics-host", "127.0.0.1", "--shutdown-grace", "5"],
        env=_env(redis_url, queue), stderr=subprocess.PIPE, text=True)
    try:
        for port in (base, base + 1):
            assert "mayu_worker_concurrency 1" in _wait_scrape(port)
    finally:
        proc.send_signal(signal.SIGTERM)
        _, err = proc.communicate(timeout=30)
    assert proc.returncode == 0, err


def test_metrics_survive_recycle(redis_url, queue):
    port = _free_ports(1)
    proc = subprocess.Popen(
        [sys.executable, "-m", "mayu", "worker", "sample_app:app", "--mode", "thread", "--threads", "1",
         "--max-tasks-per-process", "2", "--metrics-port", str(port), "--metrics-host", "127.0.0.1"],
        env=_env(redis_url, queue), stderr=subprocess.PIPE, text=True)
    series = f'mayu_tasks_settled_total{{queue="{queue}",task="sample.add",outcome="success"}}'

    def wait_for(pred, what):
        deadline = time.monotonic() + 20
        while True:
            try:
                if pred(_value(_scrape(port), series)):
                    return
            except OSError:
                pass  # between interpreters
            assert time.monotonic() < deadline, what
            time.sleep(0.1)

    try:
        add = _producer(redis_url, queue)
        assert add.send(1, 1).result(timeout=20) == 2
        assert add.send(2, 2).result(timeout=20) == 4   # 2nd task: the interpreter recycles (execv)
        # The old interpreter reported 2; the fresh one starts with no series at all.
        wait_for(lambda v: v is None, "metrics endpoint did not come back after recycling")
        assert add.send(3, 3).result(timeout=20) == 6
        wait_for(lambda v: v == 1, "the re-exec'd interpreter does not count its tasks")
    finally:
        proc.send_signal(signal.SIGTERM)
        _, err = proc.communicate(timeout=30)
    assert proc.returncode == 0, err


def test_cli_rejects_port_zero_with_several_processes(redis_url, queue):
    r = subprocess.run([sys.executable, "-m", "mayu", "worker", "sample_app:app", "--mode", "thread",
                        "--processes", "2", "--metrics-port", "0"],
                       env=_env(redis_url, queue), capture_output=True, text=True, timeout=30)
    assert r.returncode == 2
    assert "--metrics-port" in r.stderr


def test_cli_port_in_use_fails_with_a_clear_message(redis_url, queue):
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        s.listen()
        port = s.getsockname()[1]
        r = subprocess.run([sys.executable, "-m", "mayu", "worker", "sample_app:app", "--mode", "thread",
                            "--threads", "1", "--metrics-port", str(port), "--metrics-host", "127.0.0.1"],
                           env=_env(redis_url, queue), capture_output=True, text=True, timeout=30)
    assert r.returncode == 1
    assert "cannot serve metrics" in r.stderr
    assert "Traceback" not in r.stderr


def test_cli_unresolvable_metrics_host_fails_cleanly(redis_url, queue):
    r = subprocess.run([sys.executable, "-m", "mayu", "worker", "sample_app:app", "--mode", "thread",
                        "--threads", "1", "--metrics-port", "9100", "--metrics-host", "no.such.host.invalid"],
                       env=_env(redis_url, queue), capture_output=True, text=True, timeout=30)
    assert r.returncode == 1
    assert "bad metrics address" in r.stderr
    assert "Traceback" not in r.stderr


def test_cli_bad_metrics_port_env_is_a_usage_error(redis_url, queue):
    r = subprocess.run([sys.executable, "-m", "mayu", "worker", "sample_app:app", "--mode", "thread"],
                       env={**_env(redis_url, queue), "MAYU_METRICS_PORT": "abc"},
                       capture_output=True, text=True, timeout=30)
    assert r.returncode == 2
    assert "--metrics-port" in r.stderr
    assert "Traceback" not in r.stderr
