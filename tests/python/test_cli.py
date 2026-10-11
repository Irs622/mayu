import os
import pathlib
import signal
import subprocess
import sys

import pytest

from mayu import App
from mayu.cli import load_app

HERE = pathlib.Path(__file__).parent


def _env(redis_url, queue):
    return {**os.environ, "MAYU_BROKER": redis_url, "MAYU_DEFAULT_QUEUE": queue,
            "PYTHONPATH": str(HERE)}


def test_worker_cli_runs_tasks_and_exits_cleanly_on_sigterm(redis_url, queue):
    proc = subprocess.Popen(
        [sys.executable, "-m", "mayu", "worker", "sample_app:app", "--threads", "2"],
        env=_env(redis_url, queue), stderr=subprocess.PIPE, text=True,
    )
    try:
        producer = App(broker=redis_url, result_ttl=60, default_queue=queue)

        @producer.task(name="sample.add")
        def add(a, b):
            pass

        assert add.send(20, 22).result(timeout=15) == 42
    finally:
        proc.send_signal(signal.SIGTERM)
        _, err = proc.communicate(timeout=15)
    assert proc.returncode == 0, err


def test_bad_app_path_exits_2(redis_url, queue):
    r = subprocess.run([sys.executable, "-m", "mayu", "worker", "nope_module:app"],
                       env=_env(redis_url, queue), capture_output=True, text=True, timeout=30)
    assert r.returncode == 2
    assert "nope_module" in r.stderr


def test_load_app_rejects_non_app(monkeypatch, redis_url, queue):
    monkeypatch.syspath_prepend(str(HERE))
    monkeypatch.setenv("MAYU_BROKER", redis_url)
    with pytest.raises(TypeError):
        load_app("sample_app:add")
    with pytest.raises(ValueError):
        load_app("sample_app")
