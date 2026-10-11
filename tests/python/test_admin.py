import json
import os
import pathlib
import subprocess
import sys
import time

import pytest

from mayu import App, TaskFailed
from mayu import admin

HERE = pathlib.Path(__file__).parent


def _env(redis_url, queue):
    return {**os.environ, "MAYU_BROKER": redis_url, "MAYU_DEFAULT_QUEUE": queue, "PYTHONPATH": str(HERE)}


def _cli(redis_url, queue, *args):
    return subprocess.run([sys.executable, "-m", "mayu", *args], env=_env(redis_url, queue),
                          capture_output=True, text=True, timeout=30)


def _wait(cond, timeout=10.0):
    deadline = time.monotonic() + timeout
    while not cond():
        assert time.monotonic() < deadline, "condition not reached"
        time.sleep(0.05)


def test_inspect_reports_depths_and_workers(app, running, queue):
    @app.task
    def boom():
        raise ValueError("x")

    @app.task
    def later():
        pass

    later.send(countdown=60)
    later.send(countdown=60)
    with running(app, threads=1) as w:
        with pytest.raises(TaskFailed):
            boom.send().result(timeout=5)
        data = admin.inspect(app)
        assert data["queues"][queue] == {"ready": 0, "claimed": 0, "delayed": 2, "dead": 1}
        assert w.worker_id in [x["worker_id"] for x in data["workers"]]
        text = admin.format_inspect(data, int(time.time() * 1000))
        assert queue in text and w.worker_id in text
    assert queue in admin.inspect(app, queues=[queue])["queues"]


def test_inspect_cli_json(redis_url, queue):
    r = _cli(redis_url, queue, "inspect", "sample_app:app", "--json")
    assert r.returncode == 0, r.stderr
    data = json.loads(r.stdout)
    assert data["queues"][queue] == {"ready": 0, "claimed": 0, "delayed": 0, "dead": 0}
    assert isinstance(data["workers"], list)


def test_format_inspect_without_workers():
    data = {"queues": {"q": {"ready": 1, "claimed": 2, "delayed": 3, "dead": 4}}, "workers": []}
    text = admin.format_inspect(data, 0)
    assert "QUEUE" in text and "READY" in text and "(none)" in text


def test_dead_list_and_replay_cli_reruns_the_task(redis_url, queue, tmp_path):
    flag = tmp_path / "flag"
    proc = subprocess.Popen([sys.executable, "-m", "mayu", "worker", "sample_app:app", "--mode", "thread",
                             "--threads", "1"], env=_env(redis_url, queue), stderr=subprocess.PIPE, text=True)
    try:
        producer = App(broker=redis_url, result_ttl=60, default_queue=queue)

        @producer.task(name="sample.flaky_file", max_retries=0)
        def flaky_file(path):
            pass

        h = flaky_file.send(str(flag))
        with pytest.raises(TaskFailed):
            h.result(timeout=15)

        r = _cli(redis_url, queue, "dead", "list", "sample_app:app")
        assert r.returncode == 0, r.stderr
        assert h.id in r.stdout and "sample.flaky_file" in r.stdout and "flag file missing" in r.stdout
        listed = json.loads(_cli(redis_url, queue, "dead", "list", "sample_app:app", "--json").stdout)
        assert [e["id"] for e in listed] == [h.id]

        flag.touch()
        r = _cli(redis_url, queue, "dead", "replay", "sample_app:app", "--queue", queue)
        assert r.returncode == 0, r.stderr
        assert "replayed 1" in r.stdout
        assert h.result(timeout=15) == "ok"   # the stored failure was cleared
        assert producer.client.queue_stats(queue)["dead"] == 0
    finally:
        proc.terminate()
        proc.communicate(timeout=30)


def test_replay_does_not_loop_on_tasks_that_fail_again(app, running, queue):
    @app.task(max_retries=0)
    def always_fails():
        raise ValueError("again")

    for _ in range(3):
        always_fails.send()
    with running(app, threads=2):
        _wait(lambda: app.client.queue_stats(queue)["dead"] == 3)
        t = time.monotonic()
        assert admin.dead_replay(app, queue) == (3, 0)
        assert time.monotonic() - t < 10
        _wait(lambda: app.client.queue_stats(queue)["dead"] == 3)  # they failed again, once each


def test_replay_limit_and_ids(app, queue):
    @app.task(max_retries=0)
    def never_runs():
        pass

    ids = [never_runs.send().id for _ in range(3)]
    # dead-letter them without a worker: claim with a native worker and report failures
    from mayu import _native
    w = _native.Worker(app.broker_url, [queue], 3, 1, 60)
    try:
        for _ in range(3):
            t = w.recv(5000)
            w.complete_failure(t.token, "E", "")
        _wait(lambda: app.client.queue_stats(queue)["dead"] == 3)
    finally:
        w.shutdown(1000)
    assert admin.dead_replay(app, queue, ids=[ids[1]]) == (1, 0)
    assert admin.dead_replay(app, queue, limit=1) == (1, 0)
    remaining = [e["id"] for e in admin.dead_list(app, queue, 10)]
    assert len(remaining) == 1 and ids[1] not in remaining


def test_dead_cli_bad_app_exits_2(redis_url, queue):
    r = _cli(redis_url, queue, "dead", "list", "nope_module:app")
    assert r.returncode == 2


def test_dead_cli_limit_below_one_exits_2(redis_url, queue):
    for sub in ("list", "replay"):
        r = _cli(redis_url, queue, "dead", sub, "sample_app:app", "--limit", "0")
        assert r.returncode == 2
        assert "--limit must be >= 1" in r.stderr
