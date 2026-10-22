import asyncio
import json
import os
import pathlib
import queue as queue_mod
import signal
import subprocess
import sys
import threading
import time
import urllib.request

import pytest

import mayu
from mayu import admin

HERE = pathlib.Path(__file__).parent
HEARTBEAT = 0.05  # how often the test workers publish; the default is 5 s


def _until(fn, what, timeout=10.0):
    """Polls `fn` until it returns something truthy."""
    deadline = time.monotonic() + timeout
    while True:
        value = fn()
        if value:
            return value
        assert time.monotonic() < deadline, f"timed out waiting for {what}"
        time.sleep(0.02)


def _at(handle, fraction):
    p = handle.progress()
    return p if p is not None and p.fraction == fraction else None


def _scrape(port):
    with urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=5) as r:
        return r.read().decode()


# -- outside a task, and validation (no fixtures: these also run concurrently under ft-stress) ----------------------

def test_outside_a_task_progress_does_nothing():
    assert mayu.progress(1, 2) is False
    assert mayu.progress(0.5) is False
    assert mayu.progress_reporter() is None
    seen = []
    t = threading.Thread(target=lambda: seen.append((mayu.progress(1, 2), mayu.progress_reporter())))
    t.start()
    t.join()
    assert seen == [(False, None)]


@pytest.mark.parametrize("args, error, match", [
    ((float("nan"),), ValueError, "done must be finite"),
    ((float("inf"), 5), ValueError, "done must be finite"),
    ((1, float("inf")), ValueError, "total must be finite"),
    ((1.5,), ValueError, "fraction between 0 and 1, got 1.5; pass total"),
    ((-0.1,), ValueError, "fraction between 0 and 1"),
    ((26, 25), ValueError, r"done must be between 0 and total \(25\), got 26"),
    ((-1, 25), ValueError, "done must be between 0 and total"),
    ((1, 0), ValueError, "total must be greater than 0, got 0"),
    ((1, -3), ValueError, "total must be greater than 0"),
    (("7", 25), TypeError, "done must be a number, got str"),
    ((7, "25"), TypeError, "total must be a number, got str"),
    ((None,), TypeError, "done must be a number, got NoneType"),
    ((True,), TypeError, "done must be a number, got bool"),
    ((1, True), TypeError, "total must be a number, got bool"),
])
def test_progress_validates_its_arguments_even_outside_a_task(args, error, match):
    with pytest.raises(error, match=match):
        mayu.progress(*args)


def test_progress_accepts_the_boundaries():
    for args in [(0,), (1,), (0.5,), (0, 10), (10, 10), (2.5, 5.0)]:
        assert mayu.progress(*args) is False  # valid, but not in a task


# -- inside tasks -------------------------------------------------------------------------------------------------

def test_handle_sees_each_report_then_none_once_done(app, running):
    go = queue_mod.Queue()
    reached = queue_mod.Queue()

    @app.task(name="p.steps")
    def steps():
        go.get(timeout=10)  # running, but nothing reported yet
        for i in range(1, 5):
            assert mayu.progress(i, 4) is True
            reached.put(i)
            go.get(timeout=10)
        return "ok"

    with running(app, threads=2, heartbeat_interval=HEARTBEAT) as w:
        h = steps.send()
        p = _until(h.progress, "the running task to be published")
        assert (p.done, p.total, p.fraction, p.remaining) == (None, None, None, None)
        assert p.worker == w.worker_id
        assert abs(p.updated - time.time()) < 30
        for i in range(1, 5):
            go.put(1)
            assert reached.get(timeout=10) == i
            p = _until(lambda: _at(h, i / 4), f"progress {i}/4")
            assert (p.done, p.total, p.worker) == (i, 4, w.worker_id)
        go.put(1)
        assert h.result(timeout=10) == "ok"
        _until(lambda: h.progress() is None, "the finished task to leave the published list")
        assert h.progress() is None


def test_a_task_that_is_not_running_has_no_progress(app, running):
    @app.task(name="p.quick")
    def quick():
        return 1

    with running(app, threads=1, heartbeat_interval=HEARTBEAT):
        h = quick.send()
        assert h.result(timeout=10) == 1
        assert h.progress() is None
    assert mayu.TaskHandle(app, "no-such-task").progress() is None


def test_a_bare_fraction_is_reported(app, running):
    go, reached = threading.Event(), threading.Event()

    @app.task(name="p.fraction")
    def fraction():
        mayu.progress(0.4)
        reached.set()
        go.wait(10)

    with running(app, threads=1, heartbeat_interval=HEARTBEAT):
        h = fraction.send()
        assert reached.wait(10)
        p = _until(lambda: _at(h, 0.4), "the fraction")
        assert (p.done, p.total) == (0.4, None)
        go.set()
        h.result(timeout=10)


def test_reporter_works_from_threads_the_task_starts_and_is_a_no_op_after(app, running):
    out, reporters = [], []

    @app.task(name="p.threads")
    def threads():
        report = mayu.progress_reporter()
        reporters.append(report)
        assert report is not None

        def child():
            out.append(("reporter", report(1, 2)))
            out.append(("bare", mayu.progress(1, 2)))  # a thread-local lookup doesn't follow the child thread
            try:
                report(3, 2)
            except ValueError as e:
                out.append(("invalid", str(e)))

        t = threading.Thread(target=child)
        t.start()
        t.join()
        return "ok"

    with running(app, threads=1, heartbeat_interval=HEARTBEAT):
        assert threads.send().result(timeout=10) == "ok"
        assert out[0] == ("reporter", True) and out[1] == ("bare", False)
        assert out[2][0] == "invalid" and "done must be between 0 and total (2)" in out[2][1]
        assert reporters[0](2, 2) is False  # the task is over
        # a later task on the same worker gets a different token, so the old reporter can't touch it
        go = threading.Event()

        @app.task(name="p.later")
        def later():
            go.wait(10)

        h = later.send()
        _until(h.progress, "the second task to run")
        assert reporters[0](1, 2) is False
        assert h.progress().fraction is None
        go.set()
        h.result(timeout=10)


def test_async_tasks_report_and_stay_apart(app, running):
    ready = threading.Barrier(3, timeout=10)
    go = threading.Event()

    @app.task(name="p.async")
    async def work(total):
        assert mayu.progress(1, total) is True
        report = mayu.progress_reporter()
        await asyncio.to_thread(ready.wait)  # never block the loop: the other task has to start too
        await asyncio.to_thread(go.wait, 10)  # the context follows to_thread, so progress works from there too
        assert mayu.progress(total, total) is True
        assert report(total, total) is True
        return total

    with running(app, threads=1, heartbeat_interval=HEARTBEAT):
        a, b = work.send(4), work.send(10)
        ready.wait()
        pa = _until(lambda: _at(a, 0.25), "async task a")
        pb = _until(lambda: _at(b, 0.1), "async task b")
        assert (pa.total, pb.total) == (4, 10)
        go.set()
        assert (a.result(timeout=10), b.result(timeout=10)) == (4, 10)


def test_serial_tasks_report(app, running):
    go, reached = threading.Event(), threading.Event()

    @app.task(name="p.serial", threadsafe=False)
    def legacy():
        mayu.progress(3, 12)
        reached.set()
        go.wait(10)

    with running(app, threads=2, heartbeat_interval=HEARTBEAT):
        h = legacy.send()
        assert reached.wait(10)
        p = _until(lambda: _at(h, 0.25), "the serial task's progress")
        assert (p.done, p.total) == (3, 12)
        go.set()
        h.result(timeout=10)


def test_progress_survives_a_task_that_fails(app, running):
    @app.task(name="p.fails", max_retries=0)
    def fails():
        mayu.progress(1, 2)
        raise RuntimeError("boom")

    @app.task(name="p.after")
    def after():
        return mayu.progress(1, 2)

    with running(app, threads=1, heartbeat_interval=HEARTBEAT):
        with pytest.raises(mayu.TaskFailed):
            fails.send().result(timeout=10)
        assert after.send().result(timeout=10) is True


def test_concurrent_tasks_and_threads_report_without_crosstalk(app, running):
    n_tasks, n_threads, steps = 4, 3, 40
    go = threading.Event()

    @app.task(name="p.busy")
    def busy(total):
        report = mayu.progress_reporter()
        lock = threading.Lock()
        counter = [0]

        def child():
            for _ in range(steps):
                with lock:  # one writer at a time, so what is published only ever grows
                    counter[0] += 1
                    assert report(counter[0], total) is True

        workers = [threading.Thread(target=child) for _ in range(n_threads)]
        for t in workers:
            t.start()
        for t in workers:
            t.join()
        go.wait(10)

    totals = [n_threads * steps + i for i in range(n_tasks)]  # distinct totals identify the task
    with running(app, threads=n_tasks, heartbeat_interval=HEARTBEAT):
        handles = [busy.send(t) for t in totals]
        last = [-1.0] * n_tasks
        deadline = time.monotonic() + 15
        while any(x < n_threads * steps for x in last):
            assert time.monotonic() < deadline, f"progress never completed: {last}"
            for i, h in enumerate(handles):
                p = h.progress()
                if p is not None and p.done is not None:
                    assert p.total == totals[i], "a report landed on another task"
                    assert p.done >= last[i], "published progress went backwards"
                    last[i] = p.done
            time.sleep(0.01)
        go.set()
        for h in handles:
            h.result(timeout=10)


def test_reports_are_counted_in_metrics(app, running, queue):
    @app.task(name="p.counted")
    def counted():
        for i in range(1, 4):
            mayu.progress(i, 3)
        with pytest.raises(ValueError):
            mayu.progress(9, 3)  # rejected before it reaches the worker: not counted

    series = f'mayu_task_progress_reports_total{{queue="{queue}",task="p.counted"}}'
    with running(app, threads=1, metrics_port=0, metrics_host="127.0.0.1") as w:
        assert series not in _scrape(w.metrics_port)  # nothing is emitted until a task reports
        counted.send().result(timeout=10)
        body = _scrape(w.metrics_port)
    assert f"{series} 3" in body.splitlines()
    assert "# TYPE mayu_task_progress_reports_total counter" in body


# -- mayu inspect ---------------------------------------------------------------------------------------------------

def _cli(redis_url, queue, *args):
    env = {**os.environ, "MAYU_BROKER": redis_url, "MAYU_DEFAULT_QUEUE": queue, "PYTHONPATH": str(HERE)}
    return subprocess.run([sys.executable, "-m", "mayu", *args], env=env, capture_output=True, text=True, timeout=30)


def test_inspect_shows_the_running_task_and_its_progress(app, running, redis_url, queue):
    go, reached = threading.Event(), threading.Event()

    @app.task(name="p.inspected")
    def inspected():
        mayu.progress(7, 25)
        reached.set()
        go.wait(10)

    with running(app, threads=1, heartbeat_interval=HEARTBEAT) as w:
        h = inspected.send()
        assert reached.wait(10)
        _until(lambda: _at(h, 7 / 25), "the report to be published")

        data = admin.inspect(app)
        mine = [t for t in data["running"] if t["id"] == h.id]
        assert len(mine) == 1
        assert mine[0]["task"] == "p.inspected" and mine[0]["worker"] == w.worker_id
        assert (mine[0]["done"], mine[0]["total"], mine[0]["fraction"]) == (7, 25, 7 / 25)
        assert mine[0]["workflow_id"] is None and mine[0]["started_ms"] > 0
        text = admin.format_inspect(data, int(time.time() * 1000))
        assert "RUNNING (" in text
        row = next(line for line in text.splitlines() if h.id in line)
        assert row.split()[:5] == ["p.inspected", h.id, w.worker_id, "7/25", "(28"]

        r = _cli(redis_url, queue, "inspect", "sample_app:app", "--json")
        assert r.returncode == 0, r.stderr
        assert [t for t in json.loads(r.stdout)["running"] if t["id"] == h.id][0]["fraction"] == 7 / 25
        r = _cli(redis_url, queue, "inspect", "sample_app:app")
        assert r.returncode == 0, r.stderr
        assert "RUNNING (" in r.stdout and h.id in r.stdout and "7/25 (28 %)" in r.stdout
        go.set()
        h.result(timeout=10)


def test_format_inspect_running_section():
    base = {"queues": {"q": {"ready": 0, "claimed": 0, "delayed": 0, "dead": 0}}, "workers": [], "workflows": []}
    row = {"task": "voice", "id": "01A", "worker": "w1", "done": 7.0, "total": 25.0, "fraction": 0.28,
           "remaining_s": 250.0, "workflow_id": "01WF", "node": "voice", "started_ms": 1}
    text = admin.format_inspect({**base, "running": [
        row,
        {**row, "id": "01B", "done": 0.4, "total": None, "fraction": 0.4, "remaining_s": None, "workflow_id": None},
        {**row, "id": "01C", "done": None, "total": None, "fraction": None, "remaining_s": None},
    ]}, 0)
    lines = text.splitlines()
    i = lines.index("RUNNING (3)")
    assert lines[i + 1].split() == ["TASK", "ID", "WORKER", "PROGRESS", "LEFT", "WORKFLOW"]
    assert lines[i + 2].split() == ["voice", "01A", "w1", "7/25", "(28", "%)", "~4m10s", "01WF"]
    assert lines[i + 3].split() == ["voice", "01B", "w1", "40", "%", "-", "-"]
    assert lines[i + 4].split() == ["voice", "01C", "w1", "-", "-", "01WF"]
    assert lines.index("WORKFLOWS (0)") > i  # between the workers and the workflows


def test_idle_inspect_text_has_no_running_section():
    base = {"queues": {"q": {"ready": 1, "claimed": 0, "delayed": 0, "dead": 0}}, "workers": [], "workflows": []}
    idle = admin.format_inspect({**base, "running": []}, 0)
    assert "RUNNING" not in idle
    assert idle == admin.format_inspect(base, 0)  # byte-identical to what an older data dict gives


# -- process mode ---------------------------------------------------------------------------------------------------

def test_process_mode_reports_progress(redis_url, queue, tmp_path):
    from mayu import App

    producer = App(broker=redis_url, result_ttl=60, default_queue=queue)

    @producer.task(name="sample.progressing")
    def progressing(path):
        pass

    flag = tmp_path / "go"
    env = {**os.environ, "MAYU_BROKER": redis_url, "MAYU_DEFAULT_QUEUE": queue, "PYTHONPATH": str(HERE)}
    proc = subprocess.Popen([sys.executable, "-m", "mayu", "worker", "sample_app:app", "--mode", "process",
                             "--processes", "1", "--heartbeat-interval", str(HEARTBEAT), "--shutdown-grace", "5"],
                            env=env, stderr=subprocess.DEVNULL)
    try:
        h = progressing.send(str(flag))
        p = _until(lambda: _at(h, 0.5), "progress from the process-mode child", timeout=30)
        assert (p.done, p.total) == (1, 2)
        flag.touch()
        assert h.result(timeout=30) == "done"
        _until(lambda: h.progress() is None, "the finished task to leave the published list")
    finally:
        proc.send_signal(signal.SIGTERM)
        try:
            proc.wait(timeout=25)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
