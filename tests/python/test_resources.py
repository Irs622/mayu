import threading
import urllib.request
import time

import pytest

from mayu.cli import parse_resources
from mayu.worker import WorkerRunner


class Gauge:
    """Counts tasks running right now and the most there ever were at once."""

    def __init__(self):
        self.lock = threading.Lock()
        self.now = 0
        self.max = 0
        self.started = 0

    def enter(self):
        with self.lock:
            self.now += 1
            self.started += 1
            self.max = max(self.max, self.now)

    def leave(self):
        with self.lock:
            self.now -= 1

    def wait_for(self, predicate, timeout=10.0):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            with self.lock:
                if predicate(self):
                    return True
            time.sleep(0.005)  # bounded poll on the counter; nothing to wait on from outside the tasks
        return False


def test_tasks_run_side_by_side_up_to_the_declared_capacity(app, running):
    gauge, gate = Gauge(), threading.Event()

    @app.task(resources={"gpu": 0.5})
    def work():
        gauge.enter()
        try:
            gate.wait(20)
        finally:
            gauge.leave()

    with running(app, threads=4, resources={"gpu": 1}):
        handles = [work.send() for _ in range(4)]
        try:
            assert gauge.wait_for(lambda g: g.now == 2), "two half-gpu tasks should run together"
            # Give a third task the chance to (wrongly) start before asserting it did not.
            assert not gauge.wait_for(lambda g: g.now > 2, timeout=0.3)
        finally:
            gate.set()
        for h in handles:
            h.result(timeout=10)
    assert gauge.max == 2
    assert gauge.started == 4


def test_a_whole_gpu_task_waits_for_the_half_gpu_tasks(app, running):
    gauge, gate = Gauge(), threading.Event()
    order = []

    @app.task(resources={"gpu": 0.5})
    def half():
        gauge.enter()
        try:
            gate.wait(20)
            order.append("half")
        finally:
            gauge.leave()

    @app.task(resources={"gpu": 1})
    def whole():
        order.append(f"whole@{gauge.now}")

    with running(app, threads=4, resources={"gpu": 1}):
        halves = [half.send(), half.send()]
        assert gauge.wait_for(lambda g: g.now == 2)
        big = whole.send()
        assert not gauge.wait_for(lambda g: g.started > 2, timeout=0.3)
        assert "whole@0" not in order
        gate.set()
        big.result(timeout=10)
        for h in halves:
            h.result(timeout=10)
    assert order[-1] == "whole@0", order


def test_tasks_without_resources_run_while_the_gpu_is_taken(app, running):
    gate = threading.Event()
    started = threading.Event()

    @app.task(resources={"gpu": 1})
    def hog():
        started.set()
        gate.wait(20)

    @app.task
    def free():
        return "ok"

    with running(app, threads=2, resources={"gpu": 1}):
        h = hog.send()
        assert started.wait(10)
        try:
            assert free.send().result(timeout=10) == "ok"
        finally:
            gate.set()
        h.result(timeout=10)


def test_runner_exposes_what_it_validated(app):
    @app.task(resources={"gpu": 0.5, "cpu": 2})
    def f():
        pass

    w = WorkerRunner(app, threads=1, resources={"gpu": 1, "cpu": 4})
    assert w.resources == {"gpu": 1.0, "cpu": 4.0}
    assert w.task_resources == {f.name: {"gpu": 0.5, "cpu": 2.0}}


def test_request_for_an_undeclared_resource_is_refused(app):
    @app.task(name="x", resources={"gpu": 1})
    def x():
        pass

    for resources in (None, {"cpu": 4}):
        with pytest.raises(ValueError) as e:
            WorkerRunner(app, threads=1, resources=resources)
        assert str(e.value) == ("task 'x' requests 'gpu' but this worker declares no such resource; "
                                "pass --resources gpu=...")


def test_request_over_capacity_is_refused(app):
    @app.task(name="x", resources={"vram_gb": 8})
    def x():
        pass

    with pytest.raises(ValueError) as e:
        WorkerRunner(app, threads=1, resources={"vram_gb": 7.5})
    assert str(e.value) == ("task 'x' requests vram_gb=8 but this worker has vram_gb=7.5; "
                            "raise --resources vram_gb=... or route the task to a bigger worker")


def test_request_equal_to_capacity_is_accepted(app):
    @app.task(resources={"vram_gb": 7.5})
    def x():
        pass

    WorkerRunner(app, threads=1, resources={"vram_gb": 7.5})


def test_tasks_on_unserved_queues_are_ignored(app, queue):
    @app.task(queue=f"{queue}-other", resources={"gpu": 1})
    def elsewhere():
        pass

    @app.task
    def here():
        pass

    w = WorkerRunner(app, threads=1, queues=[queue])
    assert w.task_resources == {}
    with pytest.raises(ValueError, match="--resources"):
        WorkerRunner(app, threads=1, queues=[f"{queue}-other"])


@pytest.mark.parametrize("bad,exc", [
    ([("gpu", 1)], TypeError),
    ("gpu=1", TypeError),
    ({1: 1}, TypeError),
    ({"": 1}, ValueError),
    ({"gpu": "1"}, TypeError),
    ({"gpu": True}, TypeError),
    ({"gpu": 0}, ValueError),
    ({"gpu": -1}, ValueError),
    ({"gpu": 0.0004}, ValueError),
    ({"gpu": float("nan")}, ValueError),
    ({"gpu": float("inf")}, ValueError),
])
def test_bad_resources_shapes_are_rejected_at_decoration(app, bad, exc):
    with pytest.raises(exc, match="resources of task"):
        @app.task(resources=bad)
        def f():
            pass


def test_resources_on_declared_tasks_and_default(app):
    @app.task
    def plain():
        pass

    stub = app.declare(name="remote", resources={"gpu": 0.25})(lambda: None)
    assert plain.resources == {}
    assert stub.resources == {"gpu": 0.25}


def test_worker_capacity_shape_is_checked(app):
    with pytest.raises(ValueError):
        WorkerRunner(app, threads=1, resources={"gpu": -1})
    with pytest.raises(TypeError):
        WorkerRunner(app, threads=1, resources=["gpu"])
    WorkerRunner(app, threads=1, resources={"gpu": 0})


@pytest.mark.parametrize("text,expected", [
    (None, None), ("", None), ("gpu=1", {"gpu": 1.0}), ("gpu=0", {"gpu": 0.0}),
    ("gpu=1,vram_gb=7.5, cpu = 12", {"gpu": 1.0, "vram_gb": 7.5, "cpu": 12.0}),
])
def test_parse_resources(text, expected):
    assert parse_resources(text) == expected


@pytest.mark.parametrize("text", [
    "gpu", "=1", "gpu=", "gpu=x", "gpu=-1", "gpu=nan", "gpu=inf", "gpu=1,,cpu=2", "gpu=1,gpu=2",
])
def test_parse_resources_rejects_malformed_input(text):
    with pytest.raises(ValueError, match="--resources"):
        parse_resources(text)


def _metrics(port: int) -> str:
    with urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=5) as r:
        return r.read().decode()


def _gauge(body: str, series: str):
    for line in body.splitlines():
        if line.startswith(series + " "):
            return float(line.rsplit(" ", 1)[1])
    return None


def test_metrics_report_resource_capacity_and_use(app, running):
    gate, started = threading.Event(), threading.Event()

    @app.task(resources={"gpu": 0.5, "vram_gb": 1.5})
    def work():
        started.set()
        gate.wait(20)

    with running(app, threads=2, resources={"gpu": 1, "vram_gb": 7.5}, metrics_port=0,
                 metrics_host="127.0.0.1") as w:
        body = _metrics(w.metrics_port)
        assert _gauge(body, 'mayu_resource_capacity{resource="gpu"}') == 1.0
        assert _gauge(body, 'mayu_resource_capacity{resource="vram_gb"}') == 7.5
        assert _gauge(body, 'mayu_resource_in_use{resource="gpu"}') == 0.0
        h = work.send()
        try:
            assert started.wait(10)
            body = _metrics(w.metrics_port)
            assert _gauge(body, 'mayu_resource_in_use{resource="gpu"}') == 0.5
            assert _gauge(body, 'mayu_resource_in_use{resource="vram_gb"}') == 1.5
        finally:
            gate.set()
        h.result(timeout=10)
        deadline = time.monotonic() + 5
        while _gauge(_metrics(w.metrics_port), 'mayu_resource_in_use{resource="gpu"}') != 0.0:
            assert time.monotonic() < deadline, "resources were not released"
            time.sleep(0.01)


def test_metrics_have_no_resource_gauges_without_resources(app, running):
    with running(app, threads=1, metrics_port=0, metrics_host="127.0.0.1") as w:
        assert "mayu_resource" not in _metrics(w.metrics_port)
