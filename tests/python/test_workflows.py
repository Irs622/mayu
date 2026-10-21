import threading
import time

import pytest

from mayu import App, Retry, TaskFailed, Workflow, WorkflowHandle, admin
from mayu.worker import WorkerRunner


def _wait(pred, timeout=5.0):
    deadline = time.monotonic() + timeout
    while not pred():
        assert time.monotonic() < deadline, "condition not reached in time"
        time.sleep(0.02)


def test_diamond_runs_in_dependency_order(app, running):
    lock = threading.Lock()
    order = []

    def mark(name):
        with lock:
            order.append(name)
            return list(order)

    @app.task
    def plan_run(prompt, run):
        mark(f"plan:{prompt}:{run}")

    @app.task
    def voice_lines(run):
        mark("voice")

    @app.task
    def render_scene(run, scene):
        mark(f"scene:{scene}")

    @app.task
    def finalize(run):
        return mark("finalize")

    wf = app.workflow("video:42")
    plan = wf.add(plan_run.s("a prompt", run=7))
    voice = wf.add(voice_lines.s(7), after=[plan])
    scenes = [wf.add(render_scene.s(7, s), after=voice) for s in ("S1", "S2", "S3")]
    last = wf.add(finalize.s(7), after=scenes)
    with running(app):
        handle = wf.send()
        assert isinstance(handle, WorkflowHandle)
        assert len(handle.nodes) == 6
        seen = handle.node(last).result(timeout=10)
        assert handle.wait(timeout=10) == "done"
    assert seen[:2] == ["plan:a prompt:7", "voice"]
    assert sorted(seen[2:5]) == ["scene:S1", "scene:S2", "scene:S3"]
    assert seen[5] == "finalize"
    assert handle.nodes[-1].id == handle.node(last).id


def test_fan_out_children_run_in_parallel(app, running):
    barrier = threading.Barrier(3, timeout=5)

    @app.task
    def root():
        return "root"

    @app.task
    def child(i):
        barrier.wait()  # only passes when all three run at the same time
        return i

    wf = app.workflow("fan")
    r = wf.add(root)
    for i in range(3):
        wf.add(child.s(i), after=r)
    with running(app, threads=4):
        handle = wf.send()
        assert [h.result(timeout=10) for h in handle.nodes[1:]] == [0, 1, 2]
        assert handle.wait(timeout=10) == "done"


def test_status_counts_follow_the_nodes(app, running):
    gate = threading.Event()

    @app.task
    def first():
        assert gate.wait(timeout=10)

    @app.task
    def second():
        return 1

    wf = app.workflow("counts", ttl=120)
    a = wf.add(first)
    wf.add(second, after=a)
    try:
        with running(app):
            handle = wf.send()
            s = handle.status()
            assert (s["id"], s["name"], s["state"]) == (handle.id, "counts", "running")
            assert (s["waiting"], s["queued"], s["done"], s["dead"]) == (1, 1, 0, 0)
            assert [(n["node"], n["task"], n["state"]) for n in s["nodes"]] == [
                ("0", first.name, "queued"), ("1", second.name, "waiting")]
            assert [n["id"] for n in s["nodes"]] == [h.id for h in handle.nodes]
            with pytest.raises(TimeoutError):
                handle.wait(timeout=0.2)
            gate.set()
            assert handle.wait(timeout=10) == "done"
            s = handle.status()
            assert (s["waiting"], s["queued"], s["done"], s["dead"], s["cancelled"]) == (0, 0, 2, 0, False)
    finally:
        gate.set()


def test_failed_node_blocks_its_child(app, running):
    ran = []

    @app.task(max_retries=0)
    def boom():
        raise ValueError("nope")

    @app.task
    def never():
        ran.append(1)

    wf = app.workflow("failing")
    b = wf.add(boom)
    wf.add(never, after=b)
    with running(app):
        handle = wf.send()
        assert handle.wait(timeout=10) == "failed"
        with pytest.raises(TaskFailed, match="nope"):
            handle.nodes[0].result(timeout=5)
    s = handle.status()
    assert [n["state"] for n in s["nodes"]] == ["dead", "waiting"]
    assert (s["dead"], s["waiting"]) == (1, 1)
    assert ran == []  # the node is marked dead in the dead-letter script, so nothing releases the child later


def test_cancel_before_any_worker_runs_skips_every_node(app, running):
    ran = []

    @app.task
    def work():
        ran.append("work")

    @app.task
    def more():
        ran.append("more")

    wf = app.workflow("cancelled")
    a = wf.add(work)
    wf.add(more, after=a)
    handle = wf.send()
    assert handle.cancel() is True
    assert handle.status()["state"] == "cancelled"
    with running(app):
        assert handle.wait(timeout=5) == "cancelled"
        with pytest.raises(TaskFailed, match="workflow cancelled"):
            handle.nodes[0].result(timeout=10)
        _wait(lambda: handle.status()["nodes"][0]["state"] == "cancelled")
    assert [n["state"] for n in handle.status()["nodes"]] == ["cancelled", "waiting"]
    assert ran == []
    assert handle.cancel() is True  # a cancelled workflow can be cancelled again; nothing changes


def test_cancel_stops_the_rest_after_a_node_finishes(app, running):
    gate = threading.Event()
    ran = []

    @app.task
    def head():
        assert gate.wait(timeout=10)
        ran.append("head")

    @app.task
    def tail():
        ran.append("tail")

    wf = app.workflow("midway")
    h = wf.add(head)
    wf.add(tail, after=h)
    try:
        with running(app):
            handle = wf.send()
            _wait(lambda: handle.status()["queued"] == 1 and app.client.queue_stats(app.default_queue)["pending"] == 1)
            assert handle.cancel() is True
            gate.set()  # head was already running: it finishes, but releases nothing
            _wait(lambda: handle.status()["nodes"][0]["state"] == "cancelled")
            assert handle.wait(timeout=5) == "cancelled"
    finally:
        gate.set()
    assert ran == ["head"]
    assert handle.status()["nodes"][1]["state"] == "waiting"


def test_cancel_a_finished_or_unknown_workflow_is_false(app, running):
    @app.task
    def one():
        return 1

    wf = app.workflow("finished")
    wf.add(one)
    with running(app):
        handle = wf.send()
        assert handle.wait(timeout=10) == "done"
    assert handle.cancel() is False
    gone = WorkflowHandle(app, "01JZZZZZZZZZZZZZZZZZZZZZZZ", [])
    assert gone.cancel() is False
    assert gone.status()["state"] == "expired"
    assert gone.wait(timeout=1) == "expired"


def test_validation_errors(app, redis_url):
    @app.task
    def two(a, b):
        return a + b

    @app.task
    def plain():
        pass

    other_app = App(broker=redis_url)

    @other_app.task(name="elsewhere")
    def elsewhere():
        pass

    wf = app.workflow("v")
    with pytest.raises(TypeError, match=r"two.*workflow node"):
        wf.add(two.s(1))
    with pytest.raises(TypeError, match="two"):
        wf.add(two)
    with pytest.raises(TypeError, match="task or task.s"):
        wf.add("not a task")
    with pytest.raises(ValueError, match="different App"):
        wf.add(elsewhere)
    with pytest.raises(ValueError, match="at least one task"):
        wf.send()

    node = wf.add(two.s(1, 2))
    other_wf = app.workflow("other")
    foreign = other_wf.add(plain)
    with pytest.raises(ValueError, match="different workflow"):
        wf.add(plain, after=[foreign])
    with pytest.raises(TypeError, match="after must be"):
        wf.add(plain, after=["nope"])
    with pytest.raises(TypeError, match="after must be"):
        wf.add(plain, after=3)
    assert wf.add(plain, after=[node, node]).index == 1  # a repeated parent counts once

    with pytest.raises(ValueError, match="positive"):
        app.workflow("t", ttl=0)
    with pytest.raises(ValueError, match="positive"):
        app.workflow("t", ttl=-5)
    with pytest.raises(ValueError, match="positive"):
        app.workflow("t", ttl=float("inf"))
    with pytest.raises(TypeError, match="ttl"):
        app.workflow("t", ttl="soon")
    with pytest.raises(TypeError, match="name"):
        app.workflow(3)
    assert isinstance(app.workflow("ok", ttl=0.5), Workflow)

    handle = wf.send()
    with pytest.raises(ValueError, match="already sent"):
        wf.send()
    with pytest.raises(ValueError, match="already sent"):
        wf.add(plain)
    with pytest.raises(ValueError, match="not a node"):
        handle.node(foreign)
    with pytest.raises(ValueError, match="not a node"):
        handle.node("0")


def test_unserializable_arguments_leave_the_workflow_unsent(app):
    @app.task
    def take(x):
        pass

    wf = app.workflow("bad-args")
    wf.add(take.s(object()))
    with pytest.raises(TypeError):
        wf.send()
    assert wf.id is None
    with pytest.raises(TypeError):
        wf.send()  # still unsent, not "already sent"


def test_double_send_from_two_threads_submits_once(app):
    @app.task
    def one():
        return 1

    wf = app.workflow("race")
    wf.add(one)
    start = threading.Barrier(4)
    handles, errors = [], []

    def go():
        start.wait()
        try:
            handles.append(wf.send())
        except ValueError as e:
            errors.append(e)

    threads = [threading.Thread(target=go) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len(handles) == 1 and len(errors) == 3


def test_declared_stub_tasks_can_be_nodes(app, running):
    @app.declare(name="wf.add")
    def add(a, b=1): ...

    @app.declare(name="wf.double")
    def double(x): ...

    wf = app.workflow("stubs")
    a = wf.add(add.s(2, b=3))
    wf.add(double.s(10), after=a)
    with pytest.raises(TypeError, match="wf.add"):
        wf.add(add.s(1, 2, 3))
    handle = wf.send()

    impl = App(broker=app.broker_url, result_ttl=60, default_queue=app.default_queue)

    @impl.task(name="wf.add")
    def add_impl(a, b=1):
        return a + b

    @impl.task(name="wf.double")
    def double_impl(x):
        return x * 2

    worker = WorkerRunner(impl, threads=2, queues=[impl.default_queue])
    worker.start()
    try:
        assert handle.nodes[0].result(timeout=10) == 5
        assert handle.nodes[1].result(timeout=10) == 20
        assert handle.wait(timeout=10) == "done"
    finally:
        worker.stop()
        worker.join()


def test_retried_node_releases_its_child_once(app, running):
    calls, ran = [], []

    @app.task(max_retries=3)
    def flaky():
        calls.append(1)
        if len(calls) == 1:
            raise Retry(countdown=0)
        return "ok"

    @app.task
    def child():
        ran.append(1)

    wf = app.workflow("retry")
    f = wf.add(flaky)
    wf.add(child, after=f)
    with running(app):
        handle = wf.send()
        assert handle.wait(timeout=10) == "done"
        assert handle.nodes[0].result(timeout=5) == "ok"
    assert (len(calls), len(ran)) == (2, 1)


def test_nodes_pass_args_and_kwargs_exactly(app, running):
    @app.task
    def echo(*args, **kwargs):
        return [list(args), kwargs]

    wf = app.workflow("args")
    first = wf.add(echo.s(1, "two", k=3))
    wf.add(echo, after=first)  # a bare task: no arguments, and no parent result
    with running(app):
        handle = wf.send()
        assert handle.nodes[0].result(timeout=10) == [[1, "two"], {"k": 3}]
        assert handle.nodes[1].result(timeout=10) == [[], {}]




def test_replaying_a_dead_node_resumes_the_workflow(app, running, queue):
    fixed = threading.Event()
    ran = []

    @app.task(max_retries=0)
    def flaky():
        if not fixed.is_set():
            raise ValueError("not yet")
        ran.append("flaky")

    @app.task
    def after_flaky():
        ran.append("after")

    wf = app.workflow("replayed")
    a = wf.add(flaky)
    wf.add(after_flaky, after=a)
    with running(app):
        handle = wf.send()
        assert handle.wait(timeout=10) == "failed"
        assert ran == []
        fixed.set()
        assert admin.dead_replay(app, queue) == (1, 0)
        assert handle.wait(timeout=10) == "done"
        handle.nodes[1].result(timeout=10)
    assert ran == ["flaky", "after"]
    assert [n["state"] for n in handle.status()["nodes"]] == ["done", "done"]
