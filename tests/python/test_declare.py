import threading

import pytest

from mayu import App, TaskFailed
from mayu.worker import WorkerRunner


def _worker_app(app):
    return App(broker=app.broker_url, result_ttl=60, default_queue=app.default_queue)


def test_declared_task_validates_and_enqueues_before_implementation(app, queue):
    other = f"{queue}-other"

    @app.declare(name="decl.add", queue=other)
    def add(a, b=1): ...

    @app.task
    def record(result): ...

    assert not add.implemented
    with pytest.raises(TypeError):
        add.send(1, 2, 3)
    with pytest.raises(TypeError):
        add.send(c=1)
    handle = add.send(1, b=2)
    assert handle.id
    assert add.s(5).args == (5,)
    assert add.send(1, link=record).id
    assert record.send(1, link=add.s(b=3)).id
    assert app.queues() == sorted({queue, other})


def test_declare_bare_decorator_form(app):
    @app.declare
    def bare(x): ...

    assert app.registry[bare.name] is bare
    assert bare.queue == app.default_queue and not bare.implemented


def test_calling_unimplemented_task_names_implement(app):
    @app.declare(name="decl.nope")
    def nope(): ...

    with pytest.raises(TypeError, match=r"decl\.nope.*declared but not implemented.*implement"):
        nope()


def test_implement_rejects_mismatched_parameters(app):
    @app.declare(name="decl.sig")
    def sig(a, b=1, *, c=2): ...

    for bad in (
        lambda a, x=1, *, c=2: None,       # name
        lambda a, b=1, c=2: None,          # kind
        lambda a, b=2, *, c=2: None,       # default
        lambda a, b=1: None,               # arity
    ):
        with pytest.raises(TypeError, match="decl.sig"):
            sig.implement(bad)
    assert not sig.implemented

    def good(a: int, b: int = 1, *, c: int = 2) -> int:  # annotations are ignored
        return a + b + c

    assert sig.implement(good) is sig
    assert sig.implemented and sig(1) == 4 and sig.__name__ == "good"


def test_implement_works_as_decorator_and_returns_task(app):
    @app.declare(name="decl.deco")
    def deco(x): ...

    @deco.implement
    def _(x):
        return x * 2

    assert _ is deco and deco(4) == 8


def test_implement_twice_or_on_plain_task_raises(app):
    @app.declare(name="decl.twice")
    def twice(): ...

    twice.implement(lambda: 1)
    with pytest.raises(ValueError, match="already implemented"):
        twice.implement(lambda: 2)

    @app.task
    def plain(): ...

    with pytest.raises(ValueError, match="app.declare"):
        plain.implement(lambda: 3)


def test_implement_async_task_cannot_be_threadsafe_false(app):
    @app.declare(name="decl.unsafe", threadsafe=False)
    def unsafe(): ...

    async def impl():
        return 1

    with pytest.raises(ValueError, match="threadsafe=False"):
        unsafe.implement(impl)
    assert not unsafe.implemented


def test_end_to_end_producer_sends_worker_runs(app, running):
    @app.declare(name="decl.mul")
    def mul(a, b): ...

    worker = _worker_app(app)

    @worker.declare(name="decl.mul")
    def mul_decl(a, b): ...

    @mul_decl.implement
    def _(a, b):
        return a * b

    @app.declare(name="decl.amul")
    def amul(a, b): ...

    @worker.declare(name="decl.amul")
    def amul_decl(a, b): ...

    @amul_decl.implement
    async def _(a, b):
        return a * b + 1

    assert amul_decl.is_async
    with running(worker):
        assert mul.send(6, 7).result(timeout=10) == 42
        assert amul.send(6, 7).result(timeout=10) == 43


def test_app_send_by_name_unregistered(app, running):
    worker = _worker_app(app)

    @worker.task(name="by.name")
    def by_name(x):
        return x + 1

    assert "by.name" not in app.registry
    with running(worker):
        assert app.send("by.name", 1).result(timeout=10) == 2
        assert app.send("by.name", x=2, countdown=0).result(timeout=10) == 3


def test_app_send_by_name_uses_default_queue_and_explicit_queue(app, queue):
    handle = app.send("anything", queue=f"{queue}-x")
    assert handle.id
    assert app.send("anything").id


def test_app_send_registered_name_is_validated(app):
    @app.declare(name="decl.arity")
    def arity(a): ...

    with pytest.raises(TypeError):
        app.send("decl.arity", 1, 2)
    assert app.send("decl.arity", 1).id


def test_concurrent_send_while_implementing(app, running):
    @app.declare(name="decl.race")
    def race(x): ...

    start = threading.Barrier(9)
    handles, errors = [], []
    lock = threading.Lock()

    def sender(n):
        try:
            start.wait(timeout=10)
            for i in range(10):
                h = race.send(n * 100 + i)
                with lock:
                    handles.append((n * 100 + i, h))
        except BaseException as e:
            errors.append(e)

    def implementer():
        try:
            start.wait(timeout=10)
            race.implement(lambda x: x + 1)
        except BaseException as e:
            errors.append(e)

    threads = [threading.Thread(target=sender, args=(n,)) for n in range(8)] + [threading.Thread(target=implementer)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=20)
    assert not errors and len(handles) == 80 and race.implemented
    with running(app):
        for x, h in handles:
            assert h.result(timeout=10) == x + 1


def test_declare_rejects_unknown_option(app):
    with pytest.raises(TypeError, match="unknown task option"):
        app.declare(name="decl.typo", queu="x")


def test_worker_fails_delivery_of_unimplemented_task(app, queue):
    worker = _worker_app(app)
    runner = WorkerRunner(worker, threads=1, queues=[queue])  # built before the declaration, so it serves the queue
    worker.declare(name="decl.stub", max_retries=0)(lambda: None)
    runner.start()
    try:
        with pytest.raises(TaskFailed, match="not implemented"):
            app.send("decl.stub").result(timeout=10)
    finally:
        runner.stop()
        runner.join()


def test_default_worker_queues_exclude_declared_only_tasks(app, queue):
    gpu = f"{queue}-gpu"
    worker = _worker_app(app)

    @worker.declare(name="decl.gpu", queue=gpu)
    def gpu_task(): ...

    @worker.task(name="decl.cpu", queue=f"{queue}-cpu")
    def cpu_task(): ...

    assert worker.queues() == sorted({queue, gpu, f"{queue}-cpu"})
    assert worker.queues(implemented=True) == sorted({queue, f"{queue}-cpu"})
    assert WorkerRunner(worker, threads=1).queues == sorted({queue, f"{queue}-cpu"})
    gpu_task.implement(lambda: 1)
    assert gpu in WorkerRunner(worker, threads=1).queues


def test_worker_refuses_explicit_queue_with_unimplemented_tasks(app, queue):
    gpu = f"{queue}-gpu"
    worker = _worker_app(app)

    @worker.declare(name="decl.b", queue=gpu)
    def b(): ...

    @worker.declare(name="decl.a", queue=gpu)
    def a(): ...

    with pytest.raises(ValueError, match=rf"queue '{gpu}' has tasks declared but not implemented.*decl\.a, decl\.b"):
        WorkerRunner(worker, threads=1, queues=[gpu])
    WorkerRunner(worker, threads=1, queues=[queue])
