import logging
import threading
import time
import urllib.request

import pytest

import mayu
from mayu import App, DiscardInstance, Pool, TaskFailed
from mayu import worker as worker_module
from mayu.pool import PoolContext, Use
from mayu.task import check_resources
from mayu.worker import WorkerRunner


def test_pool_decorator_returns_a_pool(app):
    @app.pool("gemma", vram_gb=5.8)
    def gemma(ctx):
        return ctx.index

    assert isinstance(gemma, Pool)
    assert app.pools == {"gemma": gemma}
    assert gemma.name == "gemma" and gemma.resources == {"vram_gb": 5.8} and gemma.discard_on == ()
    assert gemma.factory(PoolContext("gemma", 3, 4)) == 3
    assert gemma.closer is None
    assert repr(gemma) == "<Pool 'gemma'>"


def test_close_registers_once_and_returns_the_function(app):
    @app.pool("tts")
    def tts(ctx): ...

    def unload(instance): ...

    assert tts.close(unload) is unload
    assert tts.closer is unload
    with pytest.raises(ValueError, match="already has a close function"):
        tts.close(lambda instance: None)
    with pytest.raises(TypeError, match="callable"):
        app.pool("other")(lambda ctx: None).close("nope")


def test_duplicate_pool_name_is_rejected(app):
    app.pool("gemma")(lambda ctx: None)
    with pytest.raises(ValueError, match="pool 'gemma' is already registered"):
        app.pool("gemma")(lambda ctx: None)


@pytest.mark.parametrize("name", ["", "a,b", "a=1", "a b", "a\tb", "a\n"])
def test_bad_pool_names(app, name):
    with pytest.raises(ValueError, match="pool name"):
        app.pool(name)
    with pytest.raises(ValueError, match="pool name"):
        app.use(name)


def test_non_string_pool_name(app):
    with pytest.raises(TypeError, match="pool name"):
        app.pool(3)
    with pytest.raises(TypeError, match="pool name"):
        app.use(None)


def test_discard_on_takes_an_exception_class_or_a_tuple(app):
    assert app.pool("a", discard_on=KeyError)(lambda ctx: None).discard_on == (KeyError,)
    assert app.pool("b", discard_on=(KeyError, OSError))(lambda ctx: None).discard_on == (KeyError, OSError)
    for bad in ("KeyError", 3, (KeyError, "x"), [KeyError], len):
        with pytest.raises(TypeError, match="discard_on"):
            app.pool("c", discard_on=bad)
    assert "c" not in app.pools


def test_pool_factory_must_be_callable(app):
    with pytest.raises(TypeError, match="callable"):
        app.pool("a")("not a function")


def test_pool_resources_are_validated(app):
    with pytest.raises(ValueError, match="at least 0.001"):
        app.pool("a", vram_gb=0)
    with pytest.raises(TypeError, match="must be a number"):
        app.pool("a", vram_gb="lots")


def test_pool_prefixed_resource_names_are_reserved(app):
    with pytest.raises(ValueError, match="reserved for pools"):
        app.pool("a", **{"pool:x": 1})
    with pytest.raises(ValueError, match="reserved for pools"):
        app.task(resources={"pool:x": 1})(lambda: None)
    with pytest.raises(ValueError, match="reserved for pools"):
        check_resources("resources", {"pool:gemma": 1}, allow_zero=True)
    assert check_resources("resources", {"pooled": 1}) == {"pooled": 1.0}


def test_use_compares_and_hashes_by_pool_name(app):
    assert app.use("gemma") == app.use("gemma") == Use("gemma")
    assert app.use("gemma") != app.use("tts")
    assert app.use("gemma") != "gemma"
    assert len({app.use("gemma"), app.use("gemma"), app.use("tts")}) == 2
    assert repr(app.use("gemma")) == "app.use('gemma')"
    assert app.use("gemma").name == "gemma"


def test_declare_and_implement_match_on_use(app):
    @app.declare(name="pools.encode")
    def encode(run, model=app.use("gemma")): ...

    assert encode.pools == {"model": "gemma"}

    @encode.implement
    def _(run, model=app.use("gemma")):
        return model

    assert encode.implemented and encode.pools == {"model": "gemma"}


def test_implement_with_another_pool_or_a_plain_default_fails(app):
    @app.declare(name="pools.a")
    def a(run, model=app.use("gemma")): ...

    for bad in (lambda run, model=app.use("tts"): None, lambda run, model=None: None, lambda run, model: None):
        with pytest.raises(TypeError, match="must match its declaration"):
            a.implement(bad)
    assert not a.implemented


def test_implement_rejects_async_with_a_pool(app):
    @app.declare(name="pools.b")
    def b(model=app.use("gemma")): ...

    async def impl(model=app.use("gemma")): ...

    # an async implementation differs from the sync declaration only in kind-of-function, so the shape matches
    with pytest.raises(TypeError, match="pools are for sync tasks"):
        b.implement(impl)
    assert not b.implemented and b.pools == {"model": "gemma"}


def test_task_pools_contents(app):
    @app.task
    def plain(x): ...

    @app.task
    def two(x, model=app.use("gemma"), *, voice=app.use("tts"), n=1): ...

    @app.task(threadsafe=False)
    def serial(model=app.use("gemma")): ...

    assert plain.pools == {}
    assert two.pools == {"model": "gemma", "voice": "tts"}
    assert serial.pools == {"model": "gemma"}


def test_pool_parameter_must_be_keyword_capable(app):
    with pytest.raises(TypeError, match="positional-only"):
        @app.task
        def bad(model=app.use("gemma"), /): ...


def test_one_pool_in_two_parameters_is_rejected(app):
    with pytest.raises(TypeError, match="one instance per pool"):
        @app.task
        def bad(a=app.use("gemma"), b=app.use("gemma")): ...


def test_async_task_cannot_use_a_pool(app):
    with pytest.raises(TypeError, match="pools are for sync tasks"):
        @app.task
        async def bad(model=app.use("gemma")): ...

    @app.task
    async def fine(x=1): ...

    assert fine.pools == {}


def test_producer_declares_a_pool_task_without_the_pool(app):
    @app.declare(name="pools.encode")
    def encode(run, model=app.use("gemma")): ...

    assert app.pools == {}
    assert encode.send("r1").id
    assert app.send("pools.encode", "r2").id
    assert encode.send("r3", link=encode.s()).id


def test_send_rejects_the_pool_parameter(app):
    @app.task
    def encode(run, model=app.use("gemma")): ...

    with pytest.raises(TypeError, match=r"parameter 'model' is filled from pool 'gemma' on the worker; don't pass it"):
        encode.send("r", model=1)
    with pytest.raises(TypeError, match="parameter 'model' is filled from pool 'gemma'"):
        encode.send("r", 1)
    with pytest.raises(TypeError, match="parameter 'model' is filled from pool 'gemma'"):
        app.send(encode.name, "r", model=1)
    assert encode.send("r").id


def test_workflow_node_rejects_the_pool_parameter(app):
    @app.task
    def encode(run, model=app.use("gemma")): ...

    wf = app.workflow("w")
    with pytest.raises(TypeError, match=r"cannot be a workflow node: parameter 'model' is filled from pool 'gemma'"):
        wf.add(encode.s("r", model=1))
    with pytest.raises(TypeError, match="missing a required argument: 'run'"):
        wf.add(encode)  # other binding errors are still reported
    assert wf.add(encode.s("r")).index == 0


def test_link_rejects_the_pool_parameter(app):
    @app.task
    def encode(run, model=app.use("gemma")): ...

    @app.task
    def first_is_pool(model=app.use("gemma")): ...

    @app.task
    def parent(x): ...

    with pytest.raises(TypeError, match=r"cannot be a link callback: parameter 'model' is filled from pool 'gemma'"):
        parent.send(1, link=encode.s(model=1))
    # the parent's result lands in the first slot, which here is the pool parameter
    with pytest.raises(TypeError, match="parameter 'model' is filled from pool 'gemma'"):
        parent.send(1, link=first_is_pool)
    assert parent.send(1, link=encode).id
    assert parent.send(1, link=[encode.s(), parent.s()]).id


def test_exports():
    assert mayu.Pool is Pool and mayu.DiscardInstance is DiscardInstance
    assert mayu.__all__ == sorted(mayu.__all__, key=lambda n: (n[0].islower(), n))


def test_discard_instance():
    assert issubclass(DiscardInstance, Exception)
    e = DiscardInstance("gone", pool="gemma")
    assert str(e) == "gone" and e.pool == "gemma"
    assert DiscardInstance().pool is None
    with pytest.raises(TypeError):
        DiscardInstance("gone", "gemma")


def test_pool_context_is_frozen():
    ctx = PoolContext("gemma", 1, 2)
    assert (ctx.name, ctx.index, ctx.size) == ("gemma", 1, 2)
    with pytest.raises(AttributeError):
        ctx.index = 0


# -- worker ---------------------------------------------------------------------------------------------------


class Model:
    """A stand-in for a loaded model; `gen` counts every instance the factory ever built."""

    def __init__(self, index, gen):
        self.index = index
        self.gen = gen


class Fleet:
    """Factory and close function for a pool, recording what the worker did with them."""

    def __init__(self, fail=None):
        self.lock = threading.Lock()
        self.fail = fail or (lambda ctx, gen: False)  # (ctx, gen) -> True makes that build raise
        self.built = []  # (index, size, gen)
        self.closed = []  # gen, in the order of the close calls
        self.gens = 0
        self.active = 0
        self.max_active = 0

    def factory(self, ctx):
        with self.lock:
            gen = self.gens
            self.gens += 1
            self.active += 1
            self.max_active = max(self.max_active, self.active)
        try:
            time.sleep(0.005)  # a slow load: gives overlapping builds the chance to show
            if self.fail(ctx, gen):
                raise OSError(f"cannot load {ctx.name}[{ctx.index}]")
            with self.lock:
                self.built.append((ctx.index, ctx.size, gen))
            return Model(ctx.index, gen)
        finally:
            with self.lock:
                self.active -= 1

    def close(self, model):
        with self.lock:
            self.closed.append(model.gen)

    def attach(self, pool):
        pool.close(self.close)
        return pool


def _eventually(predicate, timeout=10.0):
    deadline = time.monotonic() + timeout
    while not predicate():
        assert time.monotonic() < deadline, "condition not reached in time"
        time.sleep(0.01)  # bounded poll: the thing awaited happens on a worker thread with no event to wait on


def _scrape(port):
    with urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=5) as r:
        return r.read().decode()


def _series(port, series):
    for line in _scrape(port).splitlines():
        if line.startswith(series + " "):
            return float(line.rsplit(" ", 1)[1])
    return None


def test_pool_admits_at_most_n_tasks_and_hands_out_distinct_instances(app, running):
    fleet = Fleet()
    fleet.attach(app.pool("gemma")(fleet.factory))
    lock, gate, arrived = threading.Lock(), threading.Event(), threading.Semaphore(0)
    state = {"now": 0, "max": 0, "busy": set(), "clashes": 0, "seen": set()}

    @app.task
    def probe(model=app.use("gemma")):
        with lock:
            state["now"] += 1
            state["max"] = max(state["max"], state["now"])
            state["clashes"] += id(model) in state["busy"]
            state["busy"].add(id(model))
            state["seen"].add((model.index, model.gen))
        arrived.release()
        try:
            gate.wait(20)
        finally:
            with lock:
                state["now"] -= 1
                state["busy"].discard(id(model))
        return model.index

    @app.task(threadsafe=False)
    def serial(model=app.use("gemma")):
        return [model.index, model.gen]

    with running(app, threads=4, pools={"gemma": 2}, metrics_port=0, metrics_host="127.0.0.1") as w:
        handles = [probe.send() for _ in range(6)]
        try:
            assert arrived.acquire(timeout=10) and arrived.acquire(timeout=10), "two tasks should hold an instance each"
            assert not arrived.acquire(timeout=0.3), "a third task must wait for an instance"
            assert _series(w.metrics_port, 'mayu_pool_size{pool="gemma"}') == 2.0
            assert _series(w.metrics_port, 'mayu_pool_busy{pool="gemma"}') == 2.0
        finally:
            gate.set()
        assert len([h.result(timeout=10) for h in handles]) == 6
        assert serial.send().result(timeout=10)[0] in (0, 1)
    assert state["max"] == 2 and state["clashes"] == 0
    assert state["seen"] == {(0, 0), (1, 1)}
    assert fleet.built == [(0, 2, 0), (1, 2, 1)] and fleet.max_active == 1
    assert sorted(fleet.closed) == [0, 1]


def test_discarded_instances_are_closed_and_rebuilt_with_the_same_index(app, running):
    fleet = Fleet()
    fleet.attach(app.pool("gemma", discard_on=(KeyError,))(fleet.factory))

    @app.task
    def probe(model=app.use("gemma")):
        return [model.index, model.gen]

    @app.task
    def broken(model=app.use("gemma")):
        raise DiscardInstance("out of memory")

    @app.task
    def other_pool(model=app.use("gemma")):
        raise DiscardInstance("not mine", pool="tts")

    @app.task
    def keyed(model=app.use("gemma")):
        raise KeyError("x")

    with running(app, threads=2, pools={"gemma": 1}, metrics_port=0, metrics_host="127.0.0.1") as w:
        assert probe.send().result(timeout=10) == [0, 0]
        with pytest.raises(TaskFailed, match="DiscardInstance"):
            broken.send().result(timeout=10)
        assert probe.send().result(timeout=10) == [0, 1]  # waited for the replacement
        assert fleet.closed == [0]
        with pytest.raises(TaskFailed, match="DiscardInstance"):
            other_pool.send().result(timeout=10)  # names another pool: this instance is fine
        assert probe.send().result(timeout=10) == [0, 1]
        with pytest.raises(TaskFailed, match="KeyError"):
            keyed.send().result(timeout=10)
        assert probe.send().result(timeout=10) == [0, 2]
        assert fleet.closed == [0, 1]
        assert fleet.built == [(0, 1, 0), (0, 1, 1), (0, 1, 2)]
        _eventually(lambda: _series(w.metrics_port, 'mayu_pool_rebuilds_total{pool="gemma"}') == 2.0)
        assert _series(w.metrics_port, 'mayu_pool_size{pool="gemma"}') == 1.0
        assert w.restart_reason is None
    assert fleet.closed == [0, 1, 2]


def test_discard_instance_without_a_name_discards_every_held_instance(app, running):
    gemma, tts = Fleet(), Fleet()
    gemma.attach(app.pool("gemma", vram_gb=1)(gemma.factory))
    tts.attach(app.pool("tts", vram_gb=1)(tts.factory))

    @app.task
    def probe(a=app.use("gemma"), b=app.use("tts")):
        return [a.gen, b.gen]

    @app.task
    def drop(which, a=app.use("gemma"), b=app.use("tts")):
        raise DiscardInstance(pool=which)

    with running(app, threads=2, resources={"vram_gb": 3}, pools={"gemma": 1, "tts": 1}):
        assert probe.send().result(timeout=10) == [0, 0]
        with pytest.raises(TaskFailed):
            drop.send("tts").result(timeout=10)
        assert probe.send().result(timeout=10) == [0, 1]
        with pytest.raises(TaskFailed):
            drop.send(None).result(timeout=10)
        assert probe.send().result(timeout=10) == [1, 2]
        assert (gemma.closed, tts.closed) == ([0], [0, 1])


def test_retry_on_discard_instance_runs_again_on_a_fresh_instance(app, running):
    fleet = Fleet()
    fleet.attach(app.pool("gemma")(fleet.factory))

    @app.task(retry_on=(DiscardInstance,), max_retries=2)
    def flaky(model=app.use("gemma")):
        if model.gen == 0:
            raise DiscardInstance("first one is broken")
        return model.gen

    with running(app, threads=2, pools={"gemma": 1}):
        assert flaky.send().result(timeout=20) == 1
    assert fleet.closed[0] == 0


def test_a_failed_rebuild_leaves_the_pool_smaller_and_the_last_one_requests_a_recycle(app, running):
    rebuilding = threading.Event()
    fleet = Fleet(fail=lambda ctx, gen: rebuilding.is_set())
    fleet.attach(app.pool("gemma")(fleet.factory))

    @app.task
    def probe(model=app.use("gemma")):
        return model.gen

    @app.task
    def broken(model=app.use("gemma")):
        raise DiscardInstance()

    with running(app, threads=2, pools={"gemma": 2}, metrics_port=0, metrics_host="127.0.0.1") as w:
        rebuilding.set()
        with pytest.raises(TaskFailed):
            broken.send().result(timeout=10)
        assert probe.send().result(timeout=10) == 1  # the other instance still serves
        assert _series(w.metrics_port, 'mayu_pool_size{pool="gemma"}') == 1.0
        assert w.restart_reason is None
        with pytest.raises(TaskFailed):
            broken.send().result(timeout=10)
        _eventually(lambda: w.restart_reason is not None)
        assert w.restart_reason == "pool 'gemma' has no instances left"
        assert _series(w.metrics_port, 'mayu_pool_size{pool="gemma"}') == 0.0


def test_hard_time_limit_forfeits_the_instance_the_zombie_holds(app, running):
    fleet = Fleet()
    fleet.attach(app.pool("gemma")(fleet.factory))
    release = threading.Event()

    @app.task(hard_time_limit=0.3)
    def stuck(model=app.use("gemma")):
        release.wait(30)

    @app.task
    def probe(model=app.use("gemma")):
        return [model.index, model.gen]

    with running(app, threads=2, pools={"gemma": 2}, shutdown_grace=2, metrics_port=0,
                 metrics_host="127.0.0.1") as w:
        try:
            with pytest.raises(TaskFailed, match="TimeLimitExceeded"):
                stuck.send().result(timeout=10)
            assert _series(w.metrics_port, 'mayu_pool_size{pool="gemma"}') == 1.0
            assert probe.send().result(timeout=10) == [1, 1]  # the zombie still holds instance 0
            assert probe.send().result(timeout=10) == [1, 1]
            assert fleet.closed == [] and w.restart_reason is None
        finally:
            release.set()
    assert fleet.closed == [1], "only the idle instance is closed; the zombie's may still be in use"


def test_every_idle_instance_is_closed_at_shutdown_last_loaded_first(app, running, caplog):
    first, second = Fleet(), Fleet()
    order = []

    def close_first(model):
        order.append(("gemma", model.index))
        if model.index == 1:
            raise RuntimeError("unload failed")
        first.close(model)

    app.pool("gemma")(first.factory).close(close_first)
    app.pool("tts")(second.factory).close(lambda m: order.append(("tts", m.index)))
    app.pool("quiet")(lambda ctx: object())  # no close function: instances are just dropped

    @app.task
    def use(a=app.use("gemma"), b=app.use("tts"), c=app.use("quiet")): ...

    logger = logging.getLogger("mayu.worker")
    logger.addHandler(caplog.handler)
    try:
        with running(app, threads=1, pools={"gemma": 3, "tts": 1, "quiet": 1}):
            pass
    finally:
        logger.removeHandler(caplog.handler)
    assert order == [("tts", 0), ("gemma", 2), ("gemma", 1), ("gemma", 0)]
    assert first.closed == [2, 0], "a failing close must not stop the others"
    assert "closing an instance of pool 'gemma' failed" in caplog.text


def test_a_load_failure_closes_what_was_built_and_names_the_instance(app, queue):
    fleet = Fleet(fail=lambda ctx, gen: ctx.index == 1)
    fleet.attach(app.pool("gemma")(fleet.factory))

    @app.task
    def use(model=app.use("gemma")): ...

    w = WorkerRunner(app, threads=1, queues=[queue], pools={"gemma": 3})
    with pytest.raises(RuntimeError, match=r"pool 'gemma' instance 1 failed to load: OSError\('cannot load gemma"):
        w.start()
    assert fleet.closed == [0] and fleet.built == [(0, 3, 0)]
    assert w.native is None


def test_instances_are_closed_when_the_native_worker_cannot_start(app, queue, monkeypatch):
    fleet = Fleet()
    fleet.attach(app.pool("gemma")(fleet.factory))

    @app.task
    def use(model=app.use("gemma")): ...

    def boom(*a, **kw):
        raise RuntimeError("no redis")

    monkeypatch.setattr(worker_module._native, "Worker", boom)
    w = WorkerRunner(app, threads=1, queues=[queue], pools={"gemma": 2})
    with pytest.raises(RuntimeError, match="no redis"):
        w.start()
    assert fleet.built == [(0, 2, 0), (1, 2, 1)] and fleet.closed == [1, 0]


def test_the_constructor_validates_without_loading(app, queue):
    fleet = Fleet()
    app.pool("gemma", vram_gb=3)(fleet.factory)

    @app.task
    def use(model=app.use("gemma")): ...

    w = WorkerRunner(app, threads=1, queues=[queue], resources={"vram_gb": 6}, pools={"gemma": 2})
    assert fleet.built == [] and w.native is None


def _runner(app, queue, **kw):
    return WorkerRunner(app, threads=1, queues=[queue], **kw)


def test_a_task_using_an_unregistered_pool_is_refused(app, queue):
    @app.task(name="enc")
    def enc(model=app.use("gemma")): ...

    with pytest.raises(ValueError) as e:
        _runner(app, queue, pools={"gemma": 1})
    assert str(e.value) == ("task 'enc' uses pool 'gemma', which this process does not define; "
                            "import the module with @app.pool('gemma')")


@pytest.mark.parametrize("pools", [None, {}, {"gemma": 0}, {"tts": 2}])
def test_a_task_using_a_pool_the_worker_does_not_size_is_refused(app, queue, pools):
    app.pool("gemma")(lambda ctx: None)
    app.pool("tts")(lambda ctx: None)

    @app.task(name="enc")
    def enc(model=app.use("gemma")): ...

    with pytest.raises(ValueError) as e:
        _runner(app, queue, pools=pools)
    assert str(e.value) == "task 'enc' uses pool 'gemma' but this worker has no instances of it; pass --pools gemma=N"


def test_a_sized_pool_that_is_not_registered_is_refused(app, queue):
    with pytest.raises(ValueError, match=r"--pools names pool 'ghost', which this process does not define; "
                                          r"import the module with @app.pool\('ghost'\)"):
        _runner(app, queue, pools={"ghost": 1})
    _runner(app, queue, pools={"ghost": 0})  # sized zero is "off", whether or not this process knows it


def test_an_unused_sized_pool_warns_and_is_not_loaded(app, queue, caplog):
    fleet = Fleet()
    app.pool("gemma", vram_gb=3)(fleet.factory)
    logger = logging.getLogger("mayu.worker")
    logger.addHandler(caplog.handler)
    try:
        w = _runner(app, queue, resources={"vram_gb": 1}, pools={"gemma": 2})  # would not even fit if reserved
    finally:
        logger.removeHandler(caplog.handler)
    assert "pool 'gemma' is sized but no task on the served queues uses it" in caplog.text
    assert w.capacity == {"vram_gb": 1.0} and w.task_resources == {}
    w._load_pools()
    assert fleet.built == []


def test_a_pool_resource_the_worker_does_not_declare_is_refused(app, queue):
    app.pool("gemma", vram_gb=3)(lambda ctx: None)

    @app.task
    def enc(model=app.use("gemma")): ...

    with pytest.raises(ValueError) as e:
        _runner(app, queue, resources={"gpu": 1}, pools={"gemma": 1})
    assert str(e.value) == ("pool 'gemma' needs 'vram_gb' per instance but this worker declares no such resource; "
                            "pass --resources vram_gb=...")


def test_reservations_cannot_exceed_the_capacity(app, queue):
    app.pool("gemma", vram_gb=5.8)(lambda ctx: None)

    @app.task
    def enc(model=app.use("gemma")): ...

    with pytest.raises(ValueError) as e:
        _runner(app, queue, resources={"vram_gb": 8}, pools={"gemma": 2})
    assert str(e.value) == ("pools reserve vram_gb=11.6 (gemma 2 × 5.8) but this worker has 8; "
                            "lower --pools or raise --resources vram_gb=...")


def test_reservations_leave_the_rest_for_tasks_and_float_drift_is_ignored(app, queue):
    app.pool("gemma", vram_gb=3)(lambda ctx: None)

    @app.task(name="fits", resources={"vram_gb": 2})
    def fits(model=app.use("gemma")): ...

    w = _runner(app, queue, resources={"vram_gb": 8, "gpu": 1}, pools={"gemma": 2})
    assert w.capacity == {"vram_gb": 2.0, "gpu": 1.0, "pool:gemma": 2.0}
    assert w.resources == {"vram_gb": 8.0, "gpu": 1.0}
    assert w.task_resources == {"fits": {"vram_gb": 2.0, "pool:gemma": 1.0}}

    @app.task(name="greedy", resources={"vram_gb": 3})
    def greedy(model=app.use("gemma")): ...

    with pytest.raises(ValueError) as e:
        _runner(app, queue, resources={"vram_gb": 8}, pools={"gemma": 2})
    assert str(e.value) == ("task 'greedy' requests vram_gb=3 but this worker has vram_gb=2 left after pools "
                            "reserve 6; raise --resources vram_gb=... or route the task to a bigger worker")


def test_milli_unit_arithmetic_avoids_float_drift(app, queue):
    app.pool("small", cpu=0.1)(lambda ctx: None)

    @app.task(name="t")
    def t(m=app.use("small")): ...

    w = _runner(app, queue, resources={"cpu": 0.3}, pools={"small": 3})  # 3 * 0.1 > 0.3 in floats
    assert w.capacity == {"cpu": 0.0, "pool:small": 3.0}


def test_pool_tasks_without_resources_still_request_their_pool(app, queue):
    app.pool("a")(lambda ctx: None)
    app.pool("b")(lambda ctx: None)

    @app.task(name="plain")
    def plain(): ...

    @app.task(name="both")
    def both(x=app.use("a"), y=app.use("b")): ...

    w = _runner(app, queue, pools={"a": 1, "b": 4})
    assert w.task_resources == {"both": {"pool:a": 1.0, "pool:b": 1.0}}
    assert w.capacity == {"pool:a": 1.0, "pool:b": 4.0}
    assert _runner(app, queue, pools={"a": 1, "b": 1}).resources == {}


@pytest.mark.parametrize("pools,exc", [
    ([("gemma", 1)], TypeError),
    ({"gemma": "1"}, TypeError),
    ({"gemma": 1.5}, TypeError),
    ({"gemma": True}, TypeError),
    ({"gemma": -1}, ValueError),
    ({"a b": 1}, ValueError),
    ({3: 1}, TypeError),
])
def test_bad_pools_shapes_are_rejected(app, queue, pools, exc):
    with pytest.raises(exc):
        _runner(app, queue, pools=pools)


def test_metrics_have_no_pool_series_without_pools(app, running):
    @app.task
    def plain(): ...

    with running(app, threads=1, metrics_port=0, metrics_host="127.0.0.1") as w:
        assert "mayu_pool" not in _scrape(w.metrics_port)
