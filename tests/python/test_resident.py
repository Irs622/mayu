"""Lazy pools: models loaded on demand, evicted when another task needs the room, scheduled by residency."""
import logging
import threading
import time
import urllib.request

import pytest

from mayu import App, DiscardInstance, TaskFailed
from mayu.worker import WorkerRunner


class Model:
    def __init__(self, name, index, gen):
        self.name = name
        self.index = index
        self.gen = gen  # which build of this pool it is


class Device:
    """A fake GPU. A factory that would take the sum of live models past the budget fails and is recorded."""

    def __init__(self, budget):
        self.lock = threading.Lock()
        self.budget = budget
        self.used = 0.0
        self.peak = 0.0
        self.gens = {}
        self.vram = {}
        self.fail = {}  # pool -> callable returning True while its factory should raise
        self.failed = 0
        self.built = []  # (pool, index, size, gen)
        self.closed = []  # (pool, gen)
        self.overcommits = []

    def pool(self, app, name, vram, **options):
        self.vram[name] = vram
        pool = app.pool(name, vram_gb=vram, **options)(lambda ctx: self._build(ctx))
        pool.close(self._close)
        return pool

    def _build(self, ctx):
        time.sleep(0.005)  # a slow load: gives a concurrent over-commit the chance to show
        if self.fail.get(ctx.name, lambda: False)():
            with self.lock:
                self.failed += 1
            raise OSError(f"cannot load {ctx.name}")
        with self.lock:
            if self.used + self.vram[ctx.name] > self.budget + 1e-9:
                self.overcommits.append((ctx.name, self.used))
                raise MemoryError(f"{ctx.name} does not fit: {self.used} in use of {self.budget}")
            gen = self.gens.get(ctx.name, 0)
            self.gens[ctx.name] = gen + 1
            self.used += self.vram[ctx.name]
            self.peak = max(self.peak, self.used)
            self.built.append((ctx.name, ctx.index, ctx.size, gen))
        return Model(ctx.name, ctx.index, gen)

    def _close(self, model):
        with self.lock:
            self.used -= self.vram[model.name]
            self.closed.append((model.name, model.gen))

    def builds(self, name):
        return [b for b in self.built if b[0] == name]


def _scrape(port):
    with urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=5) as r:
        return r.read().decode()


def _series(port, series):
    for line in _scrape(port).splitlines():
        if line.startswith(series + " "):
            return float(line.rsplit(" ", 1)[1])
    return None


def _eventually(predicate, timeout=10.0):
    deadline = time.monotonic() + timeout
    while not predicate():
        assert time.monotonic() < deadline, "condition not reached in time"
        time.sleep(0.01)  # bounded poll: the thing awaited happens on a worker thread with no event to wait on


LAZY = dict(pool_mode="lazy", metrics_port=0, metrics_host="127.0.0.1")


def _two_models(app, budget=7.5):
    dev = Device(budget)
    dev.pool(app, "gemma", 7.4)
    dev.pool(app, "tts", 7.2)

    @app.task
    def g(m=app.use("gemma")):
        return [m.name, m.index, m.gen]

    @app.task
    def t(m=app.use("tts")):
        return [m.name, m.index, m.gen]

    return dev, g, t


def test_a_task_for_the_other_model_evicts_the_loaded_one(app, running):
    dev, g, t = _two_models(app)
    with running(app, threads=1, resources={"vram_gb": 7.5}, pools={"gemma": 1, "tts": 1}, **LAZY):
        assert g.send().result(timeout=10) == ["gemma", 0, 0]
        assert dev.built == [("gemma", 0, 1, 0)] and dev.closed == []
        assert t.send().result(timeout=10) == ["tts", 0, 0]
        assert dev.closed == [("gemma", 0)]
        assert t.send().result(timeout=10) == ["tts", 0, 0]  # resident: no build, no close
        assert len(dev.built) == 2 and len(dev.closed) == 1
        assert g.send().result(timeout=10) == ["gemma", 0, 1]
        assert dev.closed == [("gemma", 0), ("tts", 0)]
    assert dev.closed == [("gemma", 0), ("tts", 0), ("gemma", 1)], "the idle instance is closed at shutdown"
    assert dev.peak <= 7.5 and dev.overcommits == []


def test_serial_tasks_carry_the_plan_too(app, running):
    dev = Device(7.5)
    dev.pool(app, "gemma", 7.4)
    dev.pool(app, "tts", 7.2)

    @app.task(threadsafe=False)
    def g(m=app.use("gemma")):
        return [m.name, m.gen]

    @app.task(threadsafe=False)
    def t(m=app.use("tts")):
        return [m.name, m.gen]

    with running(app, threads=2, resources={"vram_gb": 7.5}, pools={"gemma": 1, "tts": 1}, **LAZY):
        assert g.send().result(timeout=10) == ["gemma", 0]
        assert t.send().result(timeout=10) == ["tts", 0]
        assert g.send().result(timeout=10) == ["gemma", 1]
    assert dev.closed[:2] == [("gemma", 0), ("tts", 0)] and dev.overcommits == []


def test_a_task_without_a_pool_evicts_a_model_it_has_no_room_beside(app, running):
    dev, g, _ = _two_models(app)

    @app.task(resources={"vram_gb": 1})
    def render():
        return "rendered"

    @app.task(resources={"vram_gb": 0.05})
    def tiny():
        return "tiny"

    @app.task(resources={"vram_gb": 1})
    async def render_async():
        return "async"

    with running(app, threads=1, resources={"vram_gb": 7.5}, pools={"gemma": 1, "tts": 1}, **LAZY):
        assert g.send().result(timeout=10) == ["gemma", 0, 0]
        assert tiny.send().result(timeout=10) == "tiny"  # fits beside the model
        assert dev.closed == []
        assert render.send().result(timeout=10) == "rendered"  # does not
        assert dev.closed == [("gemma", 0)]
        assert g.send().result(timeout=10) == ["gemma", 0, 1]
        assert render_async.send().result(timeout=10) == "async"  # an async task makes room as well
        assert dev.closed == [("gemma", 0), ("gemma", 1)]
        assert g.send().result(timeout=10) == ["gemma", 0, 2]  # the plan stayed consistent: loaded again, once
    assert dev.overcommits == []


def test_throughput_schedule_groups_tasks_by_model(app, running):
    dev = Device(7.5)
    dev.pool(app, "gemma", 7.4)
    dev.pool(app, "tts", 7.2)
    order, lock = [], threading.Lock()

    @app.task
    def g(m=app.use("gemma")):
        with lock:
            order.append("g")

    @app.task
    def t(m=app.use("tts")):
        with lock:
            order.append("t")

    handles = [(g if i % 2 == 0 else t).send() for i in range(6)]  # g t g t g t, all fetched in one batch
    with running(app, threads=1, resources={"vram_gb": 7.5}, pools={"gemma": 1, "tts": 1}, prefetch_multiplier=8,
                 schedule="throughput", preload=["gemma"], **LAZY):
        for h in handles:
            h.result(timeout=30)
    assert order == ["g", "g", "g", "t", "t", "t"]
    assert len(dev.builds("gemma")) == 1 and len(dev.builds("tts")) == 1
    assert dev.overcommits == []


def test_an_instance_a_running_task_holds_is_never_closed(app, running):
    dev, _, t = _two_models(app)
    started, release = threading.Event(), threading.Event()

    @app.task
    def hold(m=app.use("gemma")):
        started.set()
        release.wait(30)
        return m.gen

    with running(app, threads=2, resources={"vram_gb": 7.5}, pools={"gemma": 1, "tts": 1}, **LAZY):
        try:
            held = hold.send()
            assert started.wait(10)
            waiting = t.send()
            with pytest.raises(TimeoutError):
                waiting.result(timeout=0.5)  # no room while gemma is busy
            assert dev.closed == [] and dev.builds("tts") == []
        finally:
            release.set()
        assert held.result(timeout=10) == 0
        assert waiting.result(timeout=10) == ["tts", 0, 0]
        assert dev.closed == [("gemma", 0)]
    assert dev.overcommits == []


def test_a_failed_load_fails_the_task_and_gives_the_memory_back(app, running):
    dev, g, t = _two_models(app)
    broken = threading.Event()
    broken.set()
    dev.fail["tts"] = broken.is_set

    with running(app, threads=1, resources={"vram_gb": 7.5}, pools={"gemma": 1, "tts": 1}, **LAZY) as w:
        assert g.send().result(timeout=10) == ["gemma", 0, 0]
        with pytest.raises(TaskFailed, match=r"pool 'tts' failed to load: OSError\('cannot load tts'\)"):
            t.send().result(timeout=10)
        assert dev.closed == [("gemma", 0)], "the model it evicted stays evicted"
        assert g.send().result(timeout=10) == ["gemma", 0, 1], "the failed load took nothing"
        broken.clear()
        assert t.send().result(timeout=10) == ["tts", 0, 0]
        assert w.restart_reason is None
        _eventually(lambda: _series(w.metrics_port, 'mayu_pool_loaded{pool="tts"}') == 1.0)
        assert _series(w.metrics_port, 'mayu_pool_loaded{pool="gemma"}') == 0.0
    assert dev.overcommits == []


def test_retry_on_applies_to_a_failed_load(app, running):
    dev = Device(8)
    dev.pool(app, "gemma", 7.4)
    broken = threading.Event()
    broken.set()
    dev.fail["gemma"] = broken.is_set

    @app.task(retry_on=(RuntimeError,), max_retries=3)
    def g(m=app.use("gemma")):
        return m.gen

    @app.task
    def fix():
        broken.clear()

    with running(app, threads=1, resources={"vram_gb": 8}, pools={"gemma": 1}, **LAZY):
        handle = g.send()
        _eventually(lambda: dev.failed >= 1)  # the first attempt failed to load and went back for a retry
        fix.send().result(timeout=10)
        assert handle.result(timeout=20) == 0
    assert dev.failed >= 1 and len(dev.builds("gemma")) == 1


def test_a_lazy_instance_discarded_is_closed_not_rebuilt_and_gives_its_index_back(app, running):
    dev = Device(20)
    dev.pool(app, "gemma", 7.4)

    @app.task
    def probe(m=app.use("gemma")):
        return [m.index, m.gen]

    @app.task
    def broken(m=app.use("gemma")):
        raise DiscardInstance("out of memory")

    with running(app, threads=1, resources={"vram_gb": 20}, pools={"gemma": 2}, **LAZY) as w:
        assert probe.send().result(timeout=10) == [0, 0]
        with pytest.raises(TaskFailed, match="DiscardInstance"):
            broken.send().result(timeout=10)
        _eventually(lambda: dev.closed == [("gemma", 0)])
        assert len(dev.built) == 1, "nothing rebuilds it"
        _eventually(lambda: _series(w.metrics_port, 'mayu_pool_loaded{pool="gemma"}') == 0.0)
        assert probe.send().result(timeout=10) == [0, 1], "the next task loads one, into the index that was freed"
        assert _series(w.metrics_port, 'mayu_pool_evictions_total{pool="gemma"}') is None, "a discard is no eviction"
        assert _series(w.metrics_port, 'mayu_pool_loads_total{pool="gemma"}') == 2.0
        assert _series(w.metrics_port, 'mayu_pool_size{pool="gemma"}') == 2.0
        assert w.restart_reason is None
    assert dev.built == [("gemma", 0, 2, 0), ("gemma", 0, 2, 1)] and dev.overcommits == []


def test_hard_time_limit_keeps_the_zombies_instance_and_its_memory(app, running):
    dev = Device(13)
    dev.pool(app, "gemma", 6)
    dev.pool(app, "tts", 8)
    release = threading.Event()

    @app.task(hard_time_limit=0.3)
    def stuck(m=app.use("gemma")):
        release.wait(30)

    @app.task
    def probe(m=app.use("gemma")):
        return [m.index, m.gen]

    @app.task
    def t(m=app.use("tts")):
        return m.gen

    with running(app, threads=2, resources={"vram_gb": 13}, pools={"gemma": 2, "tts": 1}, shutdown_grace=2,
                 **LAZY) as w:
        try:
            with pytest.raises(TaskFailed, match="TimeLimitExceeded"):
                stuck.send().result(timeout=10)
            assert _series(w.metrics_port, 'mayu_pool_size{pool="gemma"}') == 1.0
            assert probe.send().result(timeout=10) == [1, 1]  # the zombie still holds instance 0
            waiting = t.send()  # 6 (zombie) + 8 > 13: it can never run, and evicting the idle gemma would not help
            with pytest.raises(TimeoutError):
                waiting.result(timeout=0.5)
            assert dev.closed == [] and w.restart_reason is None
            assert _series(w.metrics_port, 'mayu_pool_loaded{pool="gemma"}') == 1.0
        finally:
            release.set()
    assert dev.closed == [("gemma", 1)], "only the idle instance is closed; the zombie's may still be in use"
    assert dev.overcommits == []


def test_a_task_that_fails_before_it_runs_undoes_its_planned_load(app, running, redis_url, queue):
    dev, g, _ = _two_models(app)
    producer = App(broker=redis_url, result_ttl=60, default_queue=queue, serializer="pickle")

    @producer.declare(name=g.name)
    def pickled(m=producer.use("gemma")): ...

    with running(app, threads=1, resources={"vram_gb": 7.5}, pools={"gemma": 1, "tts": 1}, **LAZY) as w:
        with pytest.raises(TaskFailed, match="refusing pickle payload"):
            pickled.send().result(timeout=10)  # planned to load gemma, then refused: nothing was built
        assert dev.built == []
        assert g.send().result(timeout=10) == ["gemma", 0, 0]
        assert len(dev.built) == 1
        assert _series(w.metrics_port, 'mayu_pool_loaded{pool="gemma"}') == 1.0


def test_preloaded_pools_are_built_at_start_in_order(app, running):
    dev, g, t = _two_models(app, budget=15)
    with running(app, threads=1, resources={"vram_gb": 15}, pools={"gemma": 1, "tts": 1}, preload=["tts", "gemma"],
                 **LAZY) as w:
        assert [b[0] for b in dev.built] == ["tts", "gemma"]
        assert _series(w.metrics_port, 'mayu_pool_loaded{pool="gemma"}') == 1.0
        assert _series(w.metrics_port, 'mayu_pool_loads_total{pool="tts"}') == 1.0
        assert g.send().result(timeout=10) == ["gemma", 0, 0] and t.send().result(timeout=10) == ["tts", 0, 0]
        assert len(dev.built) == 2 and dev.closed == []


def test_a_preload_failure_closes_what_was_built(app, queue):
    dev, _, _ = _two_models(app, budget=15)
    dev.fail["gemma"] = lambda: True
    w = WorkerRunner(app, threads=1, queues=[queue], resources={"vram_gb": 15}, pools={"gemma": 1, "tts": 1},
                     pool_mode="lazy", preload=["tts", "gemma"])
    with pytest.raises(RuntimeError, match=r"pool 'gemma' instance 0 failed to load: OSError\('cannot load gemma'\)"):
        w.start()
    assert dev.closed == [("tts", 0)] and w.native is None


def test_metrics_after_a_swap(app, running):
    dev, g, t = _two_models(app)
    with running(app, threads=1, resources={"vram_gb": 7.5}, pools={"gemma": 1, "tts": 1}, **LAZY) as w:
        g.send().result(timeout=10)
        t.send().result(timeout=10)
        port = w.metrics_port
        assert _series(port, 'mayu_pool_loaded{pool="gemma"}') == 0.0
        assert _series(port, 'mayu_pool_loaded{pool="tts"}') == 1.0
        assert _series(port, 'mayu_pool_loads_total{pool="gemma"}') == 1.0
        assert _series(port, 'mayu_pool_loads_total{pool="tts"}') == 1.0
        assert _series(port, 'mayu_pool_evictions_total{pool="gemma"}') == 1.0
        assert _series(port, 'mayu_pool_evictions_total{pool="tts"}') is None
        assert _series(port, 'mayu_pool_size{pool="gemma"}') == 1.0


def test_eager_workers_have_no_lazy_metrics(app, running):
    app.pool("gemma")(lambda ctx: object())

    @app.task
    def g(m=app.use("gemma")):
        return 1

    with running(app, threads=1, pools={"gemma": 1}, metrics_port=0, metrics_host="127.0.0.1") as w:
        assert g.send().result(timeout=10) == 1
        text = _scrape(w.metrics_port)
    assert "mayu_pool_loaded" not in text and "mayu_pool_loads_total" not in text


def test_lazy_instances_log_their_load(app, running, caplog):
    _, g, _ = _two_models(app)
    logger = logging.getLogger("mayu.worker")
    logger.addHandler(caplog.handler)
    caplog.set_level(logging.INFO, logger="mayu.worker")
    try:
        with running(app, threads=1, resources={"vram_gb": 7.5}, pools={"gemma": 1, "tts": 1}, **LAZY):
            g.send().result(timeout=10)
    finally:
        logger.removeHandler(caplog.handler)
    loaded = list({id(r): r for r in caplog.records if r.getMessage() == "pool instance loaded"}.values())
    assert len(loaded) == 1 and loaded[0].pool == "gemma" and loaded[0].load_seconds >= 0


# -- the pool state ---------------------------------------------------------------------------------------------


def test_pool_state_hands_out_the_lowest_index_and_orders_instances_by_recency():
    from mayu.worker import _PoolState

    state = _PoolState(object(), 3, lazy=True)
    assert [state.take_index() for _ in range(3)] == [0, 1, 2] and state.take_index() is None
    state.give_index(1)
    state.give_index(0)
    assert state.take_index() == 0
    for i in range(3):
        state.push(i, f"m{i}")
    assert state.take_recent() == (2, "m2") and state.take_lru() == (0, "m0") and state.take_recent() == (1, "m1")
    assert state.take_recent() is None and state.take_lru() is None


# -- constructor validation -----------------------------------------------------------------------------------------


def _validated(app):
    app.pool("gemma", vram_gb=7.4)(lambda ctx: None)
    app.pool("tts", vram_gb=7.2)(lambda ctx: None)
    app.pool("spare", vram_gb=1)(lambda ctx: None)

    @app.task(name="v.g")
    def g(m=app.use("gemma")): ...

    @app.task(name="v.a_t", resources={"vram_gb": 0.2})
    def t(m=app.use("tts")): ...


def _runner(app, queue, **kw):
    kw.setdefault("resources", {"vram_gb": 7.5})
    kw.setdefault("pools", {"gemma": 1, "tts": 1})
    return WorkerRunner(app, threads=1, queues=[queue], **kw)


@pytest.mark.parametrize("kw,message", [
    ({"pool_mode": "sometimes"}, "pool_mode must be 'eager' or 'lazy', got 'sometimes'"),
    ({"pool_mode": "lazy", "schedule": "fastest"}, "schedule must be 'throughput' or 'oldest', got 'fastest'"),
    ({"schedule": "oldest"}, "--schedule needs --pool-mode lazy"),
    ({"preload": ["gemma"]}, "--preload needs --pool-mode lazy"),
    ({"pool_mode": "lazy", "preload": ["ghost"]},
     "--preload names pool 'ghost', which this process does not define; import the module with @app.pool('ghost')"),
    ({"pool_mode": "lazy", "preload": ["spare"]},
     "--preload names pool 'spare' but this worker has no instances of it; pass --pools spare=N"),
    ({"pool_mode": "lazy", "preload": ["spare"], "pools": {"gemma": 1, "tts": 1, "spare": 1}},
     "--preload names pool 'spare', which no task on the served queues uses"),
    ({"pool_mode": "lazy", "preload": ["gemma", "gemma"]}, "--preload lists pool 'gemma' more than once"),
    ({"pool_mode": "lazy", "preload": ["gemma", "tts"]},
     "preloading gemma, tts needs vram_gb=14.6 but this worker has vram_gb=7.5; preload fewer pools or raise "
     "--resources vram_gb=..."),
    ({"pool_mode": "lazy", "resources": {"vram_gb": 7.35}},
     "task 'v.a_t' needs vram_gb=7.4 (tts 7.2 + 0.2 of its own) but this worker has vram_gb=7.35; "
     "raise --resources vram_gb=... or route the task to a bigger worker"),
])
def test_lazy_options_are_validated(app, queue, kw, message):
    _validated(app)
    with pytest.raises(ValueError) as e:
        _runner(app, queue, **kw)
    assert message in str(e.value)


def test_preload_must_be_a_list_of_names(app, queue):
    _validated(app)
    for bad in ("gemma", [1]):
        with pytest.raises(TypeError, match="preload must be a list of pool names"):
            _runner(app, queue, pool_mode="lazy", preload=bad)


def test_a_lazy_worker_reserves_nothing_and_may_have_more_instances_than_fit(app, queue):
    _validated(app)
    w = _runner(app, queue, pool_mode="lazy", pools={"gemma": 3, "tts": 1}, preload=["gemma"])
    assert w.capacity == {"vram_gb": 7.5, "pool:gemma": 3.0, "pool:tts": 1.0}
    assert w.task_resources == {"v.g": {"pool:gemma": 1.0}, "v.a_t": {"vram_gb": 0.2, "pool:tts": 1.0}}
    assert w.lazy_pools == {"gemma": ({"vram_gb": 7.4}, 1), "tts": ({"vram_gb": 7.2}, 0)}
    assert w.schedule == "oldest"
    assert _runner(app, queue, pool_mode="lazy", schedule="throughput").schedule == "throughput"
    eager = _runner(app, queue, resources={"vram_gb": 20}, pools={"gemma": 1, "tts": 1})
    assert eager.lazy_pools == {} and eager.schedule is None and eager.capacity["vram_gb"] == 5.4


def test_a_task_needing_a_model_larger_than_the_worker_is_refused(app, queue):
    app.pool("big", vram_gb=9)(lambda ctx: None)

    @app.task(name="x")
    def x(m=app.use("big")): ...

    with pytest.raises(ValueError) as e:
        _runner(app, queue, pool_mode="lazy", pools={"big": 1})
    assert str(e.value) == ("task 'x' needs vram_gb=9 (big 9) but this worker has vram_gb=7.5; "
                            "raise --resources vram_gb=... or route the task to a bigger worker")


def test_a_lazy_worker_without_pools_is_an_ordinary_one(app, queue):
    @app.task
    def plain(): ...

    w = WorkerRunner(app, threads=1, queues=[queue], pool_mode="lazy")
    assert not w.lazy and w.schedule is None and w.lazy_pools == {}
    w.start()
    try:
        assert plain.send().result(timeout=10) is None
    finally:
        w.stop()
        w.join()
