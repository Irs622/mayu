"""Parked models: a lazy pool's idle instance moves to host RAM instead of being closed, and is staged ahead of need."""
import collections
import re
import threading
import time
import urllib.request

import pytest

from mayu import TaskFailed
from mayu.worker import WorkerRunner, _PoolState


class Model:
    def __init__(self, name, index, gen, where):
        self.name = name
        self.index = index
        self.gen = gen  # which build of this pool it is
        self.where = where  # "gpu" or "ram"


class Rig:
    """A fake GPU and host RAM. Hooks move a `Model` between them and record every call; a move that would
    overcommit a budget, or that starts from the wrong place, is recorded instead of raised."""

    def __init__(self, gpu, ram):
        self.lock = threading.Lock()
        self.budget = {"gpu": gpu, "ram": ram}
        self.used = {"gpu": 0.0, "ram": 0.0}
        self.gens = collections.Counter()
        self.size = {}  # pool -> (vram, ram)
        self.fail = set()  # (op, pool) whose hook raises
        self.log = []  # (op, pool, gen)
        self.closed = []  # (pool, gen, where)
        self.errors = []
        self.seen = {}  # (op, pool) -> Event set when the op has happened

    def pool(self, app, name, vram, ram, stage=False, **options):
        self.size[name] = (vram, ram)
        for op in ("build", "park", "unpark", "stage"):
            self.seen[(op, name)] = threading.Event()
        pool = app.pool(name, vram_gb=vram, parked={"ram_gb": ram}, **options)(self._build)
        pool.park(self._park)
        pool.unpark(self._unpark)
        if stage:
            pool.stage(self._stage)
        pool.close(self._close)
        return pool

    def _enter(self, where, name, amount, op):
        self.used[where] += amount
        if self.used[where] > self.budget[where] + 1e-9:
            self.errors.append(f"{op} {name} overcommits {where}: {self.used[where]} of {self.budget[where]}")

    def _hook(self, op, model):
        if (op, model.name) in self.fail:
            raise OSError(f"cannot {op} {model.name}")

    def _build(self, ctx):
        time.sleep(0.002)  # a slow move gives a concurrent mistake the chance to show
        with self.lock:
            if ("build", ctx.name) in self.fail:
                raise OSError(f"cannot build {ctx.name}")
            gen = self.gens[ctx.name]
            self.gens[ctx.name] += 1
            self._enter("gpu", ctx.name, self.size[ctx.name][0], "build")
            self.log.append(("build", ctx.name, gen))
        self.seen[("build", ctx.name)].set()
        return Model(ctx.name, ctx.index, gen, "gpu")

    def _stage(self, ctx):
        time.sleep(0.002)
        with self.lock:
            if ("stage", ctx.name) in self.fail:
                raise OSError(f"cannot stage {ctx.name}")
            gen = self.gens[ctx.name]
            self.gens[ctx.name] += 1
            self._enter("ram", ctx.name, self.size[ctx.name][1], "stage")
            self.log.append(("stage", ctx.name, gen))
        self.seen[("stage", ctx.name)].set()
        return Model(ctx.name, ctx.index, gen, "ram")

    def _park(self, m):
        time.sleep(0.002)
        with self.lock:
            self._hook("park", m)
            if m.where != "gpu":
                self.errors.append(f"park {m.name} from {m.where}")
            vram, ram = self.size[m.name]
            self.used["gpu"] -= vram
            self._enter("ram", m.name, ram, "park")
            m.where = "ram"
            self.log.append(("park", m.name, m.gen))
        self.seen[("park", m.name)].set()

    def _unpark(self, m):
        time.sleep(0.002)
        with self.lock:
            self._hook("unpark", m)
            if m.where != "ram":
                self.errors.append(f"unpark {m.name} from {m.where}")
            vram, ram = self.size[m.name]
            self.used["ram"] -= ram
            self._enter("gpu", m.name, vram, "unpark")
            m.where = "gpu"
            self.log.append(("unpark", m.name, m.gen))
        self.seen[("unpark", m.name)].set()

    def _close(self, m):
        with self.lock:
            vram, ram = self.size[m.name]
            self.used[m.where] -= vram if m.where == "gpu" else ram
            self.closed.append((m.name, m.gen, m.where))
            self.log.append(("close", m.name, m.gen))

    def ops(self, op, name=None):
        return [e for e in self.log if e[0] == op and (name is None or e[1] == name)]


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


def _in_use(w, resource):
    return next(in_use for name, _, in_use in w.native.resource_usage() if name == resource)


LAZY = dict(pool_mode="lazy", metrics_port=0, metrics_host="127.0.0.1")
RES = {"vram_gb": 7.5, "ram_gb": 16}


def _two_models(app, **gemma_tts):
    rig = Rig(7.5, 16)
    rig.pool(app, "gemma", 7.4, 6.0)
    rig.pool(app, "tts", 7.2, 6.6, **gemma_tts)

    @app.task
    def g(m=app.use("gemma")):
        return [m.name, m.gen, m.where]

    @app.task
    def t(m=app.use("tts")):
        return [m.name, m.gen, m.where]

    return rig, g, t


def _sound(rig, w):
    """The ledger and the rig agree on how much of each resource is in use, and no move was illegal."""
    _eventually(lambda: abs(_in_use(w, "vram_gb") - rig.used["gpu"]) < 1e-6
                and abs(_in_use(w, "ram_gb") - rig.used["ram"]) < 1e-6)
    assert rig.errors == []


# -- park and unpark ------------------------------------------------------------------------------------------------


def test_a_swap_parks_the_loaded_model_and_unparks_the_other(app, running):
    rig, g, t = _two_models(app)
    with running(app, threads=1, resources=RES, pools={"gemma": 1, "tts": 1}, **LAZY) as w:
        assert g.send().result(timeout=10) == ["gemma", 0, "gpu"]
        assert t.send().result(timeout=10) == ["tts", 0, "gpu"]
        assert g.send().result(timeout=10) == ["gemma", 0, "gpu"], "the same instance, brought back"
        assert rig.log == [("build", "gemma", 0), ("park", "gemma", 0), ("build", "tts", 0), ("park", "tts", 0),
                           ("unpark", "gemma", 0)]
        port = w.metrics_port
        for pool, parks, unparks, parked in (("gemma", 1.0, 1.0, 0.0), ("tts", 1.0, None, 1.0)):
            assert _series(port, f'mayu_pool_parks_total{{pool="{pool}"}}') == parks
            assert _series(port, f'mayu_pool_unparks_total{{pool="{pool}"}}') == unparks
            assert _series(port, f'mayu_pool_parked{{pool="{pool}"}}') == parked
            assert _series(port, f'mayu_pool_loads_total{{pool="{pool}"}}') == 1.0, "built once per pool"
            assert _series(port, f'mayu_pool_evictions_total{{pool="{pool}"}}') is None
        move = 'mayu_pool_move_seconds_count{pool="gemma",kind="%s"}'
        assert [_series(port, move % k) for k in ("build", "park", "unpark", "stage")] == [1.0, 1.0, 1.0, None]
        _sound(rig, w)
        assert rig.used == {"gpu": 7.4, "ram": 6.6}
    assert sorted(rig.closed) == [("gemma", 0, "gpu"), ("tts", 0, "ram")], "shutdown closes the parked one too"


def test_a_park_hook_that_raises_closes_the_instance_and_the_task_still_runs(app, running):
    rig, g, t = _two_models(app)
    rig.fail.add(("park", "gemma"))
    with running(app, threads=1, resources=RES, pools={"gemma": 1, "tts": 1}, **LAZY) as w:
        g.send().result(timeout=10)
        assert t.send().result(timeout=10) == ["tts", 0, "gpu"]
        assert rig.closed == [("gemma", 0, "gpu")]
        port = w.metrics_port
        assert _series(port, 'mayu_pool_evictions_total{pool="gemma"}') == 1.0
        assert _series(port, 'mayu_pool_parks_total{pool="gemma"}') is None
        assert _series(port, 'mayu_pool_parked{pool="gemma"}') == 0.0
        _sound(rig, w)
        rig.fail.clear()
        assert g.send().result(timeout=10) == ["gemma", 1, "gpu"], "nothing was parked: built again"
        assert rig.ops("park", "tts") == [("park", "tts", 0)]
        _sound(rig, w)


def test_an_unpark_hook_that_raises_closes_the_instance_and_the_task_builds_a_fresh_one(app, running):
    rig, g, t = _two_models(app)
    with running(app, threads=1, resources=RES, pools={"gemma": 1, "tts": 1}, **LAZY) as w:
        g.send().result(timeout=10)
        t.send().result(timeout=10)  # gemma parked
        rig.fail.add(("unpark", "gemma"))
        assert g.send().result(timeout=10) == ["gemma", 1, "gpu"]
        assert ("close", "gemma", 0) in rig.log
        assert rig.closed[0] == ("gemma", 0, "ram"), "the half-moved instance is closed, in whatever state it is in"
        port = w.metrics_port
        assert _series(port, 'mayu_pool_unparks_total{pool="gemma"}') is None
        assert _series(port, 'mayu_pool_loads_total{pool="gemma"}') == 2.0
        assert _series(port, 'mayu_pool_loaded{pool="gemma"}') == 1.0
        assert _series(port, 'mayu_pool_parked{pool="gemma"}') == 0.0
        assert _series(port, 'mayu_pool_parked{pool="tts"}') == 1.0
        _sound(rig, w)


def test_a_failed_fresh_build_after_a_failed_unpark_gives_the_room_back(app, running):
    rig, g, t = _two_models(app)
    with running(app, threads=1, resources=RES, pools={"gemma": 1, "tts": 1}, **LAZY) as w:
        g.send().result(timeout=10)
        t.send().result(timeout=10)  # gemma parked
        rig.fail.update({("unpark", "gemma"), ("build", "gemma")})
        with pytest.raises(TaskFailed, match="pool 'gemma' failed to load"):
            g.send().result(timeout=10)
        port = w.metrics_port
        _eventually(lambda: _series(port, 'mayu_pool_loaded{pool="gemma"}') == 0.0)
        assert _series(port, 'mayu_pool_parked{pool="tts"}') == 1.0, "the park for the task still happened"
        _sound(rig, w)
        assert rig.used == {"gpu": 0.0, "ram": 6.6}
        rig.fail.clear()
        assert g.send().result(timeout=10) == ["gemma", 1, "gpu"]
        assert t.send().result(timeout=10) == ["tts", 0, "gpu"], "the parked tts is still good"
        _sound(rig, w)


def test_without_ram_room_the_victim_is_evicted_not_parked(app, running):
    rig = Rig(7.5, 7)
    rig.pool(app, "gemma", 7.4, 6.0)
    rig.pool(app, "tts", 7.2, 6.0)
    started, release = threading.Event(), threading.Event()

    @app.task(resources={"ram_gb": 5})
    def hog():
        started.set()
        release.wait(30)

    @app.task
    def g(m=app.use("gemma")):
        return [m.name, m.gen, m.where]

    @app.task
    def t(m=app.use("tts")):
        return [m.name, m.gen, m.where]

    with running(app, threads=2, resources={"vram_gb": 7.5, "ram_gb": 7}, pools={"gemma": 1, "tts": 1}, **LAZY) as w:
        g.send().result(timeout=10)
        hog.send()
        try:
            assert started.wait(10)
            assert t.send().result(timeout=10) == ["tts", 0, "gpu"]
        finally:
            release.set()
        assert rig.closed == [("gemma", 0, "gpu")] and rig.ops("park") == []
        assert _series(w.metrics_port, 'mayu_pool_evictions_total{pool="gemma"}') == 1.0
        assert _series(w.metrics_port, 'mayu_pool_parks_total{pool="gemma"}') is None
        _sound(rig, w)


def test_a_parked_instance_is_dropped_when_another_park_needs_its_room(app, running):
    rig = Rig(7.5, 7)
    for name, vram in (("gemma", 7.4), ("tts", 7.2), ("vox", 7.3)):
        rig.pool(app, name, vram, 6.0)

    def make(pool):
        @app.task(name=f"use_{pool}")
        def use(m=app.use(pool)):
            return [m.name, m.gen, m.where]

        return use

    g, t, v = make("gemma"), make("tts"), make("vox")
    with running(app, threads=1, resources={"vram_gb": 7.5, "ram_gb": 7},
                 pools={"gemma": 1, "tts": 1, "vox": 1}, **LAZY) as w:
        g.send().result(timeout=10)
        t.send().result(timeout=10)  # gemma parked
        assert v.send().result(timeout=10) == ["vox", 0, "gpu"]  # tts parks, so gemma's parked copy goes
        assert [e for e in rig.log if e[0] in ("park", "close")] == [
            ("park", "gemma", 0), ("close", "gemma", 0), ("park", "tts", 0)], "drops come before parks"
        assert rig.closed == [("gemma", 0, "ram")]
        port = w.metrics_port
        assert _series(port, 'mayu_pool_drops_total{pool="gemma"}') == 1.0
        assert _series(port, 'mayu_pool_parked{pool="gemma"}') == 0.0
        assert _series(port, 'mayu_pool_parked{pool="tts"}') == 1.0
        _sound(rig, w)


# -- staging --------------------------------------------------------------------------------------------------------


def _workflow(app, release, started=None, gemma=False):
    """a (holds the GPU resource, and the gemma model if `gemma`, until `release`) -> b (needs tts)."""
    @app.task(resources={"gpu": 1})
    def a(hold: float = 60.0, m=app.use("gemma") if gemma else None):
        if started is not None:
            started.set()
        release.wait(hold)

    @app.task
    def b(m=app.use("tts")):
        return [m.name, m.gen, m.where]

    wf = app.workflow("parked")
    na = wf.add(a)
    nb = wf.add(b, after=[na])
    return a, wf, nb


STAGING = dict(RES, gpu=1)


def test_a_model_is_staged_while_the_node_before_it_runs_and_unparked_for_its_task(app, running, caplog):
    rig = Rig(7.5, 16)
    rig.pool(app, "tts", 7.2, 6.6, stage=True)
    release, started = threading.Event(), threading.Event()
    _, wf, nb = _workflow(app, release, started)
    caplog.set_level("INFO", logger="mayu.worker")
    with running(app, threads=2, resources=STAGING, pools={"tts": 1}, lookahead=True, **LAZY) as w:
        try:
            handle = wf.send()
            assert started.wait(10)
            assert rig.seen[("stage", "tts")].wait(10), "staged while a still holds the GPU"
            logged = [r.getMessage() for r in caplog.records if r.getMessage().startswith("staging pool tts")]
            assert len(logged) == 1 and re.fullmatch(
                r"staging pool tts into host RAM: needed in ~\d+ s \(0/1 of its tasks done\)", logged[0]
            ), logged
            assert not release.is_set()
            assert rig.seen[("unpark", "tts")].wait(10), "and moved to the GPU ahead of b, which still waits"
            assert _series(w.metrics_port, 'mayu_pool_stages_total{pool="tts"}') == 1.0
        finally:
            release.set()
        assert handle.node(nb).result(timeout=10) == ["tts", 0, "gpu"]
        assert rig.ops("build") == [], "the factory never ran"
        assert [e[0] for e in rig.log] == ["stage", "unpark"]
        assert _series(w.metrics_port, 'mayu_pool_unparks_total{pool="tts"}') == 1.0
        assert _series(w.metrics_port, 'mayu_pool_move_seconds_count{pool="tts",kind="stage"}') == 1.0
        assert _series(w.metrics_port, 'mayu_pool_move_seconds_count{pool="tts",kind="unpark"}') == 1.0
        _sound(rig, w)
    assert rig.closed == [("tts", 0, "gpu")]


def test_staging_waits_until_the_model_is_needed_within_its_lead_time(app, running):
    rig = Rig(7.5, 16)
    rig.pool(app, "gemma", 7.4, 6.0)  # holds the GPU while a runs, so tts is not built ahead (lookahead)
    rig.pool(app, "tts", 7.2, 6.6, stage=True)
    release = threading.Event()
    a, wf, nb = _workflow(app, release, gemma=True)
    with running(app, threads=2, resources=STAGING, pools={"gemma": 1, "tts": 1}, lookahead=True, **LAZY) as w:
        port = w.metrics_port
        try:
            a.send(hold=6.5).result(timeout=30)  # a's run time on this worker: ~6.5 s
            # stage + unpark take ~0, so a stage is due STAGE_MARGIN (5 s) before the model is needed
            w.native.pool_timing("tts", "stage", 0.01)
            w.native.pool_timing("tts", "unpark", 0.01)
            lead = 5.02
            handle = wf.send()
            _eventually(lambda: _series(port, 'mayu_pool_next_need_seconds{pool="tts"}') is not None)
            need = _series(port, 'mayu_pool_next_need_seconds{pool="tts"}')
            assert lead < need <= 6.6, "b is expected to be needed in about as long as a takes"
            assert not rig.seen[("stage", "tts")].is_set(), "too early to stage"
            assert rig.seen[("stage", "tts")].wait(20), "the window opens as a runs"
            assert _series(port, 'mayu_pool_next_need_seconds{pool="tts"}') <= lead
            assert not release.is_set()
        finally:
            release.set()
        assert handle.node(nb).result(timeout=10) == ["tts", 0, "gpu"]
        assert rig.ops("build", "tts") == [], "b unparked the staged instance"
        assert [e[:2] for e in rig.log if e[0] in ("stage", "park", "unpark")] == [
            ("stage", "tts"), ("park", "gemma"), ("unpark", "tts")]
        _sound(rig, w)


def test_a_failed_stage_is_logged_and_the_task_builds_the_model_itself(app, running, caplog):
    rig = Rig(7.5, 16)
    rig.pool(app, "tts", 7.2, 6.6, stage=True)
    rig.fail.add(("stage", "tts"))
    release, started = threading.Event(), threading.Event()
    _, wf, nb = _workflow(app, release, started)
    caplog.set_level("WARNING", logger="mayu.worker")
    with running(app, threads=2, resources=STAGING, pools={"tts": 1}, lookahead=True, **LAZY) as w:
        try:
            handle = wf.send()
            assert started.wait(10)
            _eventually(lambda: any("staging pool 'tts' failed" in r.getMessage() for r in caplog.records))
            assert not any(e[0] == "stage" for e in rig.log)
            assert _series(w.metrics_port, 'mayu_pool_parked{pool="tts"}') == 0.0, "the room was given back"
        finally:
            release.set()
        assert handle.node(nb).result(timeout=10) == ["tts", 0, "gpu"]
        assert rig.ops("build") == [("build", "tts", 0)]
        assert _series(w.metrics_port, 'mayu_pool_stages_total{pool="tts"}') is None
        _sound(rig, w)


def test_a_failed_lookahead_unpark_closes_the_instance_and_the_task_builds_the_model_itself(app, running):
    rig = Rig(7.5, 16)
    rig.pool(app, "tts", 7.2, 6.6, stage=True)
    rig.fail.add(("unpark", "tts"))
    release, started = threading.Event(), threading.Event()
    _, wf, nb = _workflow(app, release, started)
    with running(app, threads=2, resources=STAGING, pools={"tts": 1}, lookahead=True, **LAZY) as w:
        try:
            handle = wf.send()
            assert started.wait(10)
            assert rig.seen[("stage", "tts")].wait(10)
            _eventually(lambda: ("close", "tts", 0) in rig.log)  # the unpark raised
            assert rig.closed == [("tts", 0, "ram")]
        finally:
            release.set()
        name, gen, where = handle.node(nb).result(timeout=10)
        assert (name, where) == ("tts", "gpu")
        # the dispatcher may stage again after the failed unpark (only a failed stage backs it off), and b's own
        # unpark then fails too: either way b ends up on an instance the factory built
        assert rig.ops("build", "tts") == [("build", "tts", gen)]
        assert _series(w.metrics_port, 'mayu_pool_unparks_total{pool="tts"}') is None
        _sound(rig, w)


# -- shutdown -------------------------------------------------------------------------------------------------------


def test_shutdown_closes_a_staged_instance_that_was_never_used(app, queue):
    rig = Rig(7.5, 16)
    rig.pool(app, "gemma", 7.4, 6.0)
    rig.pool(app, "tts", 7.2, 6.6, stage=True)
    release, started = threading.Event(), threading.Event()
    _, wf, _ = _workflow(app, release, started, gemma=True)  # gemma holds the GPU, so tts is staged, not unparked
    w = WorkerRunner(app, threads=2, queues=[queue], resources=STAGING, pools={"gemma": 1, "tts": 1}, lookahead=True,
                     shutdown_grace=20, **LAZY)
    w.start()
    try:
        wf.send()
        assert started.wait(10)
        assert rig.seen[("stage", "tts")].wait(10)
        w.stop()
    finally:
        release.set()
    w.join()
    assert sorted(rig.closed) == [("gemma", 0, "gpu"), ("tts", 0, "ram")], "each exactly once"
    assert rig.used == {"gpu": 0.0, "ram": 0.0} and rig.errors == []


def test_join_waits_for_a_stage_in_progress_and_closes_what_it_built(app, queue):
    rig = Rig(7.5, 16)
    rig.pool(app, "tts", 7.2, 6.6, stage=True)
    entered, gate = threading.Event(), threading.Event()
    stage = app.pools["tts"].stager

    def slow(ctx):
        entered.set()
        gate.wait(30)
        return stage(ctx)

    app.pools["tts"].stager = slow
    release = threading.Event()
    _, wf, _ = _workflow(app, release)
    w = WorkerRunner(app, threads=2, queues=[queue], resources=STAGING, pools={"tts": 1}, lookahead=True,
                     shutdown_grace=20, **LAZY)
    w.start()
    joiner = threading.Thread(target=w.join, daemon=True)
    try:
        wf.send()
        assert entered.wait(10)
        w.stop()
        release.set()
        joiner.start()
        _eventually(w._halt.is_set)  # join is past the task threads and waits for the stage
        joiner.join(0.2)
        assert joiner.is_alive() and rig.closed == []
    finally:
        release.set()
        gate.set()
    joiner.join(10)
    assert not joiner.is_alive()
    assert rig.closed == [("tts", 0, "ram")]


# -- concurrency ----------------------------------------------------------------------------------------------------


def test_instances_stay_in_exactly_one_state_under_concurrent_swaps(app, running):
    rig, g, t = _two_models(app)
    with running(app, threads=4, resources=RES, pools={"gemma": 1, "tts": 1}, **LAZY) as w:
        handles = [(g if i % 2 == 0 else t).send() for i in range(40)]
        results = [h.result(timeout=60) for h in handles]
        assert [r[0] for r in results] == ["gemma", "tts"] * 20
        assert all(r[2] == "gpu" for r in results), "a task always runs on the GPU"
        port = w.metrics_port
        _sound(rig, w)
        for pool in ("gemma", "tts"):
            parks = _series(port, f'mayu_pool_parks_total{{pool="{pool}"}}') or 0.0
            unparks = _series(port, f'mayu_pool_unparks_total{{pool="{pool}"}}') or 0.0
            parked = _series(port, f'mayu_pool_parked{{pool="{pool}"}}')
            loaded = _series(port, f'mayu_pool_loaded{{pool="{pool}"}}')
            assert parks - unparks == parked, f"{pool}: the counters add up to the parked gauge"
            assert parked + loaded == 1.0, f"{pool}: one instance, in one place"
        assert dict(w.native.parked_usage()) == {p: int(_series(port, f'mayu_pool_parked{{pool="{p}"}}'))
                                                 for p in ("gemma", "tts")}
        assert rig.used in ({"gpu": 7.4, "ram": 6.6}, {"gpu": 7.2, "ram": 6.0}), "one on the GPU, the other parked"
        assert rig.ops("build", "gemma") == [("build", "gemma", 0)] and rig.ops("build", "tts") == [("build", "tts", 0)]
    assert len(rig.closed) == 2 and len(set(rig.closed)) == 2


# -- the pool state -------------------------------------------------------------------------------------------------


def test_pool_state_keeps_a_parked_list_apart_from_the_idle_one():
    state = _PoolState(object(), 3, lazy=True)
    assert state.take_parked() is None and state.take_parked_lru() is None
    for i in range(3):
        state.push_parked(i, f"p{i}")
    state.push(9, "idle")
    assert state.take_parked() == (2, "p2") and state.take_parked_lru() == (0, "p0")
    assert state.take_recent() == (9, "idle") and state.take_recent() is None
    assert state.drain() == [] and state.drain_parked() == [(1, "p1")] and state.drain_parked() == []


def test_taking_a_parked_instance_waits_for_one_that_is_still_being_parked():
    state = _PoolState(object(), 1, lazy=True)
    threading.Timer(0.05, state.push_parked, (0, "late")).start()
    started = time.monotonic()
    assert state.take_parked(wait=10) == (0, "late")
    assert time.monotonic() - started < 5
    assert state.take_parked(wait=0.05) is None


# -- API and validation ---------------------------------------------------------------------------------------------


def _plain(app):
    app.pool("gemma", vram_gb=7.4)(lambda ctx: None)

    @app.task(name="v.g")
    def g(m=app.use("gemma")): ...


def _runner(app, queue, pool, **kw):
    kw.setdefault("resources", dict(RES))
    kw.setdefault("pool_mode", "lazy")
    return WorkerRunner(app, threads=1, queues=[queue], pools={pool.name: 1}, **kw)


def _tts(app, **options):
    pool = app.pool("tts", vram_gb=7.2, **options)(lambda ctx: None)

    @app.task(name="v.t")
    def t(m=app.use("tts")): ...

    return pool


@pytest.mark.parametrize("name,attr", [("park", "parker"), ("unpark", "unparker"), ("stage", "stager")])
def test_hooks_register_once_and_return_the_function(app, name, attr):
    pool = app.pool("tts", vram_gb=7.2, parked={"ram_gb": 6.6})(lambda ctx: None)

    def hook(x): ...

    assert getattr(pool, name)(hook) is hook and getattr(pool, attr) is hook
    with pytest.raises(ValueError, match=f"pool 'tts' already has a {name} function"):
        getattr(pool, name)(lambda x: None)
    other = app.pool("other")(lambda ctx: None)
    with pytest.raises(TypeError, match=f"the {name} function of pool 'other' must be callable"):
        getattr(other, name)(3)
    assert getattr(other, attr) is None and pool.parked == {"ram_gb": 6.6}


def test_parked_is_checked_like_pool_resources(app):
    with pytest.raises(ValueError, match="parked of pool 'a' names no resource"):
        app.pool("a", parked={})
    with pytest.raises(TypeError, match="must be a number"):
        app.pool("a", parked={"ram_gb": "lots"})
    with pytest.raises(ValueError, match="at least 0.001"):
        app.pool("a", parked={"ram_gb": 0})
    with pytest.raises(TypeError, match="parked of pool 'a' must be a dict"):
        app.pool("a", parked=6.6)
    assert app.pool("b")(lambda ctx: None).parked is None
    assert app.pools.keys() == {"b"}


@pytest.mark.parametrize("hooks,message", [
    ((), "pool 'tts' has parked= but no park function; register one with the pool's @park decorator"),
    (("park",), "pool 'tts' has parked= but no unpark function; register one with the pool's @unpark decorator"),
    (("unpark",), "pool 'tts' has parked= but no park function"),
    (("park", "stage"), "pool 'tts' has parked= but no unpark function"),
])
def test_parked_needs_both_hooks(app, queue, hooks, message):
    pool = _tts(app, parked={"ram_gb": 6.6})
    for h in hooks:
        getattr(pool, h)(lambda *a: None)
    with pytest.raises(ValueError) as e:
        _runner(app, queue, pool)
    assert message in str(e.value)


@pytest.mark.parametrize("hook", ["park", "unpark", "stage"])
def test_hooks_need_parked(app, queue, hook):
    pool = _tts(app)
    getattr(pool, hook)(lambda *a: None)
    with pytest.raises(ValueError, match=f"pool 'tts' has a {hook} function but no parked="):
        _runner(app, queue, pool)


def test_parked_resources_must_be_declared_on_the_worker(app, queue):
    pool = _tts(app, parked={"ram_gb": 6.6})
    pool.park(lambda m: None)
    pool.unpark(lambda m: None)
    with pytest.raises(ValueError) as e:
        _runner(app, queue, pool, resources={"vram_gb": 7.5})
    assert str(e.value) == "pool 'tts' parks into ram_gb but this worker has no ram_gb; add --resources ram_gb=..."


def test_a_parked_claim_larger_than_the_worker_has_is_refused(app, queue):
    pool = _tts(app, parked={"ram_gb": 20})
    pool.park(lambda m: None)
    pool.unpark(lambda m: None)
    with pytest.raises(ValueError) as e:
        _runner(app, queue, pool)
    assert "pool 'tts' parks ram_gb=20 but this worker has ram_gb=16; raise --resources ram_gb=..." in str(e.value)


def test_eager_mode_ignores_parked_and_its_hooks(app, running):
    pool = app.pool("tts", parked={"ram_gb": 6.6}, linger=5)(lambda ctx: "model")  # no ram_gb, no hooks: fine

    @app.task
    def t(m=app.use("tts")):
        return m

    with running(app, threads=1, pools={"tts": 1}) as w:
        assert t.send().result(timeout=10) == "model"
        assert w.lazy is False and w._parkable is False and w._stageable is False
    assert pool.parker is None


def test_a_worker_without_parked_pools_is_told_nothing_new(app, queue):
    _plain(app)
    w = WorkerRunner(app, threads=1, queues=[queue], resources=dict(RES), pools={"gemma": 1}, pool_mode="lazy",
                     lookahead=True)
    assert w.lazy and w._parkable is False and w._stageable is False
    assert WorkerRunner._lazy_args(app.pools["gemma"], {"vram_gb": 7.4}, 0) == ({"vram_gb": 7.4}, 0, None)
    pool = _tts(app, parked={"ram_gb": 6.6}, linger=3)
    pool.stage(lambda ctx: None)
    assert WorkerRunner._lazy_args(pool, {"vram_gb": 7.2}, 1) == ({"vram_gb": 7.2}, 1, 3.0, {"ram_gb": 6.6}, True)
