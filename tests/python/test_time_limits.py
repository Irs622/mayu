import asyncio
import time

import pytest

from mayu import SoftTimeLimitExceeded, TaskFailed, TimeLimitExceeded


def _busy(seconds):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        pass


def test_soft_limit_can_be_caught_for_cleanup(app, running):
    @app.task(soft_time_limit=0.2)
    def careful():
        try:
            _busy(5)
        except SoftTimeLimitExceeded:
            return "cleaned up"
        return "finished"

    with running(app, threads=1):
        assert careful.send().result(timeout=5) == "cleaned up"


def test_uncaught_soft_limit_fails_task(app, running):
    @app.task(soft_time_limit=0.2)
    def runaway():
        _busy(5)

    with running(app, threads=1):
        with pytest.raises(TaskFailed, match="SoftTimeLimitExceeded"):
            runaway.send().result(timeout=5)


def test_hard_limit_abandons_thread_and_worker_keeps_serving(app, running):
    @app.task(hard_time_limit=0.3)
    def stuck():
        time.sleep(30)  # a C call: no exception can interrupt it

    @app.task
    def quick():
        return "ok"

    with running(app, threads=1, shutdown_grace=2) as w:
        t = time.monotonic()
        with pytest.raises(TaskFailed, match="TimeLimitExceeded"):
            stuck.send().result(timeout=5)
        assert time.monotonic() - t < 2
        assert quick.send().result(timeout=5) == "ok"
        assert sum(1 for s in w._slots if s.zombie) == 1
    assert time.monotonic() - t < 6, "shutdown must not wait for the zombie thread"


def test_hard_limit_follows_retry_policy(app, running):
    runs = []

    @app.task(hard_time_limit=0.3, max_retries=1, retry_on=(TimeLimitExceeded,))
    def stuck():
        runs.append(1)
        time.sleep(5)

    with running(app, threads=1, shutdown_grace=2):
        with pytest.raises(TaskFailed, match="retries exhausted"):
            stuck.send().result(timeout=10)
    assert len(runs) == 2


def test_soft_limit_racing_completion_never_breaks_worker(app, running):
    @app.task(soft_time_limit=0.05)
    def edge(i):
        _busy(0.05)
        return i

    @app.task
    def quick():
        return "alive"

    with running(app, threads=2) as w:
        for h in [edge.send(i) for i in range(40)]:
            try:
                h.result(timeout=10)
            except TaskFailed as e:
                assert "SoftTimeLimitExceeded" in e.error
        assert quick.send().result(timeout=5) == "alive"
        assert all(s.thread.is_alive() for s in w._slots if not s.zombie)
        assert not any(s.zombie for s in w._slots)


def test_async_soft_limit(app, running):
    @app.task(soft_time_limit=0.2)
    async def slow():
        await asyncio.sleep(5)

    with running(app, threads=1):
        with pytest.raises(TaskFailed, match="SoftTimeLimitExceeded"):
            slow.send().result(timeout=5)


def test_async_hard_limit(app, running):
    @app.task(hard_time_limit=0.2)
    async def slow():
        await asyncio.sleep(5)

    with running(app, threads=1):
        with pytest.raises(TaskFailed, match="TimeLimitExceeded"):
            slow.send().result(timeout=5)


def test_async_task_raising_its_own_timeout_is_not_a_limit(app, running):
    @app.task(soft_time_limit=5)
    async def upstream():
        raise TimeoutError("upstream timed out")

    with running(app, threads=1):
        with pytest.raises(TaskFailed) as ei:
            upstream.send().result(timeout=5)
    assert "upstream timed out" in ei.value.error
    assert "TimeLimit" not in ei.value.error


def test_soft_limit_landing_anywhere_in_run_sync_yields_exactly_one_outcome():
    """Soft limits fired at 2 ms against 1.5-2.5 ms tasks: the exception lands at arbitrary points,
    including inside the arm/disarm bookkeeping. No hang, no escape, one report per task."""
    import collections
    import random
    import threading
    from types import SimpleNamespace

    from mayu.worker import WorkerRunner, _Slot

    outcomes = collections.defaultdict(list)

    class Native:
        def complete_success(self, token, data, links=None):
            outcomes[token].append("ok")

        def complete_failure(self, token, error, tb):
            outcomes[token].append(error)

        def complete_retry(self, token, ms, error):
            outcomes[token].append("retry")

    class Serializer:
        def dumps(self, value):
            return b""

    app = SimpleNamespace(result_ttl=None, queues=lambda **kw: ["q"], registry={}, pools={})
    runner = WorkerRunner(app, threads=1)
    runner.native = Native()

    def supervise():
        while not runner._halt.is_set():
            for run in runner._limits.tick():
                runner._on_hard_limit(run)
            time.sleep(0.0005)

    supervisor = threading.Thread(target=supervise, daemon=True)
    supervisor.start()

    count = 1500
    failures = []

    def worker_thread():
        slot = _Slot("t")
        slot.ident = threading.get_ident()
        task = SimpleNamespace(soft_time_limit=0.002, hard_time_limit=None, retry_on=(), pools={},
                               fn=lambda: _busy(random.uniform(0.0015, 0.0025)))
        try:
            for token in range(count):
                runner._run_sync(token, task, Serializer(), (), {}, slot)
        except BaseException as e:  # a soft limit escaping the method would end up here
            failures.append(e)

    t = threading.Thread(target=worker_thread, daemon=True)
    t.start()
    t.join(60)
    runner._halt.set()
    assert not t.is_alive(), "run_sync wedged (a lock leaked to an interrupted thread?)"
    assert failures == []
    assert len(outcomes) == count and all(len(v) == 1 for v in outcomes.values())
    assert all(v[0] == "ok" or "SoftTimeLimitExceeded" in v[0] for v in outcomes.values())
    assert len(runner._limits) == 0
