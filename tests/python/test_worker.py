import asyncio
import sys
import threading
import time

import pytest

from mayu import App, Retry, TaskFailed


def test_sync_task_result(app, running):
    @app.task
    def add(a, b=0):
        return a + b

    with running(app):
        assert add.send(2, b=3).result(timeout=5) == 5


def test_async_task_result(app, running):
    @app.task
    async def slow_double(x):
        await asyncio.sleep(0.05)
        return x * 2

    with running(app):
        assert slow_double.send(21).result(timeout=5) == 42


def test_failure_raises_taskfailed_and_dead_letters(app, running):
    @app.task
    def boom():
        raise ValueError("nope")

    with running(app):
        with pytest.raises(TaskFailed) as ei:
            boom.send().result(timeout=5)
    assert "ValueError" in ei.value.error
    assert "nope" in ei.value.traceback
    assert app.client.queue_stats(app.default_queue)["dead"] == 1


def test_retry_then_success(app, running):
    calls = []

    @app.task(max_retries=5)
    def flaky():
        calls.append(1)
        if len(calls) < 3:
            raise Retry(countdown=0)
        return "ok"

    with running(app):
        assert flaky.send().result(timeout=5) == "ok"
    assert len(calls) == 3


def test_retry_on_exhausts_then_fails(app, running):
    calls = []

    @app.task(max_retries=2, retry_on=(TimeoutError,))
    def always_timeout():
        calls.append(1)
        raise TimeoutError("slow upstream")

    # default backoff base is 1s with full jitter: 2 retries finish well under the timeout
    with running(app):
        with pytest.raises(TaskFailed, match="retries exhausted"):
            always_timeout.send().result(timeout=15)
    assert len(calls) == 3


def test_countdown_delays_execution(app, running):
    ran_at = []

    @app.task
    def stamp():
        ran_at.append(time.monotonic())

    with running(app):
        sent = time.monotonic()
        stamp.send(countdown=0.5).result(timeout=5)
    assert ran_at[0] - sent >= 0.45


def test_unknown_task_dead_letters(app, running, redis_url):
    other = App(broker=redis_url, result_ttl=60, default_queue=app.default_queue)

    @other.task(name="ghost")
    def ghost():
        pass

    with running(app):
        with pytest.raises(TaskFailed, match="unknown task"):
            ghost.send().result(timeout=5)


def test_pickle_payload_refused_by_non_pickle_worker(app, running, redis_url):
    executed = []

    @app.task(name="p.t")
    def t():
        executed.append(1)

    producer = App(broker=redis_url, result_ttl=60, default_queue=app.default_queue, serializer="pickle")

    @producer.task(name="p.t")
    def t_producer():
        pass

    with running(app):
        with pytest.raises(TaskFailed, match="pickle"):
            t_producer.send().result(timeout=5)
    assert executed == []


def test_unserializable_result_is_failure_and_worker_survives(app, running):
    @app.task
    def bad():
        return {1, 2, 3}  # msgpack can't encode sets

    @app.task
    def good():
        return "fine"

    with running(app, threads=1):
        with pytest.raises(TaskFailed, match="not serializable"):
            bad.send().result(timeout=5)
        assert good.send().result(timeout=5) == "fine"


def test_threads_run_concurrently(app, running):
    @app.task
    def nap():
        time.sleep(0.5)

    with running(app, threads=8):
        t = time.monotonic()
        handles = [nap.send() for _ in range(8)]
        for h in handles:
            h.result(timeout=5)
        assert time.monotonic() - t < 2.0


@pytest.mark.skipif(sys._is_gil_enabled(), reason="CPU parallelism needs free-threaded Python")
def test_cpu_bound_tasks_run_in_parallel(app, running):
    @app.task
    def spin(n):
        x = 0
        for i in range(n):
            x += i
        return x

    n = 10_000_000  # enough work that thread and Redis overhead don't swing the ratio on a 4-vCPU runner
    with running(app, threads=4):
        t = time.monotonic()
        spin.send(n).result(timeout=30)
        single = time.monotonic() - t
        t = time.monotonic()
        for h in [spin.send(n) for _ in range(4)]:
            h.result(timeout=30)
        parallel = time.monotonic() - t
    # One after another would take 4x; under 3x is parallel even with the runner's other threads competing.
    assert parallel < single * 3, f"4 tasks took {parallel:.2f}s vs {single:.2f}s for one"


def test_shutdown_requeues_prefetched_tasks(app, running):
    started = threading.Event()

    @app.task
    def slow(i):
        started.set()
        time.sleep(0.3)
        return i

    handles = [slow.send(i) for i in range(4)]
    with running(app, threads=1, prefetch_multiplier=4, shutdown_grace=5):
        assert started.wait(5)
    stats = app.client.queue_stats(app.default_queue)
    assert stats["pending"] == 0, "nothing stranded in the PEL"
    with running(app, threads=4):
        assert sorted(h.result(timeout=5) for h in handles) == [0, 1, 2, 3]


def test_gil_stays_disabled_after_imports():
    import sysconfig
    if not sysconfig.get_config_var("Py_GIL_DISABLED"):
        pytest.skip("not a free-threaded build")
    import mayu  # noqa: F401
    import mayu.worker  # noqa: F401
    import msgpack  # noqa: F401
    assert not sys._is_gil_enabled(), "an import re-enabled the GIL"
