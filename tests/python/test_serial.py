import threading
import time

import pytest

from mayu import TaskFailed


def test_threadsafe_false_tasks_run_one_at_a_time(app, running):
    spans, lock = [], threading.Lock()

    @app.task(threadsafe=False)
    def legacy(i):
        t0 = time.monotonic()
        time.sleep(0.2)
        with lock:
            spans.append((t0, time.monotonic()))
        return i

    @app.task
    def normal():
        time.sleep(0.2)
        return "n"

    with running(app, threads=4):
        t = time.monotonic()
        legacy_handles = [legacy.send(i) for i in range(4)]
        normal_handles = [normal.send() for _ in range(3)]
        assert [h.result(timeout=10) for h in normal_handles] == ["n", "n", "n"]
        normal_done = time.monotonic() - t
        assert sorted(h.result(timeout=10) for h in legacy_handles) == [0, 1, 2, 3]
    spans.sort()
    assert all(a[1] <= b[0] for a, b in zip(spans, spans[1:])), "legacy tasks overlapped"
    assert normal_done < 0.7, "thread-safe tasks must not queue behind the serial lane"


def test_async_threadsafe_false_rejected(app):
    with pytest.raises(ValueError, match="async"):
        @app.task(threadsafe=False)
        async def f():
            pass


def test_threadsafe_false_respects_hard_limit(app, running):
    @app.task(threadsafe=False, hard_time_limit=0.3)
    def stuck():
        time.sleep(30)

    @app.task(threadsafe=False)
    def legacy_quick():
        return "still serving"

    with running(app, threads=2, shutdown_grace=2):
        with pytest.raises(TaskFailed, match="TimeLimitExceeded"):
            stuck.send().result(timeout=5)
        assert legacy_quick.send().result(timeout=5) == "still serving"
