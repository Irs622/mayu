"""Free-threading stress (spec §9): shared mayu objects used from many threads at once."""
import threading
import uuid

import pytest

from mayu import App

THREADS = 16

pytestmark = pytest.mark.thread_unsafe(reason="starts its own threads")


def _hammer(fn, threads=THREADS):
    """Runs fn(i) on `threads` threads released together; re-raises the first error."""
    barrier = threading.Barrier(threads)
    errors = []

    def run(i):
        barrier.wait()
        try:
            fn(i)
        except BaseException as e:  # noqa: BLE001 - collected and asserted below
            errors.append(e)

    ts = [threading.Thread(target=run, args=(i,)) for i in range(threads)]
    for t in ts:
        t.start()
    for t in ts:
        t.join(60)
    assert not any(t.is_alive() for t in ts), "a stress thread hung"
    return errors


def _app(redis_url):
    return App(broker=redis_url, result_ttl=60, default_queue=f"t-{uuid.uuid4().hex[:12]}")


def test_first_client_use_from_many_threads_creates_one_client(redis_url):
    app = _app(redis_url)
    seen = []
    assert not _hammer(lambda i: seen.append(id(app.client)))
    assert len(set(seen)) == 1


def test_concurrent_registration_of_distinct_tasks(redis_url):
    app = _app(redis_url)
    assert not _hammer(lambda i: app.task(name=f"stress.t{i}")(lambda: i))
    assert sorted(app.registry) == sorted(f"stress.t{i}" for i in range(THREADS))


def test_concurrent_registration_of_one_name_has_one_winner(redis_url):
    app = _app(redis_url)
    errors = _hammer(lambda i: app.task(name="stress.same")(lambda: i))
    assert len(errors) == THREADS - 1
    assert all(isinstance(e, ValueError) and "already registered" in str(e) for e in errors)
    assert list(app.registry) == ["stress.same"]


def test_concurrent_send_from_one_app(redis_url, running):
    app = _app(redis_url)

    @app.task(name="stress.add")
    def add(a, b):
        return a + b

    handles = [[] for _ in range(THREADS)]
    assert not _hammer(lambda i: handles[i].extend(add.send(i, j) for j in range(50)))
    with running(app, threads=8):
        for i, hs in enumerate(handles):
            assert [h.result(timeout=30) for h in hs] == [i + j for j in range(50)]


def test_concurrent_result_waits(redis_url, running):
    app = _app(redis_url)

    @app.task(name="stress.echo")
    def echo(x):
        return x

    hs = [echo.send(k) for k in range(THREADS * 4)]
    out = [None] * len(hs)

    def wait(i):
        for k in range(i, len(hs), THREADS):
            out[k] = hs[k].result(timeout=30)

    with running(app, threads=8):
        assert not _hammer(wait)
    assert out == list(range(len(hs)))
