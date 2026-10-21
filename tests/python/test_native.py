import sys
import threading
import time

import pytest

from mayu import _native


def test_enqueue_recv_complete_result(redis_url, queue):
    client = _native.Client(redis_url)
    worker = _native.Worker(redis_url, [queue], 2, 2, 60)
    task_id = client.enqueue("t.add", queue, b"\x01\x02", "application/msgpack", 3)

    rt = worker.recv(3000)
    assert rt is not None
    assert (rt.id, rt.task, rt.queue, rt.attempt, rt.max_retries) == (task_id, "t.add", queue, 0, 3)
    assert rt.payload == b"\x01\x02"
    assert rt.content_type == "application/msgpack"

    worker.complete_success(rt.token, b"result")
    raw = client.get_result(task_id, 3000)
    assert raw is not None
    worker.shutdown(1000)


def test_recv_times_out_with_none(redis_url, queue):
    worker = _native.Worker(redis_url, [queue], 1)
    t = time.monotonic()
    assert worker.recv(100) is None
    assert time.monotonic() - t >= 0.09
    worker.shutdown(1000)


def test_recv_releases_interpreter(redis_url, queue):
    """Other Python threads keep running while recv blocks in Rust."""
    worker = _native.Worker(redis_url, [queue], 1)
    counter = 0
    stop = False

    def spin():
        nonlocal counter
        while not stop:
            counter += 1

    th = threading.Thread(target=spin)
    th.start()
    worker.recv(300)
    stop = True
    th.join()
    assert counter > 1000
    worker.shutdown(1000)


def test_queue_stats(redis_url, queue):
    client = _native.Client(redis_url)
    client.enqueue("t", queue, b"", "application/msgpack", 0)
    client.enqueue("t", queue, b"", "application/msgpack", 0, int(time.time() * 1000) + 60_000)
    stats = client.queue_stats(queue)
    assert stats == {"stream": 1, "pending": 0, "delayed": 1, "dead": 0}


def test_get_result_timeout_returns_none(redis_url):
    client = _native.Client(redis_url)
    assert client.get_result("does-not-exist", 50) is None


def test_bad_url_raises():
    with pytest.raises(RuntimeError):
        _native.Client("redis://127.0.0.1:1/0")


def _busy_until_raised(body):
    class Boom(Exception):
        pass

    caught, idents = [], []
    ready = threading.Event()

    def busy():
        idents.append(threading.get_ident())
        ready.set()
        try:
            body(time.monotonic() + 5)
        except Boom:
            caught.append(True)

    th = threading.Thread(target=busy)
    th.start()
    ready.wait()
    assert _native.async_raise(idents[0], Boom) == 1
    th.join(5)
    return caught


def test_async_raise_interrupts_busy_thread():
    def body(deadline):
        while time.monotonic() < deadline:
            x = sum((1, 2))  # noqa: F841
    assert _busy_until_raised(body) == [True]


@pytest.mark.skipif(sys.version_info < (3, 14), reason="CPython 3.13 can deliver an async exception "
                    "outside the enclosing try when it lands in a while loop with an empty body")
def test_async_raise_interrupts_empty_busy_loop():
    def body(deadline):
        while time.monotonic() < deadline:
            pass
    assert _busy_until_raised(body) == [True]


def test_async_raise_unknown_thread_returns_zero():
    assert _native.async_raise(1, RuntimeError) == 0


def test_async_raise_none_clears_pending():
    class Boom(Exception):
        pass

    gate = threading.Event()
    idents, errors = [], []
    started = threading.Event()

    def blocked():
        idents.append(threading.get_ident())
        started.set()
        try:
            gate.wait()  # inside a C call: the async exception stays pending
            for _ in range(1000):
                pass
        except Boom:
            errors.append("raised")

    th = threading.Thread(target=blocked)
    th.start()
    started.wait()
    time.sleep(0.05)
    assert _native.async_raise(idents[0], Boom) == 1
    assert _native.async_raise(idents[0], None) == 1
    gate.set()
    th.join(5)
    assert errors == []


def _lazy_worker(redis_url, queue, linger=None, **kw):
    return _native.Worker(
        redis_url, [queue], 1,
        resources={"vram_gb": 7.5, "pool:tts": 1.0}, task_resources={"t": {"pool:tts": 1.0}},
        lazy_pools={"tts": ({"vram_gb": 7.2}, 0, linger)}, schedule="oldest", **kw,
    )


@pytest.mark.parametrize("bad", [0.0, -1.0, float("nan"), float("inf")])
def test_lazy_pool_linger_must_be_finite_and_positive(redis_url, queue, bad):
    with pytest.raises(ValueError, match="linger for pool 'tts'"):
        _lazy_worker(redis_url, queue, linger=bad)


def test_lookahead_and_gpu_bindings(redis_url, queue):
    worker = _lazy_worker(redis_url, queue, linger=30.0, lookahead=True)
    assert worker.lookahead_begin() is None  # no workflow demand yet
    with pytest.raises(ValueError):
        worker.lookahead_done("tts", True)  # nothing is loading
    assert worker.blocked("vram_gb") == 0
    assert ("vram_gb", 7.5, 0.0) in worker.resource_usage()
    worker.pool_lookahead_loaded("tts")
    worker.set_gpu_stats([(0, "GPU-x", 0.5, 1 << 30, 8 << 30, None, 70.0, None)])
    worker.shutdown(1000)
