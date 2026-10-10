import threading
import time

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
    import pytest

    with pytest.raises(RuntimeError):
        _native.Client("redis://127.0.0.1:1/0")
