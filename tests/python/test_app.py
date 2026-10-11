import datetime as dt
import time

import pytest

from mayu import App, TaskFailed
from mayu import serializers


def test_serializers_roundtrip():
    for name in ("msgpack", "json", "pickle"):
        s = serializers.get_serializer(name)
        assert s.loads(s.dumps([[1, "a"], {"k": 2}])) == [[1, "a"], {"k": 2}]
        assert serializers.for_content_type(s.content_type, allow_pickle=True) is s


def test_pickle_requires_opt_in():
    with pytest.raises(ValueError, match="pickle"):
        serializers.for_content_type(serializers.PICKLE, allow_pickle=False)
    with pytest.raises(ValueError):
        serializers.for_content_type("application/x-unknown", allow_pickle=True)
    with pytest.raises(ValueError):
        serializers.get_serializer("yaml")


def test_task_decorator_bare_and_with_options(app):
    @app.task
    def add(a, b):
        return a + b

    @app.task(name="custom.name", queue="other", max_retries=7, retry_on=(TimeoutError,))
    def other():
        pass

    assert add(2, 3) == 5, "calling the task runs it locally"
    assert add.name.endswith("add") and add.queue == app.default_queue and add.max_retries == 3
    assert (other.name, other.queue, other.max_retries, other.retry_on) == ("custom.name", "other", 7, (TimeoutError,))
    assert app.registry["custom.name"] is other
    assert set(app.queues()) == {app.default_queue, "other"}


def test_duplicate_task_name_rejected(app):
    @app.task(name="dup")
    def a():
        pass

    with pytest.raises(ValueError, match="dup"):
        @app.task(name="dup")
        def b():
            pass


def test_reserved_parameter_names_rejected(app):
    with pytest.raises(TypeError, match="countdown"):
        @app.task
        def bad(countdown):
            pass


def test_send_validates_signature(app):
    @app.task
    def add(a, b):
        return a + b

    with pytest.raises(TypeError):
        add.send(1)
    with pytest.raises(TypeError):
        add.send(1, 2, c=3)


def test_send_rejects_unserializable_args(app):
    @app.task
    def f(x):
        pass

    with pytest.raises(TypeError):
        f.send(object())


def test_eta_validation(app):
    @app.task
    def f():
        pass

    with pytest.raises(ValueError, match="timezone"):
        f.send(eta=dt.datetime.now())
    with pytest.raises(ValueError, match="not both"):
        f.send(countdown=1, eta=dt.datetime.now(dt.timezone.utc))
    f.send(countdown=-5)  # negative countdown means "now"
    assert app.client.queue_stats(app.default_queue)["stream"] == 1


def test_send_enqueues_and_countdown_delays(app):
    @app.task
    def f(x):
        pass

    h = f.send(1)
    f.delay(2)
    f.send(3, countdown=60)
    f.send(4, eta=dt.datetime.now(dt.timezone.utc) + dt.timedelta(minutes=5))
    assert len(h.id) == 26
    stats = app.client.queue_stats(app.default_queue)
    assert (stats["stream"], stats["delayed"]) == (2, 2)


def test_send_queue_override(app, queue):
    @app.task
    def f():
        pass

    f.send(queue=queue + "-x")
    assert app.client.queue_stats(queue + "-x")["stream"] == 1


def test_result_disabled_raises(redis_url, queue):
    app = App(broker=redis_url, default_queue=queue)

    @app.task
    def f():
        pass

    with pytest.raises(RuntimeError, match="results are disabled"):
        f.send().result(timeout=0.1)


def test_result_timeout(app):
    @app.task
    def f():
        pass

    t = time.monotonic()
    with pytest.raises(TimeoutError):
        f.send().result(timeout=0.2)
    assert time.monotonic() - t >= 0.15


def test_env_overrides(monkeypatch, redis_url):
    monkeypatch.setenv("MAYU_BROKER", redis_url)
    monkeypatch.setenv("MAYU_RESULT_TTL", "0")
    monkeypatch.setenv("MAYU_DEFAULT_QUEUE", "envq")
    monkeypatch.setenv("MAYU_SERIALIZER", "json")
    app = App(broker="redis://ignored:1/0", result_ttl=99)
    assert app.broker_url == redis_url
    assert app.result_ttl is None
    assert app.default_queue == "envq"
    assert app.serializer.content_type == serializers.JSON


def test_missing_broker_raises(monkeypatch):
    monkeypatch.delenv("MAYU_BROKER", raising=False)
    with pytest.raises(ValueError, match="broker"):
        App()


def test_taskfailed_is_exported():
    e = TaskFailed("id", "ValueError('x')", "tb")
    assert "ValueError" in str(e) and e.traceback == "tb"
