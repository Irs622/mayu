import time

import pytest

from mayu import App, Retry, Signature, TaskFailed, TaskHandle


def _wait(pred, timeout=5.0):
    deadline = time.monotonic() + timeout
    while not pred():
        assert time.monotonic() < deadline, "condition not reached in time"
        time.sleep(0.02)


def test_link_passes_result_and_extra_args(app, running):
    got = []

    @app.task
    def add(a, b):
        return a + b

    @app.task
    def record(result, tag, *, sep="-"):
        got.append(f"{tag}{sep}{result}")

    with running(app):
        assert add.send(2, 3, link=record.s("sum", sep=":")).result(timeout=5) == 5
        _wait(lambda: got)
    assert got == ["sum:5"]


def test_link_accepts_tasks_lists_and_other_queues(app, running, queue):
    other = f"{queue}-other"
    got = []

    @app.task
    def double(x):
        return x * 2

    @app.task
    def here(result):
        got.append(("here", result))

    @app.task(queue=other)
    def there(result, extra):
        got.append(("there", result, extra))

    with running(app, queues=[app.default_queue, other]):
        double.send(4, link=[here, there.s("x")])
        _wait(lambda: len(got) == 2)
    assert sorted(got) == [("here", 8), ("there", 8, "x")]


def test_link_from_async_task(app, running):
    got = []

    @app.task
    async def fetch():
        return "body"

    @app.task
    def store(result):
        got.append(result)

    with running(app):
        fetch.send(link=store)
        _wait(lambda: got)
    assert got == ["body"]


def test_link_not_called_when_parent_fails(app, running):
    got = []

    @app.task
    def boom():
        raise ValueError("no")

    @app.task
    def after(result):
        got.append(result)

    with running(app):
        with pytest.raises(TaskFailed):
            boom.send(link=after).result(timeout=5)
        time.sleep(0.3)
    assert got == []


def test_link_fires_once_after_retries(app, running):
    calls, got = [], []

    @app.task(max_retries=5)
    def flaky():
        calls.append(1)
        if len(calls) < 3:
            raise Retry(countdown=0)
        return "ok"

    @app.task
    def after(result):
        got.append(result)

    with running(app):
        assert flaky.send(link=after).result(timeout=5) == "ok"
        _wait(lambda: got)
        time.sleep(0.3)
    assert got == ["ok"]


def test_link_works_with_results_disabled(redis_url, queue, running):
    app = App(broker=redis_url, default_queue=queue)  # result_ttl=None
    got = []

    @app.task
    def make():
        return {"k": 1}

    @app.task
    def use(result):
        got.append(result)

    with running(app):
        make.send(link=use)
        _wait(lambda: got)
    assert got == [{"k": 1}]


def test_unserializable_result_with_link_fails_parent(redis_url, queue, running):
    app = App(broker=redis_url, default_queue=queue)  # results disabled: only the link needs it
    got = []

    @app.task
    def weird():
        return object()

    @app.task
    def use(result):
        got.append(result)

    with running(app):
        weird.send(link=use)
        _wait(lambda: app.client.queue_stats(queue)["dead"] == 1)
    assert got == []


def test_link_validated_at_send(app):
    """Signatures are checked where they are used, so `.s()` itself accepts any arguments."""
    @app.task
    def parent():
        return 1

    @app.task
    def takes_nothing():
        pass

    @app.task
    def takes_result(result):
        pass

    @app.task
    def takes_extra(result, extra):
        pass

    with pytest.raises(TypeError, match="result"):
        parent.send(link=takes_nothing)
    with pytest.raises(TypeError, match="result"):
        parent.send(link=takes_result.s("unexpected extra"))
    with pytest.raises(TypeError, match="link"):
        parent.send(link="not a task")
    sig = takes_extra.s(object())  # valid arguments...
    with pytest.raises(TypeError):
        parent.send(link=sig)  # ...that the serializer cannot encode
    assert isinstance(takes_result.s(), Signature)


def test_link_is_a_reserved_parameter_name(app):
    with pytest.raises(TypeError, match="link"):
        @app.task
        def bad(link):
            pass


def test_link_on_threadsafe_false_parent(app, running):
    got = []

    @app.task(threadsafe=False)
    def make():
        return 41

    @app.task
    def use(result):
        got.append(result)

    with running(app):
        make.send(link=use)
        _wait(lambda: got)
    assert got == [41]


@pytest.mark.parametrize("bad", ["nope", -1, 2**32, None])
def test_malformed_link_field_fails_the_parent_instead_of_hanging(app, running, queue, bad):
    @app.task
    def parent():
        return 1

    link = {"task": "x", "queue": queue, "max_retries": bad, "args": [], "kwargs": {}}
    _assert_parent_fails_on(app, running, queue, parent, link)


def test_link_entry_missing_a_key_fails_the_parent(app, running, queue):
    @app.task
    def parent():
        return 1

    link = {"task": "x", "queue": queue, "args": [], "kwargs": {}}  # no max_retries
    _assert_parent_fails_on(app, running, queue, parent, link)


def _assert_parent_fails_on(app, running, queue, parent, link):
    payload = app.serializer.dumps([[], {}, {"links": [link]}])
    task_id = app.client.enqueue(parent.name, queue, payload, app.serializer.content_type, 0)
    with running(app):
        _wait(lambda: app.client.queue_stats(queue)["dead"] == 1)
    with pytest.raises(TaskFailed) as exc:
        TaskHandle(app, task_id).result(timeout=5)
    assert "bad link callback" in exc.value.error
    assert "not serializable" not in exc.value.error
