import asyncio

import pytest

pytest.importorskip("opentelemetry.sdk.trace")

from opentelemetry import trace  # noqa: E402
from opentelemetry.sdk.trace import TracerProvider  # noqa: E402
from opentelemetry.sdk.trace.export import SimpleSpanProcessor  # noqa: E402
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter  # noqa: E402
from opentelemetry.trace import SpanKind, StatusCode  # noqa: E402

from mayu import TaskFailed  # noqa: E402
from mayu import otel  # noqa: E402

_EXPORTER = InMemorySpanExporter()


@pytest.fixture(scope="module", autouse=True)
def _provider():
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(_EXPORTER))
    trace.set_tracer_provider(provider)  # global, once per process
    yield


@pytest.fixture
def spans():
    _EXPORTER.clear()
    return _EXPORTER


def _wait_spans(exporter, n, timeout=5.0):
    import time
    deadline = time.monotonic() + timeout
    while len(exporter.get_finished_spans()) < n:
        assert time.monotonic() < deadline, [s.name for s in exporter.get_finished_spans()]
        time.sleep(0.02)
    return exporter.get_finished_spans()


def _one(spans, name):
    found = [s for s in spans if s.name == name]
    assert len(found) == 1, [s.name for s in spans]
    return found[0]


def test_send_and_process_spans_share_the_trace(app, running, spans, queue):
    tracer = trace.get_tracer("test")

    @app.task(name="o.add")
    def add(a, b):
        with tracer.start_as_current_span("inside"):
            return a + b

    with running(app):
        h = add.send(1, 2)
        assert h.result(timeout=5) == 3
    got = _wait_spans(spans, 3)
    send, process, inside = _one(got, "send o.add"), _one(got, "process o.add"), _one(got, "inside")
    assert send.kind == SpanKind.PRODUCER and process.kind == SpanKind.CONSUMER
    assert process.context.trace_id == send.context.trace_id
    assert process.parent.span_id == send.context.span_id
    assert inside.parent.span_id == process.context.span_id
    assert send.attributes["messaging.system"] == "mayu"
    assert send.attributes["messaging.destination.name"] == queue
    assert send.attributes["messaging.message.id"] == h.id
    assert process.attributes["messaging.message.id"] == h.id
    assert process.attributes["mayu.task.attempt"] == 0
    assert process.status.status_code != StatusCode.ERROR


def test_send_joins_the_callers_trace(app, running, spans):
    @app.task(name="o.noop")
    def noop():
        return None

    tracer = trace.get_tracer("test")
    with running(app):
        with tracer.start_as_current_span("request") as outer:
            h = noop.send()
        h.result(timeout=5)
    got = _wait_spans(spans, 3)
    assert _one(got, "process o.noop").context.trace_id == outer.get_span_context().trace_id


def test_failure_marks_the_process_span(app, running, spans):
    @app.task(name="o.boom", max_retries=0)
    def boom():
        raise ValueError("nope")

    with running(app):
        with pytest.raises(TaskFailed):
            boom.send().result(timeout=5)
    process = _one(_wait_spans(spans, 2), "process o.boom")
    assert process.status.status_code == StatusCode.ERROR
    assert any(e.name == "exception" for e in process.events)


def test_async_task_runs_inside_its_span(app, running, spans):
    tracer = trace.get_tracer("test")

    @app.task(name="o.async")
    async def double(x):
        await asyncio.sleep(0.01)
        with tracer.start_as_current_span("inside-async"):
            return x * 2

    with running(app):
        assert double.send(4).result(timeout=5) == 8
    got = _wait_spans(spans, 3)
    assert _one(got, "inside-async").parent.span_id == _one(got, "process o.async").context.span_id


def test_link_callback_joins_the_parents_trace(app, running, spans):
    @app.task(name="o.parent")
    def parent():
        return 1

    @app.task(name="o.child")
    def child(x):
        return x

    with running(app):
        parent.send(link=child.s())
        got = _wait_spans(spans, 3)  # send parent, process parent, process child
    assert _one(got, "process o.child").context.trace_id == _one(got, "send o.parent").context.trace_id


def test_task_without_trace_context_gets_a_root_span(app, running, spans, queue):
    @app.task(name="o.bare")
    def bare():
        return "ok"

    payload = app.serializer.dumps([[], {}])
    with running(app):
        task_id = app.client.enqueue("o.bare", queue, payload, app.serializer.content_type, 0)
        from mayu.handle import TaskHandle
        assert TaskHandle(app, task_id).result(timeout=5) == "ok"
    process = _one(_wait_spans(spans, 1), "process o.bare")
    assert process.parent is None


def test_without_opentelemetry_nothing_changes(app, running, monkeypatch, spans):
    monkeypatch.setattr(otel, "_trace", None)

    @app.task(name="o.plain")
    def plain():
        return 7

    with running(app):
        assert plain.send().result(timeout=5) == 7
    assert otel.start_send("x", "q") == (None, None)
    assert [s.name for s in spans.get_finished_spans()] == []
