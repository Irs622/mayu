"""OpenTelemetry spans and W3C trace-context propagation, active only when `opentelemetry-api` is
installed (`pip install mayu[otel]`). The envelope carries the producer's `traceparent`."""
import contextlib

try:
    from opentelemetry import context as _context
    from opentelemetry import propagate as _propagate
    from opentelemetry import trace as _trace
    from opentelemetry.trace import SpanKind, Status, StatusCode
except ImportError:  # optional dependency
    _trace = None


def _tracer():
    return _trace.get_tracer("mayu")


def start_send(task_name: str, queue: str):
    """A PRODUCER span for enqueueing, and the `traceparent` to put in the envelope."""
    if _trace is None:
        return None, None
    span = _tracer().start_span(f"send {task_name}", kind=SpanKind.PRODUCER, attributes={
        "messaging.system": "mayu",
        "messaging.operation.type": "send",
        "messaging.destination.name": queue,
        "mayu.task.name": task_name,
    })
    carrier: dict = {}
    _propagate.inject(carrier, context=_trace.set_span_in_context(span))
    return span, carrier.get("traceparent")


def set_message_id(span, task_id: str) -> None:
    if span is not None:
        span.set_attribute("messaging.message.id", task_id)


def start_process(ready):
    """A CONSUMER span for running `ready`, a child of the producer's context when there is one."""
    if _trace is None:
        return None
    parent = _propagate.extract({"traceparent": ready.trace_ctx}) if ready.trace_ctx else None
    return _tracer().start_span(f"process {ready.task}", context=parent, kind=SpanKind.CONSUMER, attributes={
        "messaging.system": "mayu",
        "messaging.operation.type": "process",
        "messaging.destination.name": ready.queue,
        "messaging.message.id": ready.id,
        "mayu.task.name": ready.task,
        "mayu.task.attempt": ready.attempt,
    })


@contextlib.contextmanager
def activate(span):
    """Makes `span` current in this thread (or coroutine), so spans the task starts nest under it."""
    if span is None or _trace is None:
        yield
        return
    token = _context.attach(_trace.set_span_in_context(span))
    try:
        yield
    finally:
        _context.detach(token)


def end(span, error: BaseException | None = None) -> None:
    if span is None:
        return
    if error is not None:
        span.record_exception(error)
        span.set_status(Status(StatusCode.ERROR, repr(error)))
    span.end()
