import datetime as dt
import functools
import inspect
import math
import time

from . import otel
from .handle import TaskHandle

RESERVED_OPTIONS = frozenset({"countdown", "eta", "queue", "link"})


_RATE_UNITS = {"s": 1.0, "m": 60.0, "h": 3600.0}


def parse_rate(rate) -> float:
    """'100/s', '10/m', '5/h' or a bare number (per second) -> task starts per second."""
    if isinstance(rate, bool):
        raise ValueError(f"bad rate_limit {rate!r}")
    if isinstance(rate, (int, float)):
        try:
            n, per = float(rate), 1.0
        except OverflowError:
            raise ValueError(f"bad rate_limit {rate!r}") from None
    elif isinstance(rate, str):
        num, _, unit = rate.strip().partition("/")
        try:
            n, per = float(num), _RATE_UNITS[unit.strip() or "s"]
        except (ValueError, KeyError):
            raise ValueError(f"bad rate_limit {rate!r}: use '<number>/s', '<number>/m' or '<number>/h'") from None
    else:
        raise TypeError(f"rate_limit must be a string or number, got {type(rate).__name__}")
    if not (math.isfinite(n) and n > 0):
        raise ValueError(f"bad rate_limit {rate!r}: must be a positive number")
    return n / per


_RESULT = object()  # stands in for the parent's result when checking a callback's arguments


class Signature:
    """A task plus extra arguments, for ``link=``. The parent's result is passed first."""

    def __init__(self, task, args=(), kwargs=None):
        kwargs = dict(kwargs or {})
        try:
            task.signature.bind(_RESULT, *args, **kwargs)
        except TypeError as e:
            raise TypeError(f"{task.name} cannot be a link callback with these arguments "
                            f"(it receives the parent's result first): {e}") from None
        self.task = task
        self.args = tuple(args)
        self.kwargs = kwargs

    def __repr__(self) -> str:
        return f"<Signature {self.task.name} args={self.args!r} kwargs={self.kwargs!r}>"

    def _wire(self) -> dict:
        return {"task": self.task.name, "queue": self.task.queue, "max_retries": self.task.max_retries,
                "args": list(self.args), "kwargs": self.kwargs}


def _as_links(link) -> list[Signature]:
    if link is None:
        return []
    items = link if isinstance(link, (list, tuple)) else [link]
    out = []
    for item in items:
        if isinstance(item, Signature):
            out.append(item)
        elif isinstance(item, Task):
            out.append(Signature(item))
        else:
            raise TypeError(f"link must be a task, task.s(...), or a list of them; got {item!r}")
    return out


def _eta_ms(countdown: float | None, eta: dt.datetime | None) -> int | None:
    if countdown is not None and eta is not None:
        raise ValueError("pass countdown or eta, not both")
    if countdown is not None:
        return int(time.time() * 1000) + max(0, int(countdown * 1000))
    if eta is not None:
        if eta.tzinfo is None:
            raise ValueError("eta must be a timezone-aware datetime")
        return int(eta.timestamp() * 1000)
    return None


class Task:
    def __init__(self, app, fn, *, name: str, queue: str, max_retries: int, retry_on: tuple,
                 soft_time_limit: float | None = None, hard_time_limit: float | None = None, threadsafe: bool = True,
                 rate_limit=None):
        self.app = app
        self.fn = fn
        self.name = name
        self.queue = queue
        self.max_retries = max_retries
        self.retry_on = retry_on
        self.signature = inspect.signature(fn)
        clash = RESERVED_OPTIONS & set(self.signature.parameters)
        if clash:
            raise TypeError(f"task {name!r} uses reserved parameter name(s) {sorted(clash)}")
        for label, value in (("soft_time_limit", soft_time_limit), ("hard_time_limit", hard_time_limit)):
            if value is not None and value <= 0:
                raise ValueError(f"{label} must be a positive number of seconds")
        if soft_time_limit is not None and hard_time_limit is not None and soft_time_limit >= hard_time_limit:
            raise ValueError("soft_time_limit must be less than hard_time_limit")
        self.soft_time_limit = soft_time_limit
        self.hard_time_limit = hard_time_limit
        self.is_async = inspect.iscoroutinefunction(fn)
        if not threadsafe and self.is_async:
            raise ValueError("threadsafe=False is for sync tasks; async tasks already run on one event-loop thread")
        self.threadsafe = threadsafe
        self.rate_limit = rate_limit
        self.rate_per_sec = None if rate_limit is None else parse_rate(rate_limit)
        functools.update_wrapper(self, fn)

    def __repr__(self) -> str:
        return f"<Task {self.name}>"

    def __call__(self, *args, **kwargs):
        return self.fn(*args, **kwargs)

    def send(self, *args, countdown: float | None = None, eta: dt.datetime | None = None,
             queue: str | None = None, link=None, **kwargs) -> TaskHandle:
        self.signature.bind(*args, **kwargs)
        links = _as_links(link)
        eta_ms = _eta_ms(countdown, eta)
        serializer = self.app.serializer
        body = [list(args), kwargs]
        if links:
            body.append({"links": [s._wire() for s in links]})
        payload = serializer.dumps(body)
        q = queue or self.queue
        span, traceparent = otel.start_send(self.name, q)
        try:
            task_id = self.app.client.enqueue(
                self.name, q, payload, serializer.content_type, self.max_retries, eta_ms, trace_ctx=traceparent
            )
        except BaseException as e:
            otel.end(span, e)
            raise
        otel.set_message_id(span, task_id)
        otel.end(span)
        return TaskHandle(self.app, task_id)

    def s(self, *args, **kwargs) -> Signature:
        """This task with extra arguments, as a ``link=`` callback (the result comes first)."""
        return Signature(self, args, kwargs)

    delay = send
