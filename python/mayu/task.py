import datetime as dt
import functools
import inspect
import time

from .handle import TaskHandle

RESERVED_OPTIONS = frozenset({"countdown", "eta", "queue"})


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
    def __init__(self, app, fn, *, name: str, queue: str, max_retries: int, retry_on: tuple):
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
        self.is_async = inspect.iscoroutinefunction(fn)
        functools.update_wrapper(self, fn)

    def __repr__(self) -> str:
        return f"<Task {self.name}>"

    def __call__(self, *args, **kwargs):
        return self.fn(*args, **kwargs)

    def send(self, *args, countdown: float | None = None, eta: dt.datetime | None = None,
             queue: str | None = None, **kwargs) -> TaskHandle:
        self.signature.bind(*args, **kwargs)
        eta_ms = _eta_ms(countdown, eta)
        serializer = self.app.serializer
        payload = serializer.dumps([list(args), kwargs])
        task_id = self.app.client.enqueue(
            self.name, queue or self.queue, payload, serializer.content_type, self.max_retries, eta_ms
        )
        return TaskHandle(self.app, task_id)

    delay = send
