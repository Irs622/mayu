"""Task progress: `progress()` from inside a running task, and the `Progress` a `TaskHandle` reports."""
import contextvars
import dataclasses
import math
import numbers
import threading
from collections.abc import Callable

# Set by the worker on each slot thread (to its `_Slot`) and, for async tasks, in the task's own context.
_slot_local = threading.local()
_async_current: contextvars.ContextVar = contextvars.ContextVar("mayu_progress", default=None)


@dataclasses.dataclass(frozen=True)
class Progress:
    """A running task's last published progress, as `TaskHandle.progress()` returns it.

    `done`, `total`, `fraction` and `remaining` are None until the task reports (`total` also when it reported
    a bare fraction, `remaining` also while the estimate is still unknown); `remaining` is seconds.
    `updated` is when the worker published this, in seconds since the epoch (the `time.time()` clock). Workers
    publish with their heartbeat, so it trails the task by up to one heartbeat interval.
    """

    done: float | None
    total: float | None
    fraction: float | None
    remaining: float | None
    worker: str
    updated: float


def _number(name: str, value) -> float:
    if isinstance(value, bool) or not isinstance(value, numbers.Real):
        raise TypeError(f"progress() {name} must be a number, got {type(value).__name__}")
    value = float(value)
    if not math.isfinite(value):
        raise ValueError(f"progress() {name} must be finite, got {value}")
    return value


def _check(done, total) -> tuple[float, float | None]:
    d = _number("done", done)
    if total is None:
        if not 0.0 <= d <= 1.0:
            raise ValueError(f"progress(done) without total reports a fraction between 0 and 1, got {d:g}; "
                             "pass total to report a count, e.g. progress(7, 25)")
        return d, None
    t = _number("total", total)
    if t <= 0.0:
        raise ValueError(f"progress() total must be greater than 0, got {t:g}")
    if not 0.0 <= d <= t:
        raise ValueError(f"progress() done must be between 0 and total ({t:g}), got {d:g}")
    return d, t


def progress(done: float, total: float | None = None) -> bool:
    """Reports how far the running task is: `progress(7, 25)` for a count, `progress(0.28)` for a fraction.

    Returns True when recorded, False outside a running task (so library code can call it unconditionally).
    The arguments are validated either way, since a wrong call is a bug standalone too. Does no I/O: the
    worker publishes the latest report with its heartbeat. From a thread the task started, use
    `progress_reporter()` instead.
    """
    d, t = _check(done, total)
    cur = _async_current.get()
    if cur is not None:
        native, token = cur
    else:
        slot = getattr(_slot_local, "slot", None)
        token = None if slot is None else slot.token
        if token is None:
            return False
        native = slot.native
    return native.progress(token, d, t)


def progress_reporter() -> Callable[[float, float | None], bool] | None:
    """A `progress()` bound to the current task, for threads it starts; None outside a running task.

    Called after the task finished it does nothing and returns False.
    """
    cur = _async_current.get()
    if cur is None:
        slot = getattr(_slot_local, "slot", None)
        token = None if slot is None else slot.token
        if token is None:
            return None
        cur = (slot.native, token)
    native, token = cur

    def report(done: float, total: float | None = None) -> bool:
        d, t = _check(done, total)
        return native.progress(token, d, t)

    return report
