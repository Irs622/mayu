"""Deadlines of running tasks. One lock decides who owns each outcome: the task's thread
(via disarm) or the supervisor (via tick, at the hard limit) — never both."""
import threading
import time
from dataclasses import dataclass

from .exceptions import SoftTimeLimitExceeded


@dataclass(eq=False)
class Run:
    token: int
    ident: int
    soft: float | None
    hard: float | None
    soft_deadline: float | None
    hard_deadline: float | None
    context: object = None
    soft_sent: bool = False
    done: bool = False
    owned: bool = False


class TimeLimits:
    def __init__(self, raise_in_thread=None, clock=time.monotonic):
        if raise_in_thread is None:
            from . import _native
            raise_in_thread = _native.async_raise
        self._raise = raise_in_thread
        self._clock = clock
        self._lock = threading.Lock()
        self._runs: dict[int, Run] = {}

    def __len__(self) -> int:
        with self._lock:
            return len(self._runs)

    def arm(self, token: int, ident: int, soft: float | None, hard: float | None, context=None) -> Run | None:
        if soft is None and hard is None:
            return None
        now = self._clock()
        run = Run(token, ident, soft, hard,
                  None if soft is None else now + soft,
                  None if hard is None else now + hard,
                  context)
        with self._lock:
            self._runs[token] = run
        return run

    def disarm(self, run: Run) -> bool:
        """Called by the task's thread when the task returns. True if it still owns the outcome."""
        with self._lock:
            if not run.done:
                run.owned = self._runs.pop(run.token, None) is run
                run.done = True
            owned, sent = run.owned, run.soft_sent
        if sent:
            # tick() never sends after done, so clearing here cannot race a new send.
            self._raise(run.ident, None)
        return owned

    def tick(self) -> list[Run]:
        """Delivers due soft limits; returns runs past their hard limit (the caller owns them)."""
        now = self._clock()
        expired = []
        with self._lock:
            for token, run in list(self._runs.items()):
                if run.hard_deadline is not None and now >= run.hard_deadline:
                    del self._runs[token]
                    run.done = True
                    run.owned = False
                    expired.append(run)
                elif run.soft_deadline is not None and not run.soft_sent and now >= run.soft_deadline:
                    run.soft_sent = True
                    self._raise(run.ident, SoftTimeLimitExceeded)
        return expired
