"""Deadlines of running tasks.

A soft limit is delivered into the task's thread as an asynchronous exception, which can land on
*any* bytecode of that thread. So the task's thread must never hold a lock while one can be pending:
interrupted inside `with lock:`, it would leak the lock and wedge the supervisor. Everything the
thread does here is therefore lock-free and idempotent (safe to simply re-run after an interrupt).

Arbitration between the task's thread and the supervisor is one append-only log per run. Appends are
atomic and the first entry never changes, so both sides can read the same decision from it:

  * `_SOFT` is appended only by the supervisor, which sends the soft exception iff it is first.
    It appends `_SENT` once the send call has returned.
  * `_HARD` (supervisor) and `_DONE` (task thread) race for the outcome: whichever comes first of
    the two owns it, so there is exactly one outcome.
  * After `_DONE`, a send can only be in flight if `_SOFT` is first; the thread waits for `_SENT`
    and then clears the pending exception, so none can fire once `disarm` has returned.
"""
import time
from dataclasses import dataclass, field

from . import _native
from .exceptions import SoftTimeLimitExceeded

_SOFT, _SENT, _HARD, _DONE = "soft", "sent", "hard", "done"


@dataclass(eq=False)
class Run:
    token: int
    ident: int
    soft: float | None
    hard: float | None
    soft_deadline: float | None
    hard_deadline: float | None
    context: object = None
    log: list = field(default_factory=list)

    @property
    def soft_sent(self) -> bool:
        return bool(self.log) and self.log[0] == _SOFT

    def owner(self) -> str | None:
        """Who owns the outcome: _HARD, _DONE, or None while undecided."""
        for entry in list(self.log):
            if entry == _HARD or entry == _DONE:
                return entry
        return None


class TimeLimits:
    def __init__(self, raise_in_thread=None, clock=time.monotonic):
        self._raise = _native.async_raise if raise_in_thread is None else raise_in_thread
        self._clock = clock
        self._runs: dict[int, Run] = {}

    def __len__(self) -> int:
        return len(self._runs)

    def arm(self, token: int, ident: int, soft: float | None, hard: float | None, context=None) -> Run | None:
        """Idempotent per token: re-arming after an interrupt returns the already-registered run."""
        if soft is None and hard is None:
            return None
        now = self._clock()
        run = Run(token, ident, soft, hard,
                  None if soft is None else now + soft,
                  None if hard is None else now + hard,
                  context)
        return self._runs.setdefault(token, run)

    def disarm(self, run: Run) -> bool:
        """Called by the task's thread when the task returns. True if it owns the outcome.
        Lock-free and idempotent: if a soft-limit exception interrupts it, call it again."""
        run.log.append(_DONE)
        owned = run.owner() == _DONE
        if self._runs.get(run.token) is run:
            self._runs.pop(run.token, None)
        if run.log[0] == _SOFT:
            while _SENT not in run.log:  # the supervisor is mid-send; let it finish before clearing
                time.sleep(0)
            self._raise(run.ident, None)
        return owned

    def tick(self) -> list[Run]:
        """Supervisor only. Delivers due soft limits; returns runs past their hard limit,
        which the caller then owns."""
        now = self._clock()
        expired = []
        for token, run in list(self._runs.items()):
            if run.hard_deadline is not None and now >= run.hard_deadline:
                run.log.append(_HARD)
                if run.owner() == _HARD:
                    self._runs.pop(token, None)
                    expired.append(run)
            elif run.soft_deadline is not None and now >= run.soft_deadline and _SOFT not in run.log:
                run.log.append(_SOFT)
                if run.log[0] == _SOFT:  # the task has not finished: we are cleared to send
                    try:
                        self._raise(run.ident, SoftTimeLimitExceeded)
                    finally:
                        run.log.append(_SENT)
        return expired
