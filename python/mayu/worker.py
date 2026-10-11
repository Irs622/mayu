import asyncio
import signal
import threading
import time
import traceback

from . import _native
from .exceptions import Retry, SoftTimeLimitExceeded, TimeLimitExceeded
from .limits import TimeLimits
from .serializers import for_content_type

_RECV_TIMEOUT_MS = 100
_SUPERVISOR_TICK = 0.05


def _ms(seconds: float) -> int:
    return max(1, int(seconds * 1000))


async def _with_time_limits(coro, soft: float | None, hard: float | None):
    limit = soft if soft is not None else hard
    if limit is None:
        return await coro
    cm = asyncio.timeout(limit)
    try:
        async with cm:
            return await coro
    except TimeoutError:
        if not cm.expired():
            raise  # the task's own TimeoutError
        if soft is not None:
            raise SoftTimeLimitExceeded(f"soft time limit ({soft}s) exceeded") from None
        raise TimeLimitExceeded(f"hard time limit ({hard}s) exceeded") from None


class _Slot:
    """One task-running OS thread. A zombie's thread is abandoned: never joined, given no new work."""

    def __init__(self, name: str):
        self.name = name
        self.thread: threading.Thread | None = None
        self.ident: int | None = None
        self.zombie = False


class WorkerRunner:
    """Runs tasks on real OS threads; Rust owns fetching, acks, retries and timers."""

    def __init__(self, app, *, threads: int, queues: list[str] | None = None,
                 prefetch_multiplier: int = 2, shutdown_grace: float = 30.0,
                 visibility_timeout: float = 30.0, heartbeat_interval: float = 5.0,
                 max_deliveries: int = 5):
        if threads < 1:
            raise ValueError("threads must be >= 1")
        if visibility_timeout <= 0 or heartbeat_interval <= 0:
            raise ValueError("visibility_timeout and heartbeat_interval must be positive")
        if max_deliveries < 1:
            raise ValueError("max_deliveries must be >= 1")
        self.app = app
        self.threads = threads
        self.queues = list(queues or app.queues())
        self.prefetch_multiplier = prefetch_multiplier
        self.shutdown_grace = shutdown_grace
        self.visibility_timeout = visibility_timeout
        self.heartbeat_interval = heartbeat_interval
        self.max_deliveries = max_deliveries
        self.native = None
        self._running = threading.Event()
        self._stopped = threading.Event()
        self._halt = threading.Event()
        self._slots: list[_Slot] = []
        self._slots_lock = threading.Lock()
        self._slot_seq = 0
        self._limits = TimeLimits()
        self._supervisor: threading.Thread | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._loop_thread: threading.Thread | None = None
        self._pending_async: set = set()
        self._pending_lock = threading.Lock()

    @property
    def worker_id(self) -> str | None:
        return None if self.native is None else self.native.worker_id

    def start(self) -> None:
        self.native = _native.Worker(
            self.app.broker_url, self.queues, self.threads, self.prefetch_multiplier, self.app.result_ttl,
            visibility_timeout_ms=_ms(self.visibility_timeout),
            reclaim_interval_ms=_ms(min(5.0, self.visibility_timeout / 2)),
            heartbeat_interval_ms=_ms(self.heartbeat_interval),
            max_deliveries=self.max_deliveries,
        )
        self._loop = asyncio.new_event_loop()
        self._loop_thread = threading.Thread(target=self._loop.run_forever, name="mayu-async", daemon=True)
        self._loop_thread.start()
        self._running.set()
        for _ in range(self.threads):
            self._start_slot()
        self._supervisor = threading.Thread(target=self._supervise, name="mayu-supervisor", daemon=True)
        self._supervisor.start()

    def stop(self) -> None:
        self._running.clear()
        if self.native is not None:
            self.native.stop_fetching()
        self._stopped.set()

    def join(self) -> None:
        deadline = time.monotonic() + self.shutdown_grace
        # Zombie threads are never waited for; a thread may turn zombie while we wait.
        while time.monotonic() < deadline:
            busy = [s for s in self._live_slots() if s.thread.is_alive()]
            if not busy:
                break
            busy[0].thread.join(min(0.05, max(0.0, deadline - time.monotonic())))
        while time.monotonic() < deadline:
            with self._pending_lock:
                if not self._pending_async:
                    break
            time.sleep(0.01)
        self._halt.set()
        self._supervisor.join(1.0)
        remaining_ms = int(max(0.0, deadline - time.monotonic()) * 1000)
        self.native.shutdown(max(remaining_ms, 1000))
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._loop_thread.join(1.0)
        if not self._loop_thread.is_alive():
            self._loop.close()

    def run(self) -> None:
        self.start()
        if threading.current_thread() is threading.main_thread():
            for sig in (signal.SIGTERM, signal.SIGINT):
                signal.signal(sig, lambda *_: self.stop())
        while not self._stopped.wait(0.5):
            pass
        self.join()

    # -- threads -------------------------------------------------------------

    def _start_slot(self) -> _Slot:
        with self._slots_lock:
            slot = _Slot(f"mayu-worker-{self._slot_seq}")
            self._slot_seq += 1
            self._slots.append(slot)
        slot.thread = threading.Thread(target=self._thread_main, args=(slot,), name=slot.name, daemon=True)
        slot.thread.start()
        return slot

    def _live_slots(self) -> list[_Slot]:
        with self._slots_lock:
            return [s for s in self._slots if not s.zombie and s.thread is not None]

    def _thread_main(self, slot: _Slot) -> None:
        slot.ident = threading.get_ident()
        native = self.native
        while self._running.is_set() and not slot.zombie:
            ready = native.recv(_RECV_TIMEOUT_MS)
            if ready is not None:
                self._execute(ready, slot)

    def _supervise(self) -> None:
        while not self._halt.wait(_SUPERVISOR_TICK):
            for run in self._limits.tick():
                try:
                    self._on_hard_limit(run)
                except Exception:
                    traceback.print_exc()  # the supervisor must outlive a failed report

    def _on_hard_limit(self, run) -> None:
        task, slot = run.context
        slot.zombie = True
        try:
            self._report_error(run.token, task, TimeLimitExceeded(f"hard time limit ({run.hard}s) exceeded"))
        finally:
            if self._running.is_set():
                self._start_slot()  # keep capacity even if the report failed

    # -- execution -----------------------------------------------------------

    def _execute(self, ready, slot: _Slot) -> None:
        task = self.app.registry.get(ready.task)
        if task is None:
            self.native.complete_failure(ready.token, f"unknown task {ready.task!r}", "")
            return
        try:
            serializer = for_content_type(ready.content_type, allow_pickle=self.app.allow_pickle)
            args, kwargs = serializer.loads(ready.payload)
            if task.is_async:
                self._submit_async(ready.token, task, serializer, args, kwargs)
                return
        except BaseException as e:
            self._report_error(ready.token, task, e)
            return
        self._run_sync(ready.token, task, serializer, args, kwargs, slot)

    def _run_sync(self, token, task, serializer, args, kwargs, slot: _Slot) -> None:
        """Exactly one outcome per task, and a soft-limit exception never leaves this method.

        The soft-limit exception can land on any bytecode of this thread from the moment the run is
        armed until `disarm` returns (which also clears one that is still pending). It is delivered
        at most once per run, so one catch-and-resume loop covers that whole span; every step in it
        is idempotent (`arm` returns the registered run, `outcome` is kept, `disarm` can be re-run).
        """
        run = outcome = owned = None
        armed = settled = False
        while not settled:
            try:
                if not armed:
                    run = self._limits.arm(token, slot.ident, task.soft_time_limit, task.hard_time_limit,
                                           context=(task, slot))
                    armed = True
                if outcome is None:
                    try:
                        outcome = (True, task.fn(*args, **kwargs))
                    except BaseException as e:
                        outcome = (False, e)
                owned = run is None or self._limits.disarm(run)
                settled = True
            except SoftTimeLimitExceeded as e:
                if outcome is None:
                    outcome = (False, e)  # the limit fired before/around the task body
                # otherwise it landed after the task finished: drop it and finish settling
        if not owned:
            slot.zombie = True  # the hard limit already settled this task; this thread retires
            return
        ok, value = outcome
        if ok:
            self._report_success(token, serializer, value)
        else:
            self._report_error(token, task, value)

    def _submit_async(self, token, task, serializer, args, kwargs) -> None:
        coro = _with_time_limits(task.fn(*args, **kwargs), task.soft_time_limit, task.hard_time_limit)
        fut = asyncio.run_coroutine_threadsafe(coro, self._loop)
        with self._pending_lock:
            self._pending_async.add(fut)
        fut.add_done_callback(lambda f: self._async_done(token, task, serializer, f))

    def _async_done(self, token, task, serializer, fut) -> None:
        with self._pending_lock:
            self._pending_async.discard(fut)
        try:
            result = fut.result()
        except BaseException as e:
            self._report_error(token, task, e)
        else:
            self._report_success(token, serializer, result)

    def _report_success(self, token, serializer, result) -> None:
        if self.app.result_ttl is None:
            self.native.complete_success(token, None)
            return
        try:
            data = serializer.dumps(result)
        except BaseException as e:
            self.native.complete_failure(token, f"result not serializable: {e!r}", traceback.format_exc())
            return
        self.native.complete_success(token, data)

    def _report_error(self, token, task, e: BaseException) -> None:
        if isinstance(e, Retry):
            ms = None if e.countdown is None else max(0, int(e.countdown * 1000))
            self.native.complete_retry(token, ms, repr(e.exc) if e.exc is not None else "Retry requested")
        elif task.retry_on and isinstance(e, task.retry_on):
            self.native.complete_retry(token, None, repr(e))
        else:
            self.native.complete_failure(token, repr(e), "".join(traceback.format_exception(e)))
