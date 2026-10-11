import asyncio
import logging
import queue
import signal
import threading
import time
import traceback

from . import _native
from .exceptions import Retry, SoftTimeLimitExceeded, TimeLimitExceeded
from .limits import TimeLimits
from .recycle import rss_mb
from .serializers import for_content_type

log = logging.getLogger("mayu.worker")

_RECV_TIMEOUT_MS = 100
_SUPERVISOR_TICK = 0.05


def _hostport(host: str, port: int) -> str:
    return f"[{host}]:{port}" if ":" in host else f"{host}:{port}"


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


class _BadLink(Exception):
    """A link entry is malformed; the original error is the cause."""


class _Slot:
    """One task-running OS thread. A zombie's thread is abandoned: never joined, given no new work."""

    def __init__(self, name: str, serial: bool = False):
        self.name = name
        self.serial = serial
        self.thread: threading.Thread | None = None
        self.ident: int | None = None
        self.zombie = False


class WorkerRunner:
    """Runs tasks on real OS threads; Rust owns fetching, acks, retries and timers."""

    def __init__(self, app, *, threads: int, queues: list[str] | None = None,
                 prefetch_multiplier: int = 2, shutdown_grace: float = 30.0,
                 visibility_timeout: float = 30.0, heartbeat_interval: float = 5.0,
                 max_deliveries: int = 5, max_zombie_threads: int = 4,
                 max_tasks_per_process: int | None = None, max_memory_per_process: float | None = None,
                 metrics_port: int | None = None, metrics_host: str = "0.0.0.0"):
        if threads < 1:
            raise ValueError("threads must be >= 1")
        if visibility_timeout <= 0 or heartbeat_interval <= 0:
            raise ValueError("visibility_timeout and heartbeat_interval must be positive")
        if max_deliveries < 1:
            raise ValueError("max_deliveries must be >= 1")
        if max_zombie_threads < 0:
            raise ValueError("max_zombie_threads must be >= 0")
        if max_tasks_per_process is not None and max_tasks_per_process < 1:
            raise ValueError("max_tasks_per_process must be >= 1")
        if max_memory_per_process is not None and max_memory_per_process <= 0:
            raise ValueError("max_memory_per_process must be positive (MB)")
        if metrics_port is not None and not 0 <= metrics_port <= 65535:
            raise ValueError("metrics_port must be between 0 and 65535")
        self.app = app
        self.threads = threads
        self.queues = list(queues or app.queues())
        self.strict_priority = bool(queues)  # only a non-empty explicit list is a priority order
        self.prefetch_multiplier = prefetch_multiplier
        self.shutdown_grace = shutdown_grace
        self.visibility_timeout = visibility_timeout
        self.heartbeat_interval = heartbeat_interval
        self.max_deliveries = max_deliveries
        self.max_zombie_threads = max_zombie_threads
        self.max_tasks_per_process = max_tasks_per_process
        self.max_memory_per_process = max_memory_per_process
        self.metrics_port_requested = metrics_port
        self.metrics_host = metrics_host
        self.restart_reason: str | None = None
        self._restart_lock = threading.Lock()
        self._settled_count = 0
        self._settled_lock = threading.Lock()
        self._next_memory_check = 0.0
        self.native = None
        self._running = threading.Event()
        self._stopped = threading.Event()
        self._halt = threading.Event()
        self._slots: list[_Slot] = []
        self._slots_lock = threading.Lock()
        self._slot_seq = 0
        self._serial_q: queue.Queue = queue.Queue()
        self._serial_lock = threading.Lock()
        self._limits = TimeLimits()
        self._supervisor: threading.Thread | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._loop_thread: threading.Thread | None = None
        self._pending_async: set = set()
        self._pending_lock = threading.Lock()

    @property
    def worker_id(self) -> str | None:
        return None if self.native is None else self.native.worker_id

    @property
    def metrics_port(self) -> int | None:
        """The port `/metrics` is served on once started (the real one when 0 was requested)."""
        return None if self.native is None else self.native.metrics_port

    def start(self) -> None:
        rate_limits = {t.name: t.rate_per_sec for t in self.app.registry.values() if t.rate_per_sec is not None}
        self.native = _native.Worker(
            self.app.broker_url, self.queues, self.threads, self.prefetch_multiplier, self.app.result_ttl,
            visibility_timeout_ms=_ms(self.visibility_timeout),
            reclaim_interval_ms=_ms(min(5.0, self.visibility_timeout / 2)),
            heartbeat_interval_ms=_ms(self.heartbeat_interval),
            max_deliveries=self.max_deliveries,
            strict_priority=self.strict_priority,
            rate_limits=rate_limits,
            metrics_addr=(None if self.metrics_port_requested is None
                          else _hostport(self.metrics_host, self.metrics_port_requested)),
        )
        self._loop = asyncio.new_event_loop()
        self._loop_thread = threading.Thread(target=self._loop.run_forever, name="mayu-async", daemon=True)
        self._loop_thread.start()
        self._running.set()
        for _ in range(self.threads):
            self._start_slot()
        self._supervisor = threading.Thread(target=self._supervise, name="mayu-supervisor", daemon=True)
        self._supervisor.start()
        log.info("worker started", extra={"worker_id": self.worker_id, "queues": self.queues, "threads": self.threads})

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
        log.info("worker stopped", extra={"worker_id": self.worker_id})

    def run(self) -> str | None:
        """Runs until stopped. Returns why the worker stopped to recycle itself, or None."""
        self.start()
        if threading.current_thread() is threading.main_thread():
            for sig in (signal.SIGTERM, signal.SIGINT):
                signal.signal(sig, lambda *_: self.stop())
        while not self._stopped.wait(0.5):
            pass
        self.join()
        return self.restart_reason

    # -- recycling -----------------------------------------------------------

    def _request_restart(self, reason: str) -> None:
        with self._restart_lock:
            if self.restart_reason is not None:
                return
            self.restart_reason = reason
        log.warning("recycling worker: %s", reason, extra={"reason": reason})
        self.stop()

    def _settled(self) -> None:
        if self.max_tasks_per_process is None:
            return
        with self._settled_lock:
            self._settled_count += 1
            n = self._settled_count
        if n >= self.max_tasks_per_process:
            self._request_restart(f"max_tasks_per_process ({self.max_tasks_per_process}) reached")

    def _check_recycle(self) -> None:
        with self._slots_lock:
            zombies = sum(1 for s in self._slots if s.zombie and s.thread is not None and s.thread.is_alive())
        if zombies > self.max_zombie_threads:
            self._request_restart(f"{zombies} zombie threads exceed max_zombie_threads ({self.max_zombie_threads})")
        now = time.monotonic()
        if self.max_memory_per_process is not None and now >= self._next_memory_check:
            self._next_memory_check = now + 1.0
            rss = rss_mb()
            if rss is not None and rss > self.max_memory_per_process:
                self._request_restart(
                    f"memory {rss:.0f} MB exceeds max_memory_per_process ({self.max_memory_per_process:g} MB)")

    # -- threads -------------------------------------------------------------

    def _start_slot(self, serial: bool = False) -> _Slot:
        with self._slots_lock:
            kind = "serial" if serial else "worker"
            slot = _Slot(f"mayu-{kind}-{self._slot_seq}", serial=serial)
            self._slot_seq += 1
            self._slots.append(slot)
        target = self._serial_main if serial else self._thread_main
        slot.thread = threading.Thread(target=target, args=(slot,), name=slot.name, daemon=True)
        slot.thread.start()
        return slot

    def _ensure_serial(self) -> None:
        """Starts the serial thread unless a live one exists (never two at once)."""
        with self._serial_lock:
            with self._slots_lock:
                alive = any(s.serial and not s.zombie for s in self._slots)
            if not alive:
                self._start_slot(serial=True)

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

    def _serial_main(self, slot: _Slot) -> None:
        slot.ident = threading.get_ident()
        while not slot.zombie:
            try:
                token, task, serializer, args, kwargs, links = self._serial_q.get(timeout=_RECV_TIMEOUT_MS / 1000)
            except queue.Empty:
                if not self._running.is_set():
                    return  # drained after stop
                continue
            self._run_sync(token, task, serializer, args, kwargs, slot, links)

    def _supervise(self) -> None:
        while not self._halt.wait(_SUPERVISOR_TICK):
            try:
                for run in self._limits.tick():
                    try:
                        self._on_hard_limit(run)
                    except Exception:
                        log.exception("worker supervisor step failed")  # one failed report must not stop the others
                self._check_recycle()
            except Exception:
                log.exception("worker supervisor step failed")  # the supervisor must outlive a bad tick

    def _on_hard_limit(self, run) -> None:
        task, slot = run.context
        slot.zombie = True
        try:
            # Decide before reporting: once clients see the failure they may submit new work, and a
            # worker that is about to recycle must already have stopped fetching.
            self._check_recycle()
        except Exception:
            log.exception("worker supervisor step failed")
        try:
            self._report_error(run.token, task, TimeLimitExceeded(f"hard time limit ({run.hard}s) exceeded"))
        finally:
            if self._running.is_set():
                # keep capacity even if the report failed
                if slot.serial:
                    self._ensure_serial()
                else:
                    self._start_slot()

    # -- execution -----------------------------------------------------------

    def _execute(self, ready, slot: _Slot) -> None:
        task = self.app.registry.get(ready.task)
        if task is None:
            self.native.complete_failure(ready.token, f"unknown task {ready.task!r}", "")
            self._settled()
            return
        try:
            serializer = for_content_type(ready.content_type, allow_pickle=self.app.allow_pickle)
            args, kwargs, *extra = serializer.loads(ready.payload)
            links = extra[0].get("links", ()) if extra else ()
            if task.is_async:
                self._submit_async(ready.token, task, serializer, args, kwargs, links)
                return
        except BaseException as e:
            self._report_error(ready.token, task, e)
            return
        if not task.threadsafe:
            self._ensure_serial()
            self._serial_q.put((ready.token, task, serializer, args, kwargs, links))
            return
        self._run_sync(ready.token, task, serializer, args, kwargs, slot, links)

    def _run_sync(self, token, task, serializer, args, kwargs, slot: _Slot, links=()) -> None:
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
            self._report_success(token, serializer, value, links)
        else:
            self._report_error(token, task, value)

    def _submit_async(self, token, task, serializer, args, kwargs, links=()) -> None:
        coro = _with_time_limits(task.fn(*args, **kwargs), task.soft_time_limit, task.hard_time_limit)
        fut = asyncio.run_coroutine_threadsafe(coro, self._loop)
        with self._pending_lock:
            self._pending_async.add(fut)
        fut.add_done_callback(lambda f: self._async_done(token, task, serializer, links, f))

    def _async_done(self, token, task, serializer, links, fut) -> None:
        with self._pending_lock:
            self._pending_async.discard(fut)
        try:
            result = fut.result()
        except BaseException as e:
            self._report_error(token, task, e)
        else:
            self._report_success(token, serializer, result, links)

    def _report_success(self, token, serializer, result, links=()) -> None:
        try:
            data = None if self.app.result_ttl is None else serializer.dumps(result)
        except BaseException as e:
            self.native.complete_failure(token, f"result not serializable: {e!r}", traceback.format_exc())
            self._settled()
            return
        try:
            # link callbacks get the result first; Rust enqueues them atomically with the ack
            follow_ups = [self._follow_up(serializer, result, cb) for cb in links]
        except _BadLink as e:
            self.native.complete_failure(token, f"bad link callback: {e.__cause__!r}", traceback.format_exc())
            self._settled()
            return
        except BaseException as e:
            self.native.complete_failure(token, f"result not serializable: {e!r}", traceback.format_exc())
            self._settled()
            return
        try:
            self.native.complete_success(token, data, follow_ups)
        except (TypeError, ValueError, OverflowError) as e:
            # argument extraction failed before reaching Rust, so no outcome was recorded yet
            self.native.complete_failure(token, f"bad link callback: {e!r}", traceback.format_exc())
        self._settled()

    @staticmethod
    def _follow_up(serializer, result, cb) -> tuple:
        try:
            task, queue = str(cb["task"]), str(cb["queue"])
            max_retries = int(cb["max_retries"])
            if not 0 <= max_retries <= 2**32 - 1:
                raise ValueError(f"link max_retries out of range: {max_retries}")
            args, kwargs = cb["args"], cb["kwargs"]
            args = [result, *args]
        except (KeyError, TypeError, ValueError, OverflowError) as e:
            raise _BadLink() from e
        payload = bytes(serializer.dumps([args, kwargs]))
        return (task, queue, payload, max_retries)

    def _report_error(self, token, task, e: BaseException) -> None:
        if isinstance(e, Retry):
            ms = None if e.countdown is None else max(0, int(e.countdown * 1000))
            self.native.complete_retry(token, ms, repr(e.exc) if e.exc is not None else "Retry requested")
            self._settled()
        elif task.retry_on and isinstance(e, task.retry_on):
            self.native.complete_retry(token, None, repr(e))
            self._settled()
        else:
            self.native.complete_failure(token, repr(e), "".join(traceback.format_exception(e)))
            self._settled()
