import asyncio
import signal
import threading
import time
import traceback

from . import _native
from .exceptions import Retry
from .serializers import for_content_type

_RECV_TIMEOUT_MS = 100


def _ms(seconds: float) -> int:
    return max(1, int(seconds * 1000))


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
        self.visibility_timeout = visibility_timeout
        self.heartbeat_interval = heartbeat_interval
        self.max_deliveries = max_deliveries
        self.app = app
        self.threads = threads
        self.queues = list(queues or app.queues())
        self.prefetch_multiplier = prefetch_multiplier
        self.shutdown_grace = shutdown_grace
        self.native = None
        self._running = threading.Event()
        self._stopped = threading.Event()
        self._threads: list[threading.Thread] = []
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
        for i in range(self.threads):
            t = threading.Thread(target=self._thread_main, name=f"mayu-worker-{i}", daemon=True)
            t.start()
            self._threads.append(t)

    def stop(self) -> None:
        self._running.clear()
        if self.native is not None:
            self.native.stop_fetching()
        self._stopped.set()

    def join(self) -> None:
        deadline = time.monotonic() + self.shutdown_grace
        for t in self._threads:
            t.join(max(0.0, deadline - time.monotonic()))
        while time.monotonic() < deadline:
            with self._pending_lock:
                if not self._pending_async:
                    break
            time.sleep(0.01)
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

    # -- execution ---------------------------------------------------------

    def _thread_main(self) -> None:
        native = self.native
        while self._running.is_set():
            ready = native.recv(_RECV_TIMEOUT_MS)
            if ready is not None:
                self._execute(ready)

    def _execute(self, ready) -> None:
        task = self.app.registry.get(ready.task)
        if task is None:
            self.native.complete_failure(ready.token, f"unknown task {ready.task!r}", "")
            return
        try:
            serializer = for_content_type(ready.content_type, allow_pickle=self.app.allow_pickle)
            args, kwargs = serializer.loads(ready.payload)
            if task.is_async:
                fut = asyncio.run_coroutine_threadsafe(task.fn(*args, **kwargs), self._loop)
                with self._pending_lock:
                    self._pending_async.add(fut)
                fut.add_done_callback(lambda f: self._async_done(ready.token, task, serializer, f))
                return
            result = task.fn(*args, **kwargs)
        except BaseException as e:
            self._report_error(ready.token, task, e)
        else:
            self._report_success(ready.token, serializer, result)

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
