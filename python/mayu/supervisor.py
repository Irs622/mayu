"""Parent of multi-process workers: keeps N child interpreters running, restarts any that exit,
and forwards graceful shutdown. Children are spawned, never forked (tokio and threads do not
survive fork)."""
import os
import signal
import subprocess
import sys
import threading
import time

RESTART_EXIT_CODE = 75  # EX_TEMPFAIL: a child asking to be replaced now (recycling, hard limit)
_POLL = 0.1
_STABLE_AFTER = 5.0  # a child that lived this long resets its crash backoff
_KILL_MARGIN = 5.0   # seconds past shutdown_grace before SIGKILL


def _describe(code: int) -> str:
    return f"signal {-code}" if code < 0 else f"code {code}"


class _Child:
    def __init__(self) -> None:
        self.proc: subprocess.Popen | None = None
        self.started = 0.0
        self.backoff = 0.0
        self.next_start = 0.0


class ProcessSupervisor:
    def __init__(self, argv: list[str], processes: int, *, shutdown_grace: float = 30.0,
                 env: dict | None = None, min_backoff: float = 1.0, max_backoff: float = 30.0):
        if processes < 1:
            raise ValueError("processes must be >= 1")
        self.argv = list(argv)
        self.shutdown_grace = shutdown_grace
        self.min_backoff = min_backoff
        self.max_backoff = max_backoff
        self.restarts = 0
        self.spawn_failures = 0
        self._env = {**(os.environ if env is None else env), "MAYU_PARENT_PID": str(os.getpid())}
        self._children = [_Child() for _ in range(processes)]
        self._stop = threading.Event()

    def pids(self) -> list[int]:
        return [c.proc.pid for c in self._children if c.proc is not None and c.proc.poll() is None]

    def stop(self) -> None:
        self._stop.set()

    def run(self) -> int:
        if threading.current_thread() is threading.main_thread():
            for sig in (signal.SIGTERM, signal.SIGINT):
                signal.signal(sig, lambda *_: self.stop())
        try:
            for c in self._children:
                self._spawn(c)
            while not self._stop.wait(_POLL):
                now = time.monotonic()
                for c in self._children:
                    if c.proc is None:
                        if now >= c.next_start:
                            self._spawn(c)
                        continue
                    code = c.proc.poll()
                    if code is None:
                        continue
                    pid, lived = c.proc.pid, now - c.started
                    c.proc = None
                    if code == RESTART_EXIT_CODE or lived >= _STABLE_AFTER:
                        c.backoff = 0.0
                    else:
                        c.backoff = min(self.max_backoff, max(self.min_backoff, c.backoff * 2))
                    c.next_start = now + c.backoff
                    self.restarts += 1
                    delay = f" in {c.backoff:.1f}s" if c.backoff else ""
                    print(f"mayu: worker process {pid} exited with {_describe(code)}; restarting{delay}",
                          file=sys.stderr, flush=True)
        finally:
            self._shutdown()
        return 0

    def _spawn(self, c: _Child) -> None:
        try:
            c.proc = subprocess.Popen(self.argv, env=self._env)
        except OSError as exc:
            # Transient (EAGAIN/EMFILE/ENOMEM) or permanent: retry with the crash backoff.
            self.spawn_failures += 1
            c.backoff = min(self.max_backoff, max(self.min_backoff, c.backoff * 2))
            c.next_start = time.monotonic() + c.backoff
            print(f"mayu: failed to start worker process: {exc}", file=sys.stderr, flush=True)
            return
        c.started = time.monotonic()

    def _shutdown(self) -> None:
        live = [c.proc for c in self._children if c.proc is not None and c.proc.poll() is None]
        for p in live:
            p.send_signal(signal.SIGTERM)
        deadline = time.monotonic() + self.shutdown_grace + _KILL_MARGIN
        for p in live:
            try:
                p.wait(timeout=max(0.1, deadline - time.monotonic()))
            except subprocess.TimeoutExpired:
                p.kill()
                p.wait()


def watch_parent(on_orphaned, interval: float = 1.0) -> threading.Thread | None:
    """In a supervised child: calls `on_orphaned()` once if the supervising parent goes away."""
    raw = os.environ.get("MAYU_PARENT_PID")
    if not raw:
        return None
    parent = int(raw)

    def loop() -> None:
        while True:
            time.sleep(interval)
            if os.getppid() != parent:
                on_orphaned()
                return

    th = threading.Thread(target=loop, name="mayu-parent-watch", daemon=True)
    th.start()
    return th
