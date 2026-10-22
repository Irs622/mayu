"""The worker's side of an env: one long-lived child interpreter (``python -m mayu.runner``) per ``app.env``.

Children are spawned, never forked, and talk over two pipes passed as extra fds (not stdin/stdout: task code
prints). A call's id is the task's native token. Every spawn is a `_Gen`; a reader thread is bound to its generation,
so a message from a child that was killed or replaced cannot touch the new one.
"""
import logging
import os
import subprocess
import threading
import time

from . import env as wire
from . import log as log_mod
from .supervisor import _describe

log = logging.getLogger("mayu.env")

MIN_BACKOFF = 1.0
MAX_BACKOFF = 30.0


class EnvSpawnError(RuntimeError):
    """The child could not be started or refused the handshake."""


class EnvUnavailable(Exception):
    """A call found no child and cannot get one now; retry it after `backoff_ms`."""

    def __init__(self, message: str, backoff_ms: int):
        super().__init__(message)
        self.backoff_ms = backoff_ms


class _Call:
    __slots__ = ("id", "event", "outcome")

    def __init__(self, call_id: int):
        self.id = call_id
        self.event = threading.Event()
        self.outcome: dict | None = None


class _Gen:
    """One spawned child process."""

    def __init__(self, proc: subprocess.Popen, rfile, wfile, epoch: int):
        self.proc = proc
        self.rfile = rfile
        self.wfile = wfile
        self.epoch = epoch
        self.lock = threading.Lock()  # guards `pending` and `dead`
        self.wlock = threading.Lock()  # one writer at a time
        self.pending: dict[int, _Call] = {}
        self.dead = False
        self.reader: threading.Thread | None = None

    def fail(self, reason: str) -> None:
        """Marks the child gone and settles every pending call as 'the child crashed'. Idempotent."""
        with self.lock:
            if self.dead:
                return
            self.dead = True
            calls = list(self.pending.values())
            self.pending.clear()
        for call in calls:
            call.outcome = {"o": "crash", "error": reason}
            call.event.set()

    def send(self, msg: dict) -> bool:
        with self.wlock:
            try:
                wire.write_frame(self.wfile, msg)
            except (OSError, ValueError):  # a closed pipe: the reader reports the crash
                return False
        return True

    def kill(self) -> None:
        try:
            self.proc.kill()
        except OSError:
            pass
        try:
            self.proc.wait(5.0)
        except subprocess.TimeoutExpired:
            log.error("env runner %s did not exit after SIGKILL", self.proc.pid)


class EnvChild:
    """The child interpreter of one env, respawned on demand when it dies.

    `owner` is the `WorkerRunner`: it provides `native` (progress), `_env_ready` and `_env_rebuilt` (pool accounting).
    """

    def __init__(self, owner, env: wire.Env, python: str, app_path: str, pools: dict[str, int],
                 store_result: bool, allow_pickle: bool):
        self.owner = owner
        self.env = env
        self.python = python
        self.app_path = app_path
        self.pools = dict(pools)
        self.store_result = store_result
        self.allow_pickle = allow_pickle
        self.gen: _Gen | None = None
        self._spawn_lock = threading.Lock()
        self._epoch = 0
        self._backoff = 0.0
        self._next_try = 0.0
        self._last_error = ""

    @property
    def name(self) -> str:
        return self.env.name

    # -- spawning ------------------------------------------------------------

    def start(self) -> None:
        """Spawns the first child and waits until it is ready; raises `EnvSpawnError` (no backoff)."""
        with self._spawn_lock:
            self.gen = self._spawn()

    def _ensure(self) -> _Gen:
        with self._spawn_lock:
            gen = self.gen
            if gen is not None and not gen.dead:
                return gen
            wait = self._next_try - time.monotonic()
            if wait > 0:  # a spawn just failed: don't spin
                raise EnvUnavailable(self._last_error, int(wait * 1000) + 1)
            try:
                gen = self._spawn()
            except EnvSpawnError as e:
                self._backoff = min(MAX_BACKOFF, max(MIN_BACKOFF, self._backoff * 2))
                self._next_try = time.monotonic() + self._backoff
                self._last_error = str(e)
                log.error("%s; retrying in %.0f s", e, self._backoff, extra={"env": self.name})
                raise EnvUnavailable(str(e), int(self._backoff * 1000)) from None
            self._backoff = 0.0
            self.gen = gen
            return gen

    def _spawn(self) -> _Gen:
        what = f"env {self.name!r} runner ({self.python})"
        r_in, w_in = os.pipe()  # parent -> child
        r_out, w_out = os.pipe()  # child -> parent
        argv = [self.python, "-m", "mayu.runner", "--app", self.app_path, "--env", self.name,
                "--fds", f"{r_in},{w_out}"]
        env = {**os.environ, "MAYU_PARENT_PID": str(os.getpid())}
        if log_mod._configured is not None:  # --log-format / --log-level may have come as flags, not env vars
            env["MAYU_LOG_FORMAT"], env["MAYU_LOG_LEVEL"] = log_mod._configured
        try:
            proc = subprocess.Popen(argv, pass_fds=(r_in, w_out), stdin=subprocess.DEVNULL, env=env)
        except OSError as e:
            for fd in (r_in, w_in, r_out, w_out):
                os.close(fd)
            raise EnvSpawnError(f"could not start the {what}: {e}") from e
        os.close(r_in)  # the child's ends: closing ours is what lets EOF reach both sides
        os.close(w_out)
        self._epoch += 1
        gen = _Gen(proc, os.fdopen(r_out, "rb"), os.fdopen(w_in, "wb"), self._epoch)
        try:
            self._handshake(gen, what)
        except BaseException:
            gen.kill()
            gen.rfile.close()
            gen.wfile.close()
            raise
        gen.reader = threading.Thread(target=self._read_loop, args=(gen,), name=f"mayu-env-{self.name}", daemon=True)
        gen.reader.start()
        self.owner._env_ready(self, gen)
        log.info("env runner started", extra={"env": self.name, "pid": gen.proc.pid, "python": self.python})
        return gen

    def _handshake(self, gen: _Gen, what: str) -> None:
        def gone(stage: str) -> EnvSpawnError:
            try:
                code = gen.proc.wait(2.0)
            except subprocess.TimeoutExpired:
                return EnvSpawnError(f"the {what} closed its pipe {stage}")
            return EnvSpawnError(f"the {what} exited with {_describe(code)} {stage}; is mayu installed for that "
                                 f"interpreter, and does it import the app?")

        theirs = wire.read_frame(gen.rfile)
        if theirs is None or theirs.get("t") != "hello":
            raise gone("before it said hello")
        problem = wire.check_hello(theirs)
        if problem:
            raise EnvSpawnError(f"the {what} cannot be used: {problem}")
        if not gen.send({"t": "start", "pools": self.pools, "store_result": self.store_result,
                         "allow_pickle": self.allow_pickle}):
            raise gone("before it was started")
        # No timeout: building the pools (loading models) can take minutes. EOF means the child is gone.
        reply = wire.read_frame(gen.rfile)
        if reply is None:
            raise gone("while loading the app")
        if reply.get("t") == "fatal":
            raise EnvSpawnError(f"the {what} failed to start: {reply.get('message')}")
        if reply.get("t") != "ready":
            raise EnvSpawnError(f"the {what} sent {reply.get('t')!r} instead of ready")

    # -- reading -------------------------------------------------------------

    def _read_loop(self, gen: _Gen) -> None:
        try:
            while True:
                msg = wire.read_frame(gen.rfile)
                if msg is None:
                    break
                self._dispatch(gen, msg)
        except Exception:
            log.exception("reading from the runner of env %r failed", self.name)
        finally:
            try:
                code = gen.proc.wait(2.0)
                reason = f"env {self.name!r} runner exited unexpectedly ({_describe(code)})"
            except subprocess.TimeoutExpired:
                gen.kill()
                reason = f"env {self.name!r} runner closed its pipe and was killed"
            gen.fail(reason)
            for f in (gen.rfile, gen.wfile):
                try:
                    f.close()
                except OSError:
                    pass

    def _dispatch(self, gen: _Gen, msg: dict) -> None:
        kind = msg.get("t")
        if kind == "progress":
            with gen.lock:
                live = msg["id"] in gen.pending
            native = self.owner.native
            if live and native is not None:
                native.progress(msg["id"], msg["done"], msg["total"])
        elif kind == "done":
            with gen.lock:
                call = gen.pending.pop(msg["id"], None)
            if call is not None:
                call.outcome = msg
                call.event.set()
        elif kind == "rebuilt":
            self.owner._env_rebuilt(self, gen, msg["pool"], bool(msg["ok"]))
        else:
            log.warning("env runner %r sent an unknown message %r", self.name, kind)

    # -- calls ---------------------------------------------------------------

    def call(self, call_id: int, task: str, content_type: str, payload, hard: float | None) -> tuple[_Gen, dict]:
        """Runs a task in the child; returns the generation that ran it and the outcome.

        The outcome is the child's ``done`` message, or ``{"o": "crash", "error"}`` if the child died, or
        ``{"o": "hard"}`` if `hard` seconds passed (the child was then killed). Raises `EnvUnavailable` when no
        child could be started."""
        gen = self._ensure()
        call = _Call(call_id)
        with gen.lock:
            if gen.dead:
                return gen, {"o": "crash", "error": f"env {self.name!r} runner exited before the task was sent"}
            gen.pending[call_id] = call
        if not gen.send({"t": "run", "id": call_id, "task": task, "ct": content_type, "payload": bytes(payload)}):
            gen.kill()
            gen.fail(f"env {self.name!r} runner closed its pipe")
        if not call.event.wait(hard):
            with gen.lock:
                still_ours = gen.pending.pop(call_id, None) is call
            if still_ours:
                gen.kill()
                gen.fail(f"env {self.name!r} runner was killed: another task ran past its hard time limit")
                return gen, {"o": "hard"}
            call.event.wait()  # the outcome landed while we decided
        return gen, call.outcome

    # -- stopping ------------------------------------------------------------

    def stop(self, timeout: float) -> None:
        """Asks the child to finish and exit; kills it after `timeout` seconds."""
        with self._spawn_lock:
            gen = self.gen
        if gen is None or gen.dead:
            return
        gen.send({"t": "stop"})
        try:
            gen.proc.wait(max(0.0, timeout))
        except subprocess.TimeoutExpired:
            log.warning("env runner %r did not stop in time; killing it", self.name)
            gen.kill()
        if gen.reader is not None:
            gen.reader.join(1.0)
        gen.fail(f"env {self.name!r} runner stopped")

    def kill(self) -> None:
        with self._spawn_lock:
            gen = self.gen
        if gen is not None:
            gen.kill()
            gen.fail(f"env {self.name!r} runner was killed")

