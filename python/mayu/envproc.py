"""The worker's side of an env: one long-lived child interpreter (``python -m mayu.runner``) per ``app.env``.

Children are spawned, never forked, and talk over two pipes passed as extra fds (not stdin/stdout: task code
prints). A call's id is the task's native token. Every spawn is a `_Gen`; a reader thread is bound to its generation,
so a message from a child that was killed or replaced cannot touch the new one.

The child only hosts pool instances. The worker keeps every pool's bookkeeping and drives each build, close, park,
unpark and stage over the pipe through a `_RemotePool` (see `mayu.env` for the protocol).
"""
import itertools
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
    """One spawned child process. `dead` is set once, for good, when it exits or is killed."""

    def __init__(self, proc: subprocess.Popen, rfile, wfile, epoch: int):
        self.proc = proc
        self.rfile = rfile
        self.wfile = wfile
        self.epoch = epoch
        self.lock = threading.Lock()  # guards `pending` and `dead`
        self.wlock = threading.Lock()  # one writer at a time
        self.pending: dict[int, _Call] = {}  # task calls
        self.ops: dict[int, _Call] = {}  # pool operations
        self.dead = False
        self.reader: threading.Thread | None = None

    def fail(self, reason: str) -> bool:
        """Marks the child gone and settles every pending call as 'the child crashed'. Returns whether this call
        did it (it is idempotent)."""
        with self.lock:
            if self.dead:
                return False
            self.dead = True
            calls = list(self.pending.values())
            ops = list(self.ops.values())
            self.pending.clear()
            self.ops.clear()
        for call in calls:
            call.outcome = {"o": "crash", "error": reason}
            call.event.set()
        for call in ops:
            call.outcome = {"ok": False, "error": reason}
            call.event.set()
        return True

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

    `owner` is the `WorkerRunner`: it provides `native` (progress).
    """

    def __init__(self, owner, env: wire.Env, python: str, app_path: str, store_result: bool, allow_pickle: bool):
        self.owner = owner
        self.env = env
        self.python = python
        self.app_path = app_path
        self.store_result = store_result
        self.allow_pickle = allow_pickle
        self.gen: _Gen | None = None
        self._spawn_lock = threading.Lock()
        self._epoch = 0
        self._backoff = 0.0
        self._next_try = 0.0
        self._last_error = ""
        self._op_ids = itertools.count(1)  # pool operations have their own ids, apart from the tasks' tokens
        self._closing = False

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
            if self._closing:
                raise EnvUnavailable(f"env {self.name!r} runner was stopped", 0)
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
        if not gen.send({"t": "start", "store_result": self.store_result, "allow_pickle": self.allow_pickle}):
            raise gone("before it was started")
        # No timeout: importing the app can take a while. EOF means the child is gone.
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
            self._lost(gen, reason)
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
        elif kind == "opdone":
            with gen.lock:
                call = gen.ops.pop(msg["id"], None)
            if call is not None:
                call.outcome = msg
                call.event.set()
        else:
            log.warning("env runner %r sent an unknown message %r", self.name, kind)

    def _lost(self, gen: _Gen, reason: str) -> None:
        """Settles a dead generation. The instances it held stay in the worker's pools as ghosts (dead `_Remote`
        handles) that whoever touches them next settles; see `WorkerRunner._run_env`."""
        gen.fail(reason)

    # -- calls ---------------------------------------------------------------

    def call(self, call_id: int, task: str, content_type: str, payload, hard: float | None, pools: dict | None = None,
             tp: str | None = None, handles=()) -> tuple[_Gen, dict]:
        """Runs a task in the child; returns the generation that ran it and the outcome.

        `pools` is ``{param: [pool, index]}``, the instances the worker checked out for it, which are `handles`
        (`_Remote`, alive when revived); `tp` is the traceparent the task's spans nest under. The outcome is the
        child's ``done`` message, or ``{"o": "crash", "error"}`` if the child died, or ``{"o": "hard"}`` if `hard`
        seconds passed (the child was then killed). Raises `EnvUnavailable` when no child could be started."""
        if any(h.gen.dead for h in handles):  # it died again since the worker revived them: run the task again
            return handles[0].gen, {"o": "crash", "error": f"env {self.name!r} runner exited before the task was sent"}
        gen = self._ensure()
        if any(h.gen is not gen for h in handles):  # only one generation is alive at a time
            return gen, {"o": "fail", "error": "a pool instance of the task belongs to another runner generation; "
                                               "this is a mayu bug", "traceback": ""}
        call = _Call(call_id)
        with gen.lock:
            if gen.dead:
                return gen, {"o": "crash", "error": f"env {self.name!r} runner exited before the task was sent"}
            gen.pending[call_id] = call
        if not gen.send({"t": "run", "id": call_id, "task": task, "ct": content_type, "payload": bytes(payload),
                         "pools": pools or {}, "tp": tp}):
            gen.kill()
            self._lost(gen, f"env {self.name!r} runner closed its pipe")
        if not call.event.wait(hard):
            with gen.lock:
                still_ours = gen.pending.pop(call_id, None) is call
            if still_ours:
                gen.kill()
                self._lost(gen, f"env {self.name!r} runner was killed: another task ran past its hard time limit")
                return gen, {"o": "hard"}
            call.event.wait()  # the outcome landed while we decided
        return gen, call.outcome

    def op(self, gen: _Gen, op: str, pool: str, index: int, size: int | None = None) -> None:
        """Runs a pool operation (build, stage, close, park, unpark) in the child of `gen` and waits for it, with no
        timeout: loading a model takes minutes. Raises `RuntimeError` if it failed or the child died."""
        call = _Call(next(self._op_ids))
        with gen.lock:
            if gen.dead:
                raise RuntimeError(f"env {self.name!r} runner is gone; cannot {op} an instance of pool {pool!r}")
            gen.ops[call.id] = call
        msg = {"t": "op", "id": call.id, "op": op, "pool": pool, "index": index}
        if size is not None:
            msg["size"] = size
        if not gen.send(msg):
            gen.kill()
            self._lost(gen, f"env {self.name!r} runner closed its pipe")
        call.event.wait()
        if not call.outcome["ok"]:
            raise RuntimeError(f"{op} of pool {pool!r} instance {index} failed in env {self.name!r}: "
                               f"{call.outcome['error']}")

    # -- stopping ------------------------------------------------------------

    def stop(self, timeout: float) -> None:
        """Asks the child to finish and exit; kills it after `timeout` seconds."""
        with self._spawn_lock:
            self._closing = True  # no respawn from here on
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
        self._lost(gen, f"env {self.name!r} runner stopped")

    def kill(self) -> None:
        with self._spawn_lock:
            self._closing = True
            gen = self.gen
        if gen is not None:
            gen.kill()
            self._lost(gen, f"env {self.name!r} runner was killed")


# -- pools ---------------------------------------------------------------------------------------------------------


class _DeadGen:
    """The generation of a ghost made without a child (see `WorkerRunner._rebuild_env`): dead from the start."""

    dead = True
    epoch = 0


DEAD_GEN = _DeadGen()


class _Remote:
    """An instance of an env pool: it lives in the child of generation `gen` at (pool, index) and dies with it."""

    __slots__ = ("pool", "index", "gen")

    def __init__(self, pool: str, index: int, gen: _Gen):
        self.pool = pool
        self.index = index
        self.gen = gen

    @property
    def epoch(self) -> int:
        return self.gen.epoch

    def __repr__(self) -> str:
        return f"<remote {self.pool!r}[{self.index}] of generation {self.gen.epoch}>"


class _RemotePool:
    """What the worker uses in place of a `Pool` declared with ``env=``: the same attributes, and the same hooks
    (each None exactly when the real pool's is), but every hook is a blocking call to the env's child, which holds
    the instances. The worker's pool code then needs no special case for an env."""

    def __init__(self, real, child: EnvChild):
        self.real = real
        self.child = child
        self.app = real.app
        self.name = real.name
        self.resources = real.resources
        self.discard_on = real.discard_on
        self.linger = real.linger
        self.parked = real.parked
        self.env = real.env

    def __repr__(self) -> str:
        return f"<RemotePool {self.name!r} in env {self.env!r}>"

    def _live(self):
        return self.child._ensure()  # EnvUnavailable: no child can be started now

    def _build(self, op: str, ctx) -> _Remote:
        gen = self._live()
        self.child.op(gen, op, self.name, ctx.index, ctx.size)
        return _Remote(self.name, ctx.index, gen)

    def factory(self, ctx) -> _Remote:
        return self._build("build", ctx)

    # The hooks are looked up when asked, so each is None exactly when the real pool's is at that moment.
    # `closer` is never None: the child must drop the instance even when the pool has no close hook.
    @property
    def closer(self):
        return self._close

    @property
    def parker(self):
        return None if self.real.parker is None else self._park

    @property
    def unparker(self):
        return None if self.real.unparker is None else self._unpark

    @property
    def stager(self):
        return None if self.real.stager is None else self._stage

    def _stage(self, ctx) -> _Remote:
        return self._build("stage", ctx)

    def _close(self, handle: _Remote) -> None:
        if handle.gen.dead:  # its memory went with the child
            return
        try:
            self.child.op(handle.gen, "close", self.name, handle.index)
        except RuntimeError:
            if not handle.gen.dead:  # the close hook failed, not the child
                raise

    def _park(self, handle: _Remote) -> None:
        self.child.op(handle.gen, "park", self.name, handle.index)

    def _unpark(self, handle: _Remote) -> None:
        self.child.op(handle.gen, "unpark", self.name, handle.index)
