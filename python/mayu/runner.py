"""``python -m mayu.runner``: the child of an env, run by a worker in the env's own interpreter.

It loads the app, builds the env's pools, and runs the tasks the worker sends over the pipes (see `mayu.env` for the
protocol). The worker owns fetching, acks and retries; the child only reports outcomes, so a crash here means the
worker settles the in-flight tasks as "run again".
"""
import argparse
import asyncio
import importlib
import logging
import os
import queue
import signal
import sys
import threading
import time
import traceback

from . import env as wire
from .exceptions import Retry, SoftTimeLimitExceeded
from .limits import TimeLimits
from .log import configure_logging
from .pool import PoolContext
from .progress import _async_current, _slot_local
from .serializers import for_content_type
from .supervisor import watch_parent
from .worker import WorkerRunner, _BadLink, _Held, _PoolState, _with_time_limits

log = logging.getLogger("mayu.runner")

_PROGRESS = object()  # wakes the writer to flush the latest progress of every running call
_TICK = 0.05


class _Out:
    """Everything the child says goes through one thread. Task threads can have a soft-limit exception pending, so
    they must never take a lock: they only `put` on a SimpleQueue and set a dict item, which are single C calls."""

    def __init__(self, wfile):
        self._wfile = wfile
        self._q: queue.SimpleQueue = queue.SimpleQueue()
        self._latest: dict[int, tuple] = {}  # call id -> last (done, total); a fast reporter costs one frame per flush
        self._thread = threading.Thread(target=self._loop, name="mayu-runner-writer", daemon=True)
        self._thread.start()

    def put(self, msg: dict) -> None:
        self._q.put(msg)

    def progress(self, token: int, done: float, total: float | None) -> bool:
        """What `mayu.progress` calls on a slot's ``native``."""
        self._latest[token] = (done, total)
        self._q.put(_PROGRESS)
        return True

    def close(self) -> None:
        self._q.put(None)
        self._thread.join()

    def _loop(self) -> None:
        try:
            while True:
                msg = self._q.get()
                if msg is None:
                    return
                if msg is _PROGRESS:
                    for call_id in list(self._latest):
                        got = self._latest.pop(call_id, None)
                        if got is not None:
                            wire.write_frame(self._wfile, {"t": "progress", "id": call_id, "done": got[0],
                                                           "total": got[1]})
                else:
                    wire.write_frame(self._wfile, msg)
        except (OSError, ValueError):
            os._exit(1)  # the worker is gone; nothing left to report to


class _ChildSlot:
    """What `mayu.progress` finds in `_slot_local` on a call's thread."""

    def __init__(self, native, ident: int):
        self.native = native
        self.ident = ident
        self.token = None


class _Child:
    def __init__(self, app, env: wire.Env, out: _Out, store_result: bool, allow_pickle: bool):
        self.app = app
        self.env = env
        self.out = out
        self.store_result = store_result
        self.allow_pickle = allow_pickle
        self.states: dict[str, _PoolState] = {}
        self.limits = TimeLimits()
        self.serial = threading.Lock()  # threadsafe=False tasks run one at a time
        self._threads: set[threading.Thread] = set()
        self._tlock = threading.Lock()
        threading.Thread(target=self._tick, name="mayu-runner-limits", daemon=True).start()

    def _tick(self) -> None:
        while True:
            time.sleep(_TICK)
            try:
                self.limits.tick()  # soft limits only: the worker enforces the hard one by killing us
            except Exception:
                log.exception("time limit step failed")

    # -- pools ---------------------------------------------------------------

    def build_pools(self, pools: dict[str, int]) -> None:
        try:
            for name, n in pools.items():
                pool = self.app.pools.get(name)
                if pool is None:
                    raise RuntimeError(f"the app has no pool {name!r}")
                if pool.env != self.env.name:
                    raise RuntimeError(f"pool {name!r} belongs to env {pool.env!r}, not {self.env.name!r}")
                state = self.states[name] = _PoolState(pool, n)
                for index in range(n):
                    started = time.monotonic()
                    try:
                        instance = pool.factory(PoolContext(name, index, n))
                    except Exception as e:
                        raise RuntimeError(f"pool {name!r} instance {index} failed to load: {e!r}") from e
                    state.push(index, instance)
                    log.info("pool instance loaded", extra={"pool": name, "index": index, "size": n,
                                                            "load_seconds": round(time.monotonic() - started, 3)})
        except BaseException:
            self.close_pools()
            raise

    def close_pools(self) -> None:
        for state in reversed(list(self.states.values())):
            for _, instance in sorted(state.drain(), key=lambda entry: entry[0], reverse=True):
                WorkerRunner._close_instance(state.pool, instance)

    def _rebuild(self, h: _Held) -> None:
        pool, state = h.state.pool, h.state
        WorkerRunner._close_instance(pool, h.instance)
        started = time.monotonic()
        try:
            fresh = pool.factory(PoolContext(pool.name, h.index, state.target))
        except Exception:
            log.exception("rebuilding instance %d of pool %r failed; the pool is one smaller", h.index, pool.name)
            self.out.put({"t": "rebuilt", "pool": pool.name, "ok": False})
            return
        state.push(h.index, fresh)  # before telling the worker, which then grows its ledger
        self.out.put({"t": "rebuilt", "pool": pool.name, "ok": True})
        log.info("pool instance rebuilt", extra={"pool": pool.name, "index": h.index,
                                                 "load_seconds": round(time.monotonic() - started, 3)})

    # -- calls ---------------------------------------------------------------

    def submit(self, msg: dict) -> None:
        self._spawn(self._call, msg)

    def _spawn(self, target, *args) -> None:
        def main():
            try:
                target(*args)
            finally:
                with self._tlock:
                    self._threads.discard(threading.current_thread())

        th = threading.Thread(target=main, name="mayu-runner-call", daemon=True)
        with self._tlock:
            self._threads.add(th)
        th.start()

    def wait(self) -> None:
        while True:
            with self._tlock:
                threads = list(self._threads)
            if not threads:
                return
            for th in threads:
                th.join()

    def _call(self, msg: dict) -> None:
        call_id = msg["id"]
        _slot_local.slot = slot = _ChildSlot(self.out, threading.get_ident())
        broken: list[_Held] = []
        try:
            outcome = self._execute(msg, slot, broken)
        except BaseException as e:  # not expected: _execute turns every task error into an outcome
            outcome = {"o": "fail", "error": repr(e), "traceback": traceback.format_exc()}
        outcome.update(t="done", id=call_id, broken=[h.state.pool.name for h in broken])
        self.out.put(outcome)
        for h in broken:  # after the report: the worker takes the instance out of its ledger first
            self._spawn(self._rebuild, h)

    def _execute(self, msg: dict, slot: _ChildSlot, broken: list) -> dict:
        name = msg["task"]
        task = self.app.registry.get(name)
        if task is None:
            return _fail(f"unknown task {name!r}")
        if task.env != self.env.name:
            return _fail(f"task {name!r} belongs to env {task.env!r}, not {self.env.name!r}")
        if not task.implemented:  # running the stub would record a bogus success
            return _fail(f"task {name!r} is declared but not implemented in env {self.env.name!r}; import its "
                         f"implementation, or list the module in app.env({self.env.name!r}, imports=[...])")
        try:
            serializer = for_content_type(msg["ct"], allow_pickle=self.allow_pickle)
            args, kwargs, *extra = serializer.loads(msg["payload"])
            links = extra[0].get("links", ()) if extra else ()
            if task.is_async:
                ok, value = True, self._run_async(task, msg["id"], args, kwargs)
            elif task.threadsafe:
                ok, value = self._run_sync(task, slot, msg["id"], args, kwargs, broken)
            else:
                with self.serial:
                    ok, value = self._run_sync(task, slot, msg["id"], args, kwargs, broken)
        except BaseException as e:
            return self._error(task, e)
        if not ok:
            return self._error(task, value)
        return self._success(serializer, links, value)

    def _run_async(self, task, call_id: int, args, kwargs):
        async def main():
            _async_current.set((self.out, call_id))  # in the task's own context: `mayu.progress` finds it
            return await _with_time_limits(task.fn(*args, **kwargs), task.soft_time_limit, None)

        return asyncio.run(main())

    def _run_sync(self, task, slot: _ChildSlot, call_id: int, args, kwargs, broken: list) -> tuple[bool, object]:
        """Mirrors `WorkerRunner._run_sync`: the soft-limit exception can land on any bytecode of this thread from
        arm until disarm returns, so one catch-and-resume loop covers that span and every step in it is idempotent."""
        held: list[_Held] = []
        try:
            for param, name in task.pools.items():
                state = self.states.get(name)
                entry = None if state is None else state.take()
                if entry is None:  # the worker's ledger admitted this task, so an instance must be free
                    raise RuntimeError(f"pool {name!r} has no free instance; this is a mayu bug")
                held.append(_Held(param, state, *entry))
        except Exception:
            for h in held:
                h.state.push(h.index, h.instance)
            raise
        kwargs = {**kwargs, **{h.param: h.instance for h in held}}
        run = outcome = None
        armed = settled = False
        while not settled:
            try:
                if not armed:
                    run = self.limits.arm(call_id, slot.ident, task.soft_time_limit, None)
                    armed = True
                if outcome is None:
                    slot.token = call_id
                    try:
                        outcome = (True, task.fn(*args, **kwargs))
                    except BaseException as e:
                        outcome = (False, e)
                    slot.token = None
                if run is not None:
                    self.limits.disarm(run)
                settled = True
            except SoftTimeLimitExceeded as e:
                slot.token = None  # the limit can land between the body and the clear above
                if outcome is None:
                    outcome = (False, e)  # the limit fired before/around the task body
                # otherwise it landed after the task finished: drop it and finish settling
        ok, value = outcome
        for h in held:  # healthy instances go back before the report: it frees the worker's `pool:` slot
            if not ok and WorkerRunner._is_broken(h, value):
                broken.append(h)
            else:
                h.state.push(h.index, h.instance)
        return outcome

    def _success(self, serializer, links, value) -> dict:
        try:
            data = serializer.dumps(value) if self.store_result else None
        except BaseException as e:
            return _fail(f"result not serializable: {e!r}", traceback.format_exc())
        try:
            follow_ups = [WorkerRunner._follow_up(serializer, value, cb) for cb in links]
        except _BadLink as e:
            return _fail(f"bad link callback: {e.__cause__!r}", traceback.format_exc())
        except BaseException as e:
            return _fail(f"result not serializable: {e!r}", traceback.format_exc())
        return {"o": "ok", "result": data, "links": [list(f) for f in follow_ups]}

    @staticmethod
    def _error(task, e: BaseException) -> dict:
        """What `WorkerRunner._report_error` would do with `e`."""
        if isinstance(e, Retry):
            ms = None if e.countdown is None else max(0, int(e.countdown * 1000))
            return {"o": "retry", "ms": ms, "error": repr(e.exc) if e.exc is not None else "Retry requested"}
        if task.retry_on and isinstance(e, task.retry_on):
            return {"o": "retry", "ms": None, "error": repr(e)}
        return _fail(repr(e), "".join(traceback.format_exception(e)))


def _fail(error: str, tb: str = "") -> dict:
    return {"o": "fail", "error": error, "traceback": tb}


def _load(args, start: dict, out: _Out) -> _Child:
    from .cli import load_app  # imported late: the CLI pulls in more than a child needs before the handshake

    app = load_app(args.app)
    env = app.envs.get(args.env)
    if env is None:
        raise RuntimeError(f"the app has no env {args.env!r}; declare it with app.env({args.env!r}, python=...)")
    for module in env.imports:
        importlib.import_module(module)
    child = _Child(app, env, out, bool(start.get("store_result")), bool(start.get("allow_pickle")))
    child.build_pools(start.get("pools") or {})
    return child


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="python -m mayu.runner", description="the child process of a mayu env")
    p.add_argument("--app", required=True)
    p.add_argument("--env", required=True)
    p.add_argument("--fds", required=True, help="the fds to read from and write to: R,W")
    args = p.parse_args(argv)
    configure_logging(os.environ.get("MAYU_LOG_FORMAT") or "text", os.environ.get("MAYU_LOG_LEVEL") or "INFO")
    # The worker decides when we stop (a `stop` message or EOF); a Ctrl-C or SIGTERM aimed at its process group
    # must not kill tasks the worker is waiting for.
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, signal.SIG_IGN)
    watch_parent(lambda: os._exit(1))
    r, w = (int(x) for x in args.fds.split(","))
    rfile, wfile = os.fdopen(r, "rb"), os.fdopen(w, "wb")
    # Before importing the app, so a version mismatch is reported even when the app does not import here.
    wire.write_frame(wfile, wire.hello())
    start = wire.read_frame(rfile)
    if start is None or start.get("t") != "start":
        return 1
    out = _Out(wfile)
    try:
        child = _load(args, start, out)
    except BaseException as e:
        log.exception("env %r could not start", args.env)
        out.put({"t": "fatal", "message": f"{type(e).__name__}: {e}"})
        out.close()
        return 1
    out.put({"t": "ready"})
    while True:
        msg = wire.read_frame(rfile)
        if msg is None or msg.get("t") == "stop":
            break
        if msg.get("t") == "run":
            child.submit(msg)
    child.wait()
    child.close_pools()
    out.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
