"""An app whose tasks run in an env's child process; test_envs.py starts workers on it.

Broker and queue come from MAYU_BROKER / MAYU_DEFAULT_QUEUE, like sample_app. MAYU_TEST_ENV_PYTHON picks the env's
interpreter (default: the one running the tests) and MAYU_TEST_FILE is a file the tasks append to.
"""
import asyncio
import itertools
import os
import sys
import threading
import time

import mayu
from mayu import App, DiscardInstance, Retry, SoftTimeLimitExceeded

app = App(result_ttl=60)
legacy = app.env("legacy", python=os.environ.get("MAYU_TEST_ENV_PYTHON") or sys.executable,
                 imports=["env_app_impl"])


def _append(line):
    with open(os.environ["MAYU_TEST_FILE"], "a") as f:
        f.write(f"{line}\n")


@app.task(name="envtest.add", env=legacy)
def add(a, b=0):
    return a + b


@app.task(name="envtest.where", env=legacy)
def where():
    return [os.getpid(), list(sys.version_info[:2])]


@app.task(name="envtest.local")
def local():
    return os.getpid()


@app.task(name="envtest.record")
def record(x):
    _append(x)


@app.task(name="envtest.aadd", env=legacy)
async def aadd(a, b):
    await asyncio.sleep(0)
    return a + b


@app.task(name="envtest.fail", env=legacy, max_retries=0)
def fail():
    raise ValueError("legacy failure")


@app.task(name="envtest.retry_later", env=legacy)
def retry_later(path):
    if not os.path.exists(path):
        open(path, "w").close()
        raise Retry(countdown=0.05)
    return "second try"


@app.task(name="envtest.flaky", env=legacy, retry_on=(KeyError,))
def flaky(path):
    if not os.path.exists(path):
        open(path, "w").close()
        raise KeyError("first time")
    return "retried"


@app.task(name="envtest.progressing", env=legacy)
def progressing(path):
    """Reports 1/2, waits for `path` to exist, then reports 2/2."""
    mayu.progress(1, 2)
    deadline = time.monotonic() + 30
    while not os.path.exists(path) and time.monotonic() < deadline:
        time.sleep(0.02)
    mayu.progress(2, 2)
    return "done"


@app.task(name="envtest.soft_caught", env=legacy, soft_time_limit=0.3)
def soft_caught():
    try:
        while True:
            pass
    except SoftTimeLimitExceeded:
        return "caught in the child"


@app.task(name="envtest.soft_uncaught", env=legacy, soft_time_limit=0.3, max_retries=0)
def soft_uncaught():
    while True:
        pass


@app.task(name="envtest.hang", env=legacy, hard_time_limit=0.5, max_retries=0)
def hang():
    time.sleep(60)


@app.task(name="envtest.crash_once", env=legacy)
def crash_once(path):
    if not os.path.exists(path):
        open(path, "w").close()
        os._exit(1)
    return "recovered"


@app.task(name="envtest.wait_for", env=legacy)
def wait_for(started, go):
    """Says it started, then waits for `go` to exist."""
    open(started, "w").close()
    deadline = time.monotonic() + 30
    while not os.path.exists(go) and time.monotonic() < deadline:
        time.sleep(0.02)
    return "finished"


_running = 0


@app.task(name="envtest.exclusive", env=legacy, threadsafe=False)
def exclusive():
    global _running
    _running += 1
    overlapped = _running > 1
    time.sleep(0.05)
    _running -= 1
    return overlapped


@app.task(name="envtest.unpicklable", env=legacy, max_retries=0)
def unpicklable():
    return object()


# -- pools, built in the child ---------------------------------------------------------------------------------------

_serial = itertools.count()
_lock = threading.Lock()


@app.pool("model", env=legacy)
def model(ctx):
    with _lock:
        return {"pid": os.getpid(), "index": ctx.index, "serial": next(_serial)}


@model.close
def _unload(instance):
    if "MAYU_TEST_FILE" in os.environ:
        _append(f"closed {instance['index']}")


@app.task(name="envtest.use_model", env=legacy)
def use_model(m=app.use("model")):
    return m


@app.task(name="envtest.discard_model", env=legacy, max_retries=0)
def discard_model(m=app.use("model")):
    raise DiscardInstance("broken")


# -- declared here, implemented in env_app_impl (which only the child imports) ----------------------------------------

@app.declare(name="envtest.declared", env="legacy")
def declared(x): ...
