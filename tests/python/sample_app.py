from mayu import App

app = App(result_ttl=60)  # broker and queue come from MAYU_BROKER / MAYU_DEFAULT_QUEUE


@app.task(name="sample.add")
def add(a, b):
    return a + b


import os
import time


@app.task(name="sample.slow_once")
def slow_once(path):
    if os.path.exists(path):
        return "recovered"
    open(path, "w").close()
    time.sleep(60)
    return "slow"


import uuid

BOOT = uuid.uuid4().hex


@app.task(name="sample.boot")
def boot():
    return BOOT


@app.task(name="sample.sleepy", hard_time_limit=0.5)
def sleepy():
    time.sleep(30)


@app.task(name="sample.whoami")
def whoami():
    time.sleep(0.2)
    return [BOOT, os.getpid()]


@app.task(name="sample.fail", max_retries=0)
def fail():
    raise ValueError("sample failure")


@app.task(name="sample.flaky_file", max_retries=0)
def flaky_file(path):
    """Fails until `path` exists."""
    if not os.path.exists(path):
        raise RuntimeError("flag file missing")
    return "ok"
