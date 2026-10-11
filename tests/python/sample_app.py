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
