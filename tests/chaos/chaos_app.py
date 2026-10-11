import random
import time

from mayu import App

app = App(result_ttl=3600)  # broker and queue come from MAYU_BROKER / MAYU_DEFAULT_QUEUE


@app.task(name="chaos.work", max_retries=0)
def work(i):
    time.sleep(random.uniform(0.05, 0.3))
    return i
