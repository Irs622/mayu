"""Tasks for bench/mixed.py: workload.py's bodies without the Redis counter (the 3.13/3.14 venvs lack `redis`)."""
import workload
from mayu import App

app = App(broker=workload.URL, default_queue="mixed")  # results off: no result_ttl


@app.task(name="mixed.noop", max_retries=0)
def noop():
    workload.noop_body()


@app.task(name="mixed.realistic", max_retries=0)
def realistic(payload):
    workload.realistic(payload)
