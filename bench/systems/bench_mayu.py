"""mayu adapter: thread mode on 3.14t (2 processes x c/2 threads), process mode on 3.14 (c processes)."""
import sys

import workload
from mayu import App

QUEUE = "bench"
app = App(broker=workload.URL, default_queue=QUEUE)  # results off: no result_ttl


@app.task(name="bench.noop", max_retries=0)
def noop():
    workload.noop_body()
    workload.done()


@app.task(name="bench.realistic", max_retries=0)
def realistic(payload):
    workload.realistic(payload)
    workload.done()


def enqueue(name: str, n: int) -> None:
    if name == "noop":
        for _ in range(n):
            noop.send()
    else:
        for _ in range(n):
            realistic.send(workload.PAYLOAD)


def _cmd(*flags):
    return [sys.executable, "-m", "mayu", "worker", "bench_mayu:app", "--queues", QUEUE, *flags]


def thread_cmd(c: int):
    return (_cmd("--mode", "thread", "--processes", "2", "--threads", str(c // 2)),
            {"BENCH_REQUIRE_NOGIL": "1"}, f"2 processes × {c // 2} threads")


def process_cmd(c: int):
    return _cmd("--mode", "process", "--processes", str(c)), {}, f"{c} processes"
