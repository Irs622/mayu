"""Celery adapter: prefork pool with c processes (Celery's default pool), Redis broker, no result backend."""
import sys

import workload
from celery import Celery

QUEUE = "bench"
app = Celery("bench_celery", broker=workload.URL)
app.conf.update(task_ignore_result=True, task_default_queue=QUEUE, worker_hijack_root_logger=False,
                broker_connection_retry_on_startup=True)


@app.task(name="bench.noop")
def noop():
    workload.noop_body()
    workload.done()


@app.task(name="bench.realistic")
def realistic(payload):
    workload.realistic(payload)
    workload.done()


def enqueue(name: str, n: int) -> None:
    with app.producer_pool.acquire(block=True) as producer:
        for _ in range(n):
            if name == "noop":
                noop.apply_async(producer=producer)
            else:
                realistic.apply_async((workload.PAYLOAD,), producer=producer)


def worker_cmd(c: int):
    return ([sys.executable, "-m", "celery", "-A", "bench_celery", "worker", "-P", "prefork", "-c", str(c),
             "-Q", QUEUE, "--without-heartbeat", "--without-gossip", "--without-mingle", "-l", "WARNING"],
            {}, f"{c} prefork processes")
