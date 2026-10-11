"""Dramatiq adapter: 2 processes x c/2 threads, Redis broker, no results middleware."""
import pathlib
import sys

import dramatiq
import workload
from dramatiq.brokers.redis import RedisBroker

QUEUE = "bench"
dramatiq.set_broker(RedisBroker(url=workload.URL))


@dramatiq.actor(actor_name="bench.noop", queue_name=QUEUE, max_retries=0)
def noop():
    workload.noop_body()
    workload.done()


@dramatiq.actor(actor_name="bench.realistic", queue_name=QUEUE, max_retries=0)
def realistic(payload):
    workload.realistic(payload)
    workload.done()


def enqueue(name: str, n: int) -> None:
    for _ in range(n):
        if name == "noop":
            noop.send()
        else:
            realistic.send(workload.PAYLOAD)


def worker_cmd(c: int):
    exe = pathlib.Path(sys.executable).with_name("dramatiq")
    return ([str(exe), "bench_dramatiq", "--processes", "2", "--threads", str(c // 2), "--queues", QUEUE],
            {}, f"2 processes × {c // 2} threads")
