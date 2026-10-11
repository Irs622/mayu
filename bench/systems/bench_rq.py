"""RQ adapter: `rq worker-pool` with c SimpleWorker processes; each runs jobs in-process, with no fork per job.

RQ's default Worker forks a work horse per job, and every horse opens fresh Redis connections. At ~900 jobs/s that
is ~1700 connections/s; through Docker's userland proxy the proxy-to-container leg piles up TIME_WAIT sockets
(~60 s each) until the ephemeral ports run out and Redis resets new connections ("Connection reset by peer",
jobs fail, the run stalls). So the default Worker cannot finish 20000 jobs on this setup. SimpleWorker avoids the
churn, and it is RQ's own documented option; it skips the per-job process isolation, which only helps RQ here.
"""
import pathlib
import sys

import workload
from redis import Redis
from rq import Queue

QUEUE = "bench"


def noop():
    workload.noop_body()
    workload.done()


def realistic(payload):
    workload.realistic(payload)
    workload.done()


def enqueue(name: str, n: int) -> None:
    q = Queue(QUEUE, connection=Redis.from_url(workload.URL))
    fn, args = (noop, ()) if name == "noop" else (realistic, (workload.PAYLOAD,))
    for start in range(0, n, 1000):
        q.enqueue_many([Queue.prepare_data(fn, args, result_ttl=0) for _ in range(min(1000, n - start))])


def worker_cmd(c: int):
    rq = pathlib.Path(sys.executable).with_name("rq")
    return ([str(rq), "worker-pool", "-n", str(c), "-u", workload.URL, "-w", "rq.worker.SimpleWorker", "-q", QUEUE],
            {}, f"{c} SimpleWorker processes, no fork per job")
