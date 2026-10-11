"""RQ adapter: `rq worker-pool` with c workers; each job runs in a forked work horse (RQ's default)."""
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
    return ([str(rq), "worker-pool", "-n", str(c), "-u", workload.URL, "-q", QUEUE],
            {}, f"{c} workers, fork per job")
