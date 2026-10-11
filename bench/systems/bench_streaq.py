"""streaQ adapter: async tasks, 2 worker processes x c/2 concurrent tasks each."""
import asyncio
import os
import pathlib
import sys

import workload
from streaq import Worker

QUEUE = "bench"
worker = Worker(redis_url=workload.URL, queue_name=QUEUE,
                concurrency=int(os.environ.get("BENCH_STREAQ_CONCURRENCY", "32")))


@worker.task(name="bench.noop", max_tries=1, ttl=0)
async def noop():
    workload.noop_body()
    await workload.adone()


@worker.task(name="bench.realistic", max_tries=1, ttl=0)
async def realistic(payload):
    await workload.arealistic(payload)
    await workload.adone()


def enqueue(name: str, n: int) -> None:
    async def go():
        async with worker:
            for start in range(0, n, 1000):
                k = min(1000, n - start)
                batch = [noop.enqueue() for _ in range(k)] if name == "noop" else \
                        [realistic.enqueue(workload.PAYLOAD) for _ in range(k)]
                await worker.enqueue_many(batch)
    asyncio.run(go())


def worker_cmd(c: int):
    exe = pathlib.Path(sys.executable).with_name("streaq")
    return ([str(exe), "run", "bench_streaq:worker", "--workers", "2"],
            {"BENCH_STREAQ_CONCURRENCY": str(c // 2)}, f"2 processes × {c // 2} async tasks")
