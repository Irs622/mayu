"""Task bodies and the completion counter, shared by every system so they all do identical work."""
import asyncio
import json
import os
import sys
import sysconfig
import threading
import time

import redis

URL = os.environ.get("MAYU_BENCH_REDIS", "redis://127.0.0.1:6390/15")
DONE_KEY = "bench:done"
IO_SECONDS = 0.005     # simulated I/O per realistic task
CPU_ROUNDS = 20_000    # about 1 ms of pure-Python work per realistic task
PAYLOAD = json.dumps({
    "user": {"id": 42, "name": "x" * 16, "tags": [f"t{i}" for i in range(50)]},
    "items": [{"sku": f"s{i}", "qty": i, "price": i * 1.5} for i in range(40)],
})

_local = threading.local()
_aclients: dict = {}


def _client() -> redis.Redis:
    c = getattr(_local, "client", None)
    if c is None:
        c = _local.client = redis.Redis.from_url(URL)
    return c


def done() -> None:
    _client().incr(DONE_KEY)


async def adone() -> None:
    import redis.asyncio as aredis
    loop = asyncio.get_running_loop()
    c = _aclients.get(loop)
    if c is None:
        c = _aclients[loop] = aredis.Redis.from_url(URL)
    await c.incr(DONE_KEY)


def noop_body() -> None:
    pass


def _cpu(doc) -> int:
    h = 0
    for i in range(CPU_ROUNDS):
        h = (h * 31 + i) & 0xFFFFFFFF
    return h + len(doc["items"])


def realistic(payload: str) -> int:
    doc = json.loads(payload)
    time.sleep(IO_SECONDS)
    return _cpu(doc)


async def arealistic(payload: str) -> int:
    doc = json.loads(payload)
    await asyncio.sleep(IO_SECONDS)
    return _cpu(doc)


def _free_threaded_build() -> bool:
    return bool(sysconfig.get_config_var("Py_GIL_DISABLED"))


def require_nogil() -> None:
    """Refuses to run thread-mode numbers with the GIL on (an import re-enabled it)."""
    if _free_threaded_build() and sys._is_gil_enabled():
        raise RuntimeError("the GIL was re-enabled by an import; mayu-thread numbers would be wrong")


if os.environ.get("BENCH_REQUIRE_NOGIL") == "1":
    require_nogil()
