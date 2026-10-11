# mayu

A Python task queue whose runtime is written in Rust and whose workers run tasks
on real parallel OS threads under free-threaded CPython (3.14t+).

> Status: early development. Delivery is at-least-once: **tasks must be idempotent.**

## Quickstart

```python
# tasks.py
from mayu import App, Retry

app = App(broker="redis://localhost:6379/0", result_ttl=3600)

@app.task(max_retries=5, retry_on=(TimeoutError,))
def add(a: int, b: int) -> int:
    return a + b

@app.task
async def fetch(url: str) -> str:
    ...
```

```bash
mayu worker tasks:app --threads 32
```

```python
from tasks import add
add.delay(2, 3)
h = add.send(2, 3, countdown=10)
h.result(timeout=30)   # 5
```

Configuration comes from constructor arguments, overridden by `MAYU_BROKER`,
`MAYU_RESULT_TTL` (`0` disables results), `MAYU_DEFAULT_QUEUE`, `MAYU_SERIALIZER`.
`countdown`, `eta` and `queue` are reserved option names and cannot be task parameters.

## Reliability

mayu delivers **at least once**: a task is acknowledged only after it finishes, so a crash
means the task runs again, never that it is lost. **Write idempotent tasks.**

- Each worker heartbeats to `mayu:workers:{worker_id}` (TTL = 3 × `--heartbeat-interval`).
- A worker keeps ownership of tasks it is running by re-claiming them every half
  `--visibility-timeout` (default 30 s).
- When a worker dies (`kill -9`, OOM, segfault), the other workers reclaim its tasks once they
  have been silent for `--visibility-timeout`.
- A task whose worker dies `--max-deliveries` times in a row (default 5) is treated as poison
  and dead-lettered instead of being retried forever.
- Redis restarts are survived: connections reconnect, and outcomes produced while Redis is
  down are recorded when it returns. Use Redis persistence (AOF) if queued tasks must survive
  a Redis restart.
- Python threads cannot be killed: set timeouts on network clients inside tasks.

Chaos test (needs Docker): `.venv/bin/pytest tests/chaos -v`
(`MAYU_CHAOS_N` / `MAYU_CHAOS_SECONDS` scale it).

## Development

```bash
scripts/redis-up.sh                                   # Redis on :6390 for tests
cargo test --workspace                                # Rust tests (need Redis)
uv venv --python 3.14t .venv
uv pip install --python .venv/bin/python maturin pytest msgpack
VIRTUAL_ENV=$PWD/.venv .venv/bin/maturin develop --uv
.venv/bin/pytest
```

Design: `docs/specs/2026-10-11-mayu-architecture-design.md`.
