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

## Time limits

```python
@app.task(soft_time_limit=30, hard_time_limit=60)
def crunch(): ...
```

- **Soft limit:** `mayu.SoftTimeLimitExceeded` is raised inside the task at its next Python
  bytecode, so it can be caught to clean up. It **cannot interrupt blocking C calls or socket
  reads**: set timeouts on network clients.
- **Hard limit:** Python threads cannot be killed. The task is recorded as failed with
  `mayu.TimeLimitExceeded` (retried if it is in `retry_on`), its thread is abandoned as a
  *zombie*, and a replacement thread takes over. Whatever the zombie produces later is discarded.
- Async tasks are cancelled at their limit instead.
- `@app.task(threadsafe=False)` runs a task on one dedicated thread, one call at a time, for
  legacy code that is not thread-safe.

The worker recycles itself (graceful stop, then a fresh interpreter in the same process) when
zombies exceed `--max-zombie-threads` (default 4), after `--max-tasks-per-process` tasks, or
above `--max-memory-per-process` MB of RSS.

## Modes

```bash
mayu worker tasks:app --threads 32 --processes 2      # thread mode: 2 processes × 32 threads
mayu worker tasks:app --mode process --processes 8    # process mode: 8 processes × 1 task each
```

`--mode auto` (default) uses thread mode when the GIL is off after importing your app, and
process mode otherwise, naming the module that re-enabled the GIL. With more than one process,
a supervising parent keeps the workers running, restarts any that crash (with backoff), and
shuts them all down gracefully on SIGTERM. In process mode, a task that exceeds its hard time
limit takes its process down with it, and the process is replaced.

Prefer **a few processes × many threads**: a segfault in a C extension takes down every task in
its process.

## Development

```bash
scripts/redis-up.sh                                   # Redis on :6390 for tests
cargo test --workspace                                # Rust tests (need Redis)
uv venv --python 3.14t .venv
uv pip install --python .venv/bin/python maturin pytest msgpack
VIRTUAL_ENV=$PWD/.venv .venv/bin/maturin develop --uv
.venv/bin/pytest
uv venv --python 3.14+gil .venv-gil && uv pip install --python .venv-gil/bin/python maturin pytest msgpack
VIRTUAL_ENV=$PWD/.venv-gil .venv-gil/bin/maturin develop --uv
scripts/test-all.sh                                   # everything, both builds
```

Design: `docs/specs/2026-10-11-mayu-architecture-design.md`.
