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
`countdown`, `eta`, `queue` and `link` are reserved option names and cannot be task parameters.

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
- On Python 3.13 a soft limit that lands in a `while` loop with an empty body can escape the
  task's `try`/`except` (a CPython 3.13 bug, fixed in 3.14). The task then fails with
  `SoftTimeLimitExceeded` without running its cleanup.
- **Hard limit:** Python threads cannot be killed. The task is recorded as failed with
  `mayu.TimeLimitExceeded` (retried if it is in `retry_on`), its thread is abandoned as a
  *zombie*, and a replacement thread takes over. Whatever the zombie produces later is discarded.
- Async tasks are cancelled at their limit instead.
- `@app.task(threadsafe=False)` runs a task on one dedicated thread, one call at a time, for
  legacy code that is not thread-safe.

The worker recycles itself (graceful stop, then a fresh interpreter in the same process) when
zombies exceed `--max-zombie-threads` (default 4), after `--max-tasks-per-process` tasks, or
above `--max-memory-per-process` MB of RSS.

## Rate limits

```python
@app.task(rate_limit="100/s")   # also "10/m", "5/h"; a bare number means per second
def call_api(): ...
```

Starts are spaced evenly (no bursts). The limit is **per worker process**: with
`--processes 4`, a `10/s` task can start 40 times a second across them. Tasks that would wait
more than a second for their turn go back to Redis until then, so a rate-limited backlog never
fills a worker's prefetch buffer or delays other tasks. A deferred task returns to Redis as a
fresh entry, so its crash-redelivery count (`--max-deliveries`) starts over. Deferred tasks are
spaced for the worker that deferred them, so adding workers doesn't speed up a backlog that is
already deferred.

## Queue priority

```bash
mayu worker tasks:app --queues high,default,low
```

The order of `--queues` is a strict priority: whenever a worker picks the next task to start,
it prefers `high` tasks over `default` ones. Tasks already claimed into its prefetch buffer, or
already running, are not preempted, and a constantly busy high queue still starves the ones
after it. Without `--queues`, a worker consumes every queue your app uses, at equal priority.

## Callbacks

```python
add.send(2, 3, link=notify.s("sum"))      # after add succeeds: notify(5, "sum")
add.send(2, 3, link=[audit, notify.s("sum")])
```

A `link=` callback runs only after its parent **succeeds**, with the parent's result as its first
argument (even when results are disabled). The callback is enqueued in the same atomic step
that acks the parent, so it is enqueued exactly once per successful parent. Like every task, it
can still run more than once if a worker crashes. Callbacks run on the callback task's queue
and do not chain further (no canvas).

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

## Metrics

```bash
mayu worker tasks:app --metrics-port 9100      # GET http://host:9100/metrics (Prometheus)
```

Each worker process serves its own metrics: with `--processes 4 --metrics-port 9100`, the
processes listen on 9100–9103, so scrape all four. `MAYU_METRICS_PORT` sets the default;
`--metrics-host` picks the interface (default `0.0.0.0`).

| Metric | Type | Labels |
|---|---|---|
| `mayu_tasks_received_total` | counter | `queue`, `task` |
| `mayu_tasks_settled_total` | counter | `queue`, `task`, `outcome` (`success`, `retry`, `dead`) |
| `mayu_task_duration_seconds` | histogram | `queue`, `task` |
| `mayu_tasks_deferred_total` | counter | `queue`, `task` (rate-limited tasks sent back to Redis) |
| `mayu_tasks_reclaimed_total` | counter | `queue` (taken over from a dead worker) |
| `mayu_broker_errors_total` | counter | `op` |
| `mayu_worker_concurrency`, `mayu_tasks_inflight`, `mayu_tasks_running` | gauge | |
| `mayu_queue_ready`, `mayu_queue_pending`, `mayu_queue_delayed`, `mayu_queue_dead` | gauge | `queue` |

Queue gauges are read from Redis on each scrape, so every worker reports the same values: use
`max by (queue) (mayu_queue_ready)`. If Redis cannot be read, the scrape still answers without them.

## Logging

```bash
mayu worker tasks:app --log-format json --log-level INFO
```

`text` (default) writes readable lines; `json` writes one object per line on stderr, from both the
Rust core and Python, with `timestamp`, `level`, `message`, `target` and event fields such as
`task_id`, `task`, `queue` and `error`. `MAYU_LOG_FORMAT` and `MAYU_LOG_LEVEL` set the defaults. Failed
tasks are logged at `WARN`, retries at `INFO`, successes at `DEBUG`. When you run `WorkerRunner`
from your own code, call `mayu.configure_logging("json")` first.

## Inspecting and dead letters

```bash
mayu inspect tasks:app                      # queue depths (ready, claimed, delayed, dead) and live workers
mayu inspect tasks:app --json
mayu dead list tasks:app --queue emails     # oldest first: when, task id, task, attempt, error
mayu dead replay tasks:app --queue emails   # run them again
mayu dead replay tasks:app --queue emails --id 01J... --limit 10
```

A replayed task keeps its id, starts again at attempt 0, and its stored failure is cleared, so
`handle.result()` waits for the new outcome. Each dead letter is moved back atomically, so
concurrent replays never enqueue a task twice. A replay only takes the dead letters that exist
when it starts: tasks that fail again during the replay stay dead. Entries whose envelope could
not be decoded are listed but never replayed.

## Tracing

```bash
pip install "mayu[otel]"
```

With OpenTelemetry installed and a tracer provider configured, `send()` creates a `send <task>`
producer span and stores its W3C `traceparent` in the task envelope. The worker runs the task inside a
`process <task>` consumer span that is its child, so spans your task creates (HTTP calls, database
queries) join the caller's trace. Retries, rate-limit deferrals and `link=` callbacks keep the original
trace context. A failed attempt marks its span as an error. Without OpenTelemetry installed, nothing
changes.

## Development

```bash
scripts/redis-up.sh                                   # Redis on :6390 for tests
cargo test --workspace                                # Rust tests (need Redis)
uv venv --python 3.14t .venv
uv pip install --python .venv/bin/python maturin pytest msgpack opentelemetry-sdk
VIRTUAL_ENV=$PWD/.venv .venv/bin/maturin develop --uv
.venv/bin/pytest
uv venv --python 3.14+gil .venv-gil && uv pip install --python .venv-gil/bin/python maturin pytest msgpack
VIRTUAL_ENV=$PWD/.venv-gil .venv-gil/bin/maturin develop --uv
scripts/matrix.sh                                     # CI matrix, Linux legs: ft (3.14t), gil (3.14), py313 (3.13)
scripts/matrix.sh py313                               # one leg
scripts/test-all.sh                                   # Rust + matrix + chaos
```

macOS (in the spec's CI matrix) is not covered by the local matrix.

Design: `docs/specs/2026-10-11-mayu-architecture-design.md`.
