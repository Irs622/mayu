# mayu

[![PyPI](https://img.shields.io/pypi/v/mayu.svg)](https://pypi.org/project/mayu/)
[![Python versions](https://img.shields.io/pypi/pyversions/mayu.svg)](https://pypi.org/project/mayu/)
[![CI](https://github.com/Siddid-Soni/mayu/actions/workflows/ci.yml/badge.svg)](https://github.com/Siddid-Soni/mayu/actions/workflows/ci.yml)
[![License: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)

**mayu is a fast, Redis-backed Python task queue with a Rust core, built for free-threaded
(no-GIL) Python.** Its workers run tasks on real parallel OS threads under CPython 3.14t, so a
few processes can do the work that Celery needs dozens of forked processes for.

> Status: alpha. Delivery is at-least-once: **tasks must be idempotent.**

- **Fast:** about 31× Celery's no-op throughput and 5× on a realistic task, with an eighth of the
  memory ([benchmarks](https://github.com/Siddid-Soni/mayu/blob/main/bench/RESULTS.md)).
- **Free-threaded first:** thread mode on Python 3.14t, with automatic fallback to process mode on
  GIL builds (3.13, 3.14).
- **Reliable:** acks after the task finishes, crash recovery, poison-task detection, dead letters
  and replay, and it survives Redis restarts.
- **Familiar API:** `@app.task`, `.delay()`, `.send()`, retries, countdown/ETA, rate limits, time
  limits, queue priority, `link=` callbacks, sync and `async def` tasks.
- **Operable:** Prometheus metrics, JSON logs, OpenTelemetry tracing, and an `inspect` CLI.

## Install

```bash
pip install mayu            # or: uv add mayu
pip install "mayu[otel]"    # with OpenTelemetry tracing
```

Requires Python 3.13+ and Redis. Use a free-threaded build (`python3.14t`, e.g.
`uv venv --python 3.14t`) to get thread mode.

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

## Resources

```python
@app.task(resources={"gpu": 0.3, "vram_gb": 1.2})
def embed(texts): ...
```

```bash
mayu worker tasks:app --resources gpu=1,vram_gb=7.5     # or MAYU_RESOURCES
mayu worker tasks:app --gpus 0,1 --resources gpu=1      # one worker process per GPU
```

A worker starts a task only when everything it requests fits in what is left of what the worker
declared, so three of the tasks above run side by side on one GPU and a fourth waits for a
finisher. Amounts are in whatever unit you pick (GPUs, GB, cores), down to 0.001. A worker
refuses to start if a task on a queue it serves requests a resource it didn't declare, or more
than it has; tasks on other queues are ignored. Tasks without `resources=` are never held back.

- **No starvation:** if the oldest, highest-priority waiting task has been due for 5 seconds,
  later tasks may start only if they request none of the resources it is waiting for.
- **Per process:** each worker process counts only its own tasks. Several processes sharing a
  GPU each think they own it, so run one process per GPU and size `--resources` for that
  process. `--gpus 0,1` does this: it starts one process per id with `CUDA_VISIBLE_DEVICES`
  set to it, before your app is imported (ids are opaque, so `GPU-abc...` UUIDs work).
  `--processes`, if given, must equal the number of ids.
- **Zombies:** resources are released when a task's outcome is reported. A task past its hard
  time limit is reported as failed while its thread keeps running, so until the worker
  recycles, a zombie can use a GPU that a new task has already been given.

## Model pools

```python
@app.pool("gemma", vram_gb=5.8)             # resources are per instance
def gemma(ctx):                             # ctx.name, ctx.index in range(ctx.size), ctx.size
    return load_gemma(device="cuda")

@gemma.close
def _(model):
    model.unload()

@app.task(queue="gpu")
def encode(text, model=app.use("gemma")): ...   # model is one instance, yours alone until the task ends
```

```bash
mayu worker tasks:app --queues gpu --pools gemma=2,tts=3 --resources gpu=1,vram_gb=40   # or MAYU_POOLS
```

Code says how to build an instance; the operator says how many. A worker builds every instance
before it fetches anything, one at a time and logging each load time, and keeps them for its life.
At most N tasks that use a pool of N run at once, so a task always finds a free instance and never
waits inside the task body: it waits in the worker's queue, like any task held back by
`resources=`. A task gets its instances before its time limits start and gives them back before
its outcome is reported.

- **Reservations:** a pool's resources are reserved for the worker's life, N × the amount, and
  come off what `--resources` leaves for tasks. `gemma=2` above holds 11.6 GB of `vram_gb`; a
  task asking for more than the remaining 28.4 makes the worker refuse to start. Resource names
  starting with `pool:` are reserved.
- **Startup checks:** a worker refuses to start if a task it serves uses a pool that the process
  doesn't register with `@app.pool` or that `--pools` doesn't size, if `--pools` names an
  unregistered pool, or if the reservations exceed what it declares. A pool sized but unused by the
  queues it serves is not loaded (a warning is logged). If a factory raises while loading, the
  instances built so far are closed and the worker exits.
- **Broken instances:** raise `DiscardInstance` (`DiscardInstance(pool="gemma")` to discard only
  that pool's instance, otherwise all the task holds), or list exception classes in
  `@app.pool(..., discard_on=CudaError)`. The task still fails or retries by its usual rules
  (`retry_on=(DiscardInstance,)` retries it on another instance). After the outcome is reported,
  the instance is closed and a replacement built with the same `ctx.index`; the pool runs one
  instance short meanwhile. If the rebuild fails, the pool stays smaller and
  `mayu_pool_size` shows it. A pool with no instances left makes the worker recycle.
- **Close:** `@pool.close` runs on discarded instances and, at shutdown, on every instance that is
  back in the pool. Instances still held by threads that outlive the shutdown grace period are not
  closed. Errors in `close` are logged and ignored.
- **Zombies:** a task past its hard time limit is reported as failed while its thread keeps its
  instance, so the pool shrinks by one for good: the instance is never reused or closed, because
  the thread may still be using it. Recycling brings the pool back to full size.
- **Per process:** each worker process loads its own instances, in process mode and with `--gpus`
  too, so size pools for one process or one GPU. A recycle restarts the process and reloads every
  instance, so recycling a worker with big models costs their load time.
- **Sync tasks only:** an `async def` task can't use a pool (`TypeError` at registration). A task
  can use each pool in only one parameter, and the parameter must be usable by keyword.
- **Producers need no pool code:** `app.use("gemma")` is only a name, resolved when a worker
  starts, so a producer can declare the task (see below) without torch or `@app.pool`. Passing
  the pool's parameter to `send()`, `wf.add()` or a `link=` raises `TypeError`. A payload that
  carries it anyway, only possible with `app.send` by name, has its keyword value overwritten by
  the instance, and a positional one fails the task with a duplicate-argument `TypeError`.

### Lazy pools (models that don't fit together)

Eager pools (the default, described above) load everything at startup and keep it. If the models
don't fit in memory together, run the worker in lazy mode: it loads a model when a task needs it and
closes idle ones to make room.

```bash
mayu worker tasks:app --queues gpu --pools gemma=1,tts=1 --pool-mode lazy --schedule throughput \
    --preload gemma --resources gpu=1,vram_gb=7.5 --prefetch-multiplier 1
# or MAYU_POOL_MODE=lazy, MAYU_SCHEDULE, MAYU_PRELOAD
```

Here `gemma` and `tts` don't fit in 7.5 GB together, so only one is in memory at a time. The pool
code and the tasks are the same as for eager pools; `--pools gemma=1` is now the most instances
loaded at once, and nothing is reserved for it at startup. `--pool-mode` applies to every pool of
the worker.

- **Loading and eviction:** a pool's resources are held only while an instance is loaded. A task
  that finds no idle instance of its pool has the worker build one, after closing idle instances
  (least recently used first) until it fits. Only idle instances are closed, never one a task is
  using. A task without a pool may close an idle model too, if it has no room beside it. Loading and
  closing happen before the task's time limits start.
- **`--preload gemma,tts`:** load one instance of each at startup, in that order. They must fit
  together, or the worker refuses to start. Without it the first task for a model pays the load.
- **`--schedule`:** a worker holds back tasks that don't fit, and lazy mode lets it prefer ones whose
  model is already loaded. Queue priority still comes first; this only orders tasks within a
  priority. `throughput` (fewest swaps) lets a task with a loaded model run ahead of other work that
  arrived up to 60 seconds before it. `oldest` (the default in lazy mode) orders by the start of the
  workflow, and by arrival for tasks outside one, so a workflow already under way finishes first; a
  loaded model is worth 10 seconds, about one model load. A task that needs a swap is passed only by
  loaded-model tasks that arrived (or started) less than that bound after it, so none waits forever.
  The bounds are not configurable.
- **Only prefetched tasks are reordered:** the worker chooses among tasks it has already claimed
  from Redis. Run GPU queues with a small `--prefetch-multiplier` (the default is 2): a larger
  buffer gives it more to choose from but claims more tasks before they can run.
- **Startup checks:** `--schedule` and `--preload` need `--pool-mode lazy`. A worker refuses to start
  if a task's own resources plus one instance of each pool it uses exceed what `--resources`
  declares, or if the preloaded pools don't fit together.
- **Broken instances:** `DiscardInstance` and `discard_on` close the instance after the outcome is
  reported, as with eager pools, but nothing is rebuilt: the next task that needs the model loads it.
- **Failed loads:** if a factory raises while loading for a task, that task fails with
  `RuntimeError("pool 'gemma' failed to load: ...")`, so `retry_on` applies, and the worker keeps
  running. Instances closed to make room for it stay closed. A failing `--preload` stops the worker
  at startup, as in eager mode.
- **Zombies:** as with eager pools, a task past its hard time limit keeps its instance, so the pool
  shrinks by one for good. Its memory also stays counted as in use until the worker recycles.
- **Close:** `@pool.close` runs on evicted and discarded instances and at shutdown, as above.

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
and do not chain further; for a DAG of tasks see [Workflows](#workflows).

## Workflows

A DAG of tasks, submitted as one unit. A node runs once all its parents have succeeded.

```python
wf = app.workflow(name="video:42")
plan = wf.add(plan_run.s(prompt, run))
voice = wf.add(voice_lines.s(run), after=[plan])
scenes = [wf.add(render_scene.s(run, s), after=[voice]) for s in ("S1", "S2", "S3")]
wf.add(finalize.s(run), after=scenes)
handle = wf.send()            # WorkflowHandle
handle.status()               # {"state": "running", "waiting": 4, "queued": 1, "done": 1, "dead": 0, ...}
handle.nodes[-1].result(60)   # a TaskHandle per node, in add order
handle.wait(timeout=600)      # "done", "failed", "cancelled" or "expired"
handle.cancel()
```

`wf.add` takes a task or `task.s(...)` with exactly the arguments the task needs; `after=` takes a node
or a list of nodes. `send()` writes the whole DAG and enqueues the nodes with no parents in one atomic
step. When a node succeeds, the step that acks it also enqueues each child whose parents are now all
done, so each child is released exactly once. Nodes still run at least once, like any task.

- Nodes do not receive their parents' results; pass data through your own storage. Nodes can't use
  `link=`, `countdown` or `eta`.
- A node that fails for good (dead-lettered) blocks its descendants and the workflow reports `failed`.
  `mayu dead replay` the node and, once it succeeds, the rest of the DAG continues.
- `handle.cancel()` stops waiting nodes from being released and skips nodes that are queued but not yet
  started (they are acked with a "workflow cancelled" failure). A node already running finishes.
- `app.workflow(name, ttl=7 * 86400)` sets how long Redis keeps the workflow after its last progress.
  If it expires, running nodes still finish but release nothing.
- Release computes stream keys at run time, so workflows, like acks with results, are not
  Redis-Cluster-safe.
- Upgrade workers before submitting workflows: an older worker acks a node but never releases its children.

## Declaring tasks without their code

A producer often can't import a task's dependencies (a GPU model, a heavy library). Declare the task
there with only its name, options and signature, and attach the code in the worker:

```python
# shared module, imported by producers and workers
@app.declare(name="ml.embed", queue="gpu")
def embed(text: str, *, normalize: bool = True): ...

# worker module, imported only by workers that serve the "gpu" queue
@embed.implement
def _(text: str, *, normalize: bool = True):
    return model.encode(text, normalize=normalize)

embed.send("hello")                     # producer: arguments are checked against the declaration
app.send("ml.embed", "hello")           # by name: checked if registered here, sent as-is if not
```

`implement` must match the declaration's parameter names, kinds and defaults (annotations are ignored)
and returns the task. Calling a declared task that isn't implemented raises `TypeError`.

A worker without `--queues` serves only queues where it implements every task (the default queue
included), so it never takes work meant for another worker. With `--queues`, naming a queue that has
an unimplemented task is an error (`mayu: queue 'gpu' has tasks declared but not
implemented in this process: ...`, exit 2). If a delivery for an unimplemented task still arrives, it
fails with "not implemented" instead of running the stub.

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

## Memory and huge pages

On Linux with transparent huge pages set to `always` (the default on Arch, Fedora and RHEL), a
free-threaded worker can use about 4x the memory it needs: every thread's slice of CPython's
object heap is backed by a 2 MB huge page. Measured with 2 processes × 32 threads, that was
525 MB PSS against 135 MB with huge pages off, at the same throughput. Process mode on a GIL
build is not affected.

```bash
mayu worker tasks:app --thp python    # default; or MAYU_THP=python
```

- `python` (default): on a free-threaded build, opt only Python's own object heap out of huge
  pages, before your app is imported. Memory your tasks allocate themselves (numpy, torch and
  other `malloc`/`mmap` buffers) keeps the system setting.
- `keep`: change nothing.
- `off`: disable huge pages for the whole process, which child processes inherit, including
  tasks that start subprocesses and, under the supervisor, the worker processes.

`WorkerRunner(..., thp="keep")` takes the same modes. A failed system call is logged as a
warning and the worker starts anyway. Linux only; elsewhere every mode does nothing. Limitation:
CPython reserves further heap arenas only after about 1 GB of live Python objects, and those
keep the system setting.

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
| `mayu_tasks_settled_total` | counter | `queue`, `task`, `outcome` (`success`, `retry`, `dead`, `cancelled`) |
| `mayu_task_duration_seconds` | histogram | `queue`, `task` |
| `mayu_tasks_deferred_total` | counter | `queue`, `task` (rate-limited tasks sent back to Redis) |
| `mayu_tasks_reclaimed_total` | counter | `queue` (taken over from a dead worker) |
| `mayu_broker_errors_total` | counter | `op` |
| `mayu_worker_concurrency`, `mayu_tasks_inflight`, `mayu_tasks_running` | gauge | |
| `mayu_queue_ready`, `mayu_queue_pending`, `mayu_queue_delayed`, `mayu_queue_dead` | gauge | `queue` |
| `mayu_resource_capacity`, `mayu_resource_in_use` | gauge | `resource` (only with `--resources`) |
| `mayu_pool_size`, `mayu_pool_busy` | gauge | `pool` (only with `--pools`; instances in the pool, and held by running tasks) |
| `mayu_pool_rebuilds_total` | counter | `pool` (instances replaced after `DiscardInstance` or `discard_on`) |
| `mayu_pool_loaded` | gauge | `pool` (only with `--pool-mode lazy`; instances in memory, idle or busy) |
| `mayu_pool_loads_total`, `mayu_pool_evictions_total` | counter | `pool` (lazy instances built, preloads included, and closed to make room) |

Pools are counted in the resource ledger as `pool:<name>`, but they appear only as `mayu_pool_*`, never under
`mayu_resource_*`.

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
mayu inspect tasks:app                      # queue depths (ready, claimed, delayed, dead), live workers, unfinished workflows
mayu inspect tasks:app --json
mayu dead list tasks:app --queue emails     # oldest first: when, task id, task, attempt, error
mayu dead replay tasks:app --queue emails   # run them again
mayu dead replay tasks:app --queue emails --id 01J... --limit 10
```

The workflows table lists those not yet done (id, name, state, node counts, age); finished ones are left
out and expire on their `ttl`.

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

## Benchmarks

mayu vs Celery, RQ, Dramatiq and streaQ at 64 concurrent tasks: no-op throughput, a realistic task
(JSON decode + 5 ms I/O + 1 ms CPU), memory at equal concurrency, and workers on Python 3.14t, 3.14 and
3.13 serving the same queues at once. Results and method:
[`bench/RESULTS.md`](https://github.com/Siddid-Soni/mayu/blob/main/bench/RESULTS.md). Reproduce with `bench/setup.sh && bench/all.sh` (needs Docker and uv).

## Development

See [CONTRIBUTING.md](https://github.com/Siddid-Soni/mayu/blob/main/CONTRIBUTING.md) for setup, the test matrix and the pull request checklist.
Coding agents should also read [AGENTS.md](https://github.com/Siddid-Soni/mayu/blob/main/AGENTS.md). The design is in
[`docs/specs/2026-10-11-mayu-architecture-design.md`](https://github.com/Siddid-Soni/mayu/blob/main/docs/specs/2026-10-11-mayu-architecture-design.md).

## License

Licensed under either of [Apache License, Version 2.0](https://github.com/Siddid-Soni/mayu/blob/main/LICENSE-APACHE) or [MIT license](https://github.com/Siddid-Soni/mayu/blob/main/LICENSE-MIT)
at your option. Unless you explicitly state otherwise, any contribution intentionally submitted for
inclusion in mayu by you, as defined in the Apache-2.0 license, shall be dual licensed as above,
without any additional terms or conditions.
