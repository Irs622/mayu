# Changelog

All notable changes to mayu are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses
[Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- `app.declare` / `task.implement` and `app.send(name, ...)` to enqueue tasks without importing their code; workers serve only queues whose tasks they implement.
- Resource admission: `resources={"gpu": 0.3}` on tasks, `--resources` on workers, `--gpus` for one worker process per GPU, and `mayu_resource_capacity` / `mayu_resource_in_use` gauges.
- Workflows: `app.workflow()` builds a DAG of tasks (`wf.add(sig, after=[...])`) that `send()` submits atomically; each node's ack releases the children whose parents are all done. `WorkflowHandle` has `status()`, `wait()` and `cancel()`, a dead-lettered node blocks its descendants until `mayu dead replay`, `mayu inspect` lists unfinished workflows, and skipped nodes count as `outcome="cancelled"` in `mayu_tasks_settled_total`.
- Model pools: `@app.pool("gemma", vram_gb=5.8)` builds a fixed number of instances of an expensive object once per worker (`--pools gemma=2` / `MAYU_POOLS`), tasks borrow one with `model=app.use("gemma")`, and `DiscardInstance` / `discard_on` replace broken ones. Adds `mayu_pool_size`, `mayu_pool_busy` and `mayu_pool_rebuilds_total`; resource names starting with `pool:` are reserved.
- Lazy model pools: `--pool-mode lazy` loads a model when a task needs it and closes idle ones (least recently used) to make room, for models that don't fit in memory together. `--preload` loads pools at startup, `--schedule throughput|oldest` prefers tasks whose model is loaded (bounded to 60 s / 10 s, queue priority first), and `MAYU_POOL_MODE`, `MAYU_SCHEDULE` and `MAYU_PRELOAD` set the defaults. Adds `mayu_pool_loaded`, `mayu_pool_loads_total` and `mayu_pool_evictions_total`. Eager mode is unchanged.

### Changed

- `Signature` arguments are checked when used (at `send()` for `link=`, at `wf.add()` for workflows), not in `task.s()`.
- The envelope header gains optional `workflow_id` and `node` fields. Old workers ignore them but can't release workflow children: upgrade workers before submitting workflows.
- Free-threaded workers no longer use about 4x the memory on Linux when transparent huge pages are `always`: `--thp python` (default, `MAYU_THP`) opts only Python's own heap out of huge pages, `keep` changes nothing and `off` disables them for the whole process. Measured at 2 × 32 threads: 525 MB down to 135 MB PSS, same throughput.

## [0.1.0] - 2026-10-12

First public release.

- Redis broker with at-least-once delivery, crash recovery, poison-task detection, dead letters and replay.
- Thread mode for free-threaded CPython 3.14t and process mode for GIL builds, with a supervising parent.
- Retries with backoff, countdown/ETA, soft and hard time limits, rate limits, queue priority and `link=` callbacks.
- Sync and `async def` tasks.
- Prometheus metrics, JSON logging, OpenTelemetry tracing and the `mayu inspect` / `mayu dead` CLI.
- Benchmarks against Celery, RQ, Dramatiq and streaQ.

[Unreleased]: https://github.com/Siddid-Soni/mayu/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/Siddid-Soni/mayu/releases/tag/v0.1.0
