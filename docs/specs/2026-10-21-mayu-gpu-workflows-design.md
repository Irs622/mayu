# mayu — GPU Workloads & Workflows Design

**Date:** 2026-10-21
**Status:** Draft
**Builds on:** [Architecture design](2026-10-11-mayu-architecture-design.md) (v0.1.0)

## 1. Intent

mayu v0.1 runs many small, independent tasks fast. This design extends it to pipelines with GPU stages:
multi-step jobs where some steps hold large models on a GPU, some steps are cheap enough to share a GPU,
and steps depend on each other.

### Driving example

A narrated explainer-video pipeline, per video:

```
plan (LLM agents) ─▶ encode prompts ─▶ voice lines ─▶ wire ─▶ render S1 ┐
                     (text encoder)    (TTS model)          ├▶ render S2 ├─▶ finalize (concat + master)
                                                            └▶ render S3 ┘
   network-bound      GPU, 5.8 GB       GPU, 6.9 GB    CPU   GPU, small, parallel   CPU (ffmpeg)
```

On an 8 GB card the encoder and the TTS model can't be resident together, so each video loads and frees
both. Scene renders each use a fraction of the GPU and can run side by side. Several videos are in flight
at once. Their stages should interleave, models should switch as rarely as possible, and CPU stages must
never wait on GPU capacity.

### Goals

1. Enqueue a task without importing its implementation (a web API must not import torch).
2. Run stages with different dependency stacks in separate envs, without lowering mayu's 3.13+ floor.
3. Admit tasks by declared resource use (`gpu`, `vram_gb`, `cpu`, user-defined), so tasks that each
   under-use a GPU run concurrently and tasks that don't fit wait.
4. Workflows: a DAG of tasks where a node is enqueued only when all of its parents succeeded.
5. Model pools for servers: N preloaded instances per worker, one per running task.
6. Resident models: load once per worker, keep loaded across tasks, evict by VRAM budget, and prefer
   tasks whose model is already loaded.

### Non-goals (this design)

- Moving artifacts. Tasks pass ids/URIs; storage is the application's (local disk, S3).
- Sharing one GPU between workers in different envs (needs a host-level coordinator; §9).
- Exactly-once, ordering guarantees (unchanged from v0.1).

## 2. Principles carried over

All v0.1 invariants hold: ack after completion, every multi-key transition is one Lua script, `mayu-core`
stays PyO3/Redis-free with `MemoryBroker` mirroring `RedisBroker`, the wire format stays backwards
compatible, and hot paths (dispatch, fetch/ack, the call wrapper) gain no per-task cost for tasks that
don't use the new features.

## 3. Declarations and implementations (Plan 8)

```python
# tasks.py: importable anywhere, no heavy imports
@app.declare(queue="gpu")
def voice_lines(out_dir: str) -> float: ...

# gpu_impl.py: imported only by GPU workers
@voice_lines.implement
def _(out_dir):
    import torch
    ...
```

- `app.declare(...)` takes the same options as `app.task(...)` and registers a `Task` whose body is never
  run. `send`, `delay`, `.s()` and `link=` work as for any task, with arguments validated against the
  declaration's signature.
- `Task.implement(fn)` attaches the code. Its parameters must match the declaration's (names, kinds,
  defaults); otherwise `TypeError`. Implementing twice, or implementing a task made with `app.task`, raises
  `ValueError`. `is_async` comes from the implementation.
- `app.send(name, *args, queue=None, countdown=None, eta=None, link=None, **kwargs)` enqueues by name with
  no import and no validation. Queue and retries come from the registry when the name is registered,
  else from the app defaults.
- Workers: with no `--queues`, a worker serves the queues of tasks implemented in its process (plus the
  default queue). Explicitly serving a queue that has declared-but-unimplemented tasks is a startup error
  naming the tasks. A delivery for an unimplemented task fails like an unknown task does today.

No wire change.

## 4. Environments (no plan: dropped)

mayu stays on 3.13+ and is built for 3.14t; it does not add 3.11/3.12 support. A stage with a heavy ML
stack runs in its own env with its own worker on its own queue, which already works:

- Orchestration and CPU stages: 3.14t, thread mode.
- GPU stages: whatever Python the ML stack supports among 3.13/3.14 GIL builds, run with `--mode thread`
  (one process owns the GPU; its tasks are GPU- or subprocess-bound, so the GIL costs little).

Stages that should share a GPU (and its ledger, §5) belong in the same GPU env. Checked for the driving
example: its TTS stack (torch 2.8) and renderer (manimgl) both install on 3.13; the orchestration stack is
GIL-free on 3.14t.

## 5. Resource admission (Plan 10)

```python
@app.task(queue="gpu", resources={"gpu": 0.3, "vram_gb": 1.2, "cpu": 2})
def render_scene(run: str, sid: str) -> str: ...
```
```bash
mayu worker app:app --queues gpu --mode thread --threads 6 --resources gpu=1,vram_gb=7.5,cpu=12 --gpus 0
```

- Requests are looked up by task name on the worker (like rate limits): no wire change.
- The Rust dispatcher keeps a per-worker ledger. A task starts only if every resource it requests fits in
  what remains. It is released when the outcome is recorded. A task that can never fit (asks for more than
  the worker has) fails with a clear error instead of waiting forever.
- Tasks that don't fit are held in the prefetch buffer, like rate-limited tasks; the visibility-extension
  loop already covers held tasks. Waiting tasks that fit may start ahead of one that doesn't (backfill),
  bounded by aging so a large task isn't starved (§7).
- `--gpus 0,1` runs one worker process per listed GPU with `CUDA_VISIBLE_DEVICES` set, via the
  supervisor's per-child arguments.
- One GPU is owned by one worker process; its tasks run on threads (GPU code and subprocesses release the
  GIL, so this works on GIL builds too). That keeps the ledger and loaded models in one process.
- The ledger is per worker. Several pools on one host split cores by configuration (`cpu=` per pool).
- Out-of-memory (`torch.OutOfMemoryError`, or a task raising `mayu.CapacityError`) requeues without
  spending a retry.

## 6. Workflows (Plan 11)

```python
wf = app.workflow(name="video:42")
plan = wf.add(plan_run.s(prompt, run))
voice = wf.add(voice_lines.s(run), after=[plan])
scenes = [wf.add(render_scene.s(run, s), after=[voice]) for s in ("S1", "S2", "S3")]
wf.add(finalize.s(run), after=scenes)
handle = wf.send()          # WorkflowHandle: status, per-node handles, cancel()
```

- `wf.send()` writes the DAG to Redis and enqueues the roots, in one Lua script.
- Release happens in the parent's ack script, extending today's `link=` follow-ups: it records the parent
  as done (`SADD`, so a duplicate run under at-least-once can't release children twice) and `XADD`s every
  child whose parents are all done.
- A parent that dead-letters leaves its descendants blocked; `mayu inspect` shows them. Replaying the
  parent resumes the workflow. `cancel()` marks the workflow cancelled; unreleased nodes are never
  enqueued and released ones are acked without running.
- Nodes don't receive parents' results in v1 (tasks share state through their arguments, e.g. a run id).
- Envelope: optional `workflow_id` and `node` header fields (`#[serde(default)]`). Old workers ignore them.
  Noted in the changelog.
- Supersedes "canvas" in the architecture spec's out-of-scope list; `link=` stays as the single-callback
  shortcut.

## 6.1 Task progress (Plan 16)

A step is a loop over items (prompts to encode, lines to voice, scenes to render), so the task knows its
position; mayu otherwise sees it only as running or finished, and a 400 s step reads the same from start to end.
The task reports it, mayu turns it into an estimate of the time left.

```python
@app.task(queue="gpu")
def voice_lines(lines, tts=app.use("tts")):
    for i, line in enumerate(lines):
        tts.voice(line)
        mayu.progress(i + 1, len(lines))      # or mayu.progress(0.4) for a fraction
```

- **API:** `mayu.progress(done, total=None)` records a count, or a fraction in `[0, 1]` without `total`, and returns
  whether it was recorded. Outside a running task it does nothing and returns `False`, so library code that
  also runs standalone can call it unconditionally; bad values raise `ValueError` / `TypeError` either way.
  `mayu.progress_reporter()` returns a callable bound to the current task for threads the task starts, since
  the current task is found through the thread. Progress may go backwards; the estimate restarts from there.
- **Finding the task:** the worker already runs each task on a slot thread, so it sets the slot's token around
  the call (one attribute write each way, no allocation) and `progress` reads it; async tasks carry it in a
  `ContextVar` set on submit. Process mode needs nothing, each child being a full worker. A task that never
  reports pays nothing.
- **Storage:** the in-flight entry of the Rust runtime holds the last report and the first one the current
  estimate is measured from, behind the lock the entry map already has. Reporting does no I/O and never
  blocks, and a report for a task that has finished is ignored.
- **Time left:** `(1 - f) / rate`, with the rate measured from the first report to the latest once they differ
  by 1 % and are 1 s apart, so setup before the loop (a model load) doesn't read as slow work. Before that,
  `elapsed * (1 - f) / f` from 5 % done; below that, unknown. A pure function in `mayu-core`.
- **Publishing:** no extra round trip. Each heartbeat writes the running tasks (id, task, queue, workflow and
  node, start time, and the progress fields when reported; at most 64, oldest first) into a `tasks` field of
  the worker's hash as msgpack in hex text, and deletes the field when none runs. It is additive: readers
  ignore unknown fields, and an unreadable `tasks` reads as empty and counts as a `progress` broker error. So
  progress is up to one heartbeat interval old.
- **Reading:** `TaskHandle.progress()` scans the live workers' hashes for the task id (an admin and UI call, not
  for tight loops), and `mayu inspect` lists running tasks with progress and time left, in text and `--json`. The
  only metric is a counter of reports per queue and task; a gauge per task would multiply label values with
  every task id.
- **Lookahead (§7.2):** with `--lookahead`, a running node's remaining time in the next-need estimate is its
  progress estimate when it has one, else what is left of the average run time. Local tasks come from the
  in-flight entries, tasks on other workers from their heartbeats, read at most once per heartbeat interval and
  only while a workflow has a queued node that isn't running here. The staging decision and
  `mayu_pool_next_need_seconds` follow, so the voice model is staged by the encode loop's real position, not
  by a guess.
- **Opt-in:** a task that doesn't call `progress` and a worker without `--lookahead` and `parked=` behave and
  export as before, apart from the heartbeat field and a `RUNNING` section in `mayu inspect` while a task runs.
- **Not in scope:** free-text messages, pushing progress to the producer (it polls), keeping progress after the
  task ends, limits based on progress.

## 7. Model pools (Plan 12)

For servers: a fixed number of instances of an expensive object (a model, a loaded tensor set), loaded at
worker start and kept for the worker's life. Code says how to build one; the operator says how many.

```python
@app.pool("gemma", vram_gb=5.8)            # per instance; optional, any declared resource
def gemma(ctx):                            # ctx.index in range(ctx.size): pick a device, a shard...
    return load_gemma(device=f"cuda:{ctx.index % torch.cuda.device_count()}")

@app.pool("tts", vram_gb=6.9)
def tts(ctx):
    return load_tts()

@encode_prompts.implement
def _(run, model=app.use("gemma")): ...    # one instance, exclusively, for the task's duration
```
```bash
mayu worker app:app --queues gpu --mode thread --threads 8 \
    --pools gemma=2,tts=3 --resources gpu=1,vram_gb=40
```

- **Admission:** pool `p` of size N is a ledger resource `pool:p` with capacity N (§5); a task that uses it
  requests `pool:p=1` on top of its own `resources=`. So at most N such tasks run at once and checkout from
  the pool's free list never blocks. No new dispatcher logic.
- **Loading:** every instance is built before the worker fetches anything, one at a time (no VRAM spikes
  from parallel loads). A failed load fails startup. Instances that declare resources reserve them for the
  worker's life: `--pools gemma=2` with `vram_gb=5.8` takes 11.6 of the worker's `vram_gb`, and the worker
  refuses to start if the pools don't fit.
- **Validation:** a served task that uses a pool missing from `--pools` (or sized 0) is a startup error.
  Tasks on other queues are ignored, as with resources.
- **Thread safety:** each instance is used by one task at a time; the free list is a locked deque. A task
  never sees two instances unless it asks for two pools.
- **Broken instances:** a task that raises `mayu.DiscardInstance` (or any exception listed in the pool's
  `discard_on=`, e.g. CUDA errors) has its instance closed (`@gemma.close`) and rebuilt before the slot
  is handed out again; the task itself fails or retries as usual.
- **Metrics:** `mayu_pool_size{pool}`, `mayu_pool_busy{pool}`, `mayu_pool_rebuilds_total{pool}`.
- **Multi-GPU:** either one process per GPU (`--gpus`, pools sized per GPU) or one process with
  `ctx.index` placing instances across devices; the ledger is per process either way.

Pools are the static, server form of residency. §7.1 adds the dynamic form for small machines.

## 7.1 Resident models and scheduling (Plan 13)

The same `@app.pool` factories, sized lazily instead of preloaded, for machines that can't hold every
model at once (an 8 GB card can't hold both models of the driving example):

```bash
mayu worker app:app --pools gemma=1,tts=1 --pool-mode lazy --resources gpu=1,vram_gb=7.5
```

- A lazy pool builds an instance on first use and keeps it after the task ends.
- The worker keeps loaded instances within its `vram_gb` budget, evicting (via the pool's `close`)
  least-recently-used idle instances when a task needs a pool that isn't loaded. It tells the dispatcher
  what is loaded.
- When capacity frees, the dispatcher picks among ready tasks that fit by
  `score = resident_bonus + workflow_age + wait_aging`:
  - a task whose model is loaded beats one that would force a swap;
  - older workflows go first (each video finishes as early as possible);
  - wait aging bounds every preference, so a stream of same-model tasks can't starve others.
- The weights are a worker option (`--schedule throughput|oldest`): `throughput` lets the resident bonus
  dominate (fewest model swaps, for batch and single-user setups), `oldest` lets workflow age dominate
  (each workflow finishes as early as possible). Per-tenant fair share belongs with hosting (§9).
- Example: video A's encode runs; video B's agents finish. When A's encode ends, B's encode (encoder
  loaded) runs before A's voicing (needs a swap); then both videos are voiced on one TTS load.
- Optional `linger=` keeps an idle model loaded when the workflow DAG shows more of its tasks pending
  upstream. Off by default until load times and run times are measured.
- `cost=` (a callable on the task, evaluated on the worker at prefetch) gives a duration hint for
  longest-first ordering within a workflow. Only tasks that declare it pay for it.

Reordering only covers tasks the worker has prefetched; GPU queues should run with a small prefetch. If
that proves too narrow, priority lanes (several streams per queue) are the follow-up; that changes the
key layout.

## 7.2 Parked models and staging (Plan 15)

Eviction (§7.1) closes a model, so the next task that needs it loads it again from disk: seconds to tens of
seconds for a multi-GB model. If the machine has spare host RAM, a lazy pool can keep the evicted model there
and bring it back over PCIe in about a second. mayu holds the accounting; moving the model is the pool's code.

```python
@app.pool("voice", vram_gb=6.9, parked={"ram_gb": 6.6})   # what one parked instance holds
def voice(ctx): return load_voice(device="cuda")

@voice.park                         # GPU -> host RAM, the object is kept
def _(model): ...
@voice.unpark                       # host RAM -> GPU, in place
def _(model): ...
@voice.stage                        # optional: build straight into the parked state, no GPU
def _(ctx): ...
```
```bash
mayu worker app:app --queues gpu --pools encoder=1,voice=1 --pool-mode lazy --schedule throughput \
    --lookahead --resources gpu=1,vram_gb=7.5,ram_gb=16
```

- **Ledger:** a parked instance holds `parked=` instead of its normal claim, and is not counted as loaded, so
  `pool:<name>` still bounds GPU-resident instances while `loaded + parked` stays within the pool's size. Every
  resource in `parked=` must be declared on the worker, so host RAM has an explicit budget. Parks, unparks,
  drops and stages are committed in the ledger, under its one lock, before Python touches an instance, like
  loads and evictions.
- **Plan:** the dispatcher's plan for a task gains `parks`, `unparks` and `drops` beside `loads` and `evicts`,
  carried out in the order drops, evicts, parks, unparks, loads, so room exists before it is used. A missing model with a parked
  instance is unparked instead of loaded. A victim of a parkable pool is parked instead of evicted when its
  `parked=` claim fits, after closing other parked instances if need be (never one the task unparks); if it
  doesn't fit, the victim is evicted as in §7.1. A parked instance is also closed when a task, a park or an
  unpark needs a resource it holds.
- **Scoring:** a task whose missing models are all parked gets a quarter of the resident bonus
  (`PARKED_BONUS_RATIO`): a swap from RAM is worth grouping, less than keeping a model on the GPU.
- **Failures:** a park hook that raises closes the instance instead (an eviction); an unpark hook that raises
  closes it and the task builds a fresh one; a failed stage gives its room back and that pool is not staged
  again for 60 s. None of them delays or drops an outcome.
- **Progress estimate:** with `--lookahead`, the worker reads each workflow it has seen (the DAG's parent links
  are already in Redis since §6, so no wire change) and estimates, from the moving average of its own run time
  per task name, when each pool is next needed: a running node has what is left of its average, a queued one
  its average, a waiting one starts when its slowest unfinished parent ends. A task never run here counts as
  zero, which errs towards staging early, never late. Only workflows this worker has fetched a task of are
  known, as for `linger=` and lookahead. A running node that reports progress (§6.1) has its own estimate
  instead of its average.
- **Victim order:** with `--lookahead` and some pool declaring `parked=`, idle (or parked) instances are
  chosen as victims in order of when their pool is next needed, the furthest first and a pool nobody needs
  before any, ties by least recently used. Otherwise it is least recently used, so an existing worker evicts as
  it did.
- **Staging:** a pool with a `stage` hook is built into host RAM once its next need is within its lead time:
  the moving averages of its stage and unpark times plus a 5 s margin, or 15 s until a stage has been
  measured. It also needs a workflow task to come, no idle, loading or parked instance, room below its size,
  and the `parked=` amount free without closing anything. Unlike a lookahead load, a stage uses no GPU
  resource, so a busy worker stages: the voice model is built in RAM while the encodes run, and is unparked
  when the GPU frees. Lookahead's usual rule then moves a parked model to the GPU when nothing is due.
- **Opt-in:** a worker whose pools don't declare `parked=` plans, evicts, logs and exports metrics exactly as
  before; the estimate is not computed without `--lookahead`.
- **Metrics:** `mayu_pool_parked`, `mayu_pool_parks_total`, `mayu_pool_unparks_total`, `mayu_pool_drops_total`,
  `mayu_pool_stages_total`, `mayu_pool_move_seconds{kind}` (build, stage, park, unpark) and
  `mayu_pool_next_need_seconds`, all per pool and only for pools with `parked=`.
- **Not in scope:** parking to disk, sharing a parked copy between worker processes, routing tasks across
  workers by parked state, `cost=`-based durations.

mayu doesn't know about any framework: with torch, the hooks keep a host mirror of the model's CUDA tensors in
one registered slab (the README has the recipe). Measured on an 8 GB laptop card with 6 GB models: about 1 s
for the first park, 0.3 s for later ones, 0.5 s to unpark, against 1 to 5 s to reload from a warm page cache.

## 8. Plans

| # | Plan | Depends on | Needs measurement |
|---|---|---|---|
| 8 | Declarations, `implement`, send by name (done) | — | no |
| ~~9~~ | ~~Python 3.11 / 3.12~~: dropped, see §4 | | |
| 10 | Resource admission, `--resources`, `--gpus` (done; capacity requeue on OOM deferred) | — | defaults only |
| 11 | Workflows (DAG) (done) | — | no |
| 12 | Model pools (`@app.pool`, `--pools`, `app.use`), server form of residency | 10 | no |
| 13 | Resident models with eviction, residency-aware scoring, `cost=` | 11, 12 | yes: load times, VRAM |
| 14 | Self-tuning concurrency (NVML, optional `gpu` extra), GPU metrics, linger | 13 | yes |
| 15 | Parked models (host-RAM tier for lazy pools), progress-aware staging | 13, 14 | yes: park, unpark and stage times, host RAM |
| 16 | Task progress (`mayu.progress`), time-left estimates, progress-aware lookahead | 11, 15 (lookahead part) | yes: estimates against actual finish times |
| later | Capacity requeue on GPU OOM without spending a retry | 10 | no |

Before Plan 13: one instrumented run of the driving example on the target GPU, recording model load
times, peak VRAM per stage, and GPU/CPU utilization with 1 vs 3 parallel renders.

## 9. Later: hosting

When pipelines like the driving example are hosted for many users on several GPU machines:

- **Tenant fairness:** a `tenant=` key on workflows; fair share or priority tiers per tenant, generalizing
  oldest-workflow-first.
- **Model-aware routing across workers:** workers advertise loaded models in their heartbeat; tasks prefer
  workers that have their model. On large GPUs (24 GB+) models co-reside, so routing matters more than
  eviction.
- **Autoscaling signals:** queue depth and wait age per model/resource for KEDA or a pool manager, with
  `min_workers` to keep warm capacity.
- **Host-level coordinator:** one ledger per host shared by pools and envs, so different envs can share a
  GPU and CPU budgets needn't be split by hand.
- **Progress and cancellation** surfaced per workflow for end users.
- **Per-task envs** (`app.env(...)`, workers spawned with another interpreter), once the coordinator exists.

Artifact storage (object store instead of local paths) stays in the application.
