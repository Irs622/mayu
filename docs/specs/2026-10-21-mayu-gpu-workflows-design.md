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
5. Resident models: load once per worker, keep loaded across tasks, evict by VRAM budget, and prefer
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

## 7. Resident models and scheduling (Plan 12)

```python
@app.resource(model="tts", vram_gb=6.9, exclusive=True)
def tts():
    return load_tts()

@tts.close
def _(server):
    unload(server)

@voice_lines.implement
def _(run, server=app.use(tts)): ...
```

- A resource loads lazily (or at startup with `eager=True`) once per worker process, under a lock.
  `exclusive=True` serializes tasks using it; otherwise it is shared by concurrent tasks.
- The worker keeps loaded models within its `vram_gb` budget, evicting least-recently-used models that no
  running task holds. It tells the dispatcher what is loaded.
- When capacity frees, the dispatcher picks among ready tasks that fit by
  `score = resident_bonus + workflow_age + wait_aging`:
  - a task whose model is loaded beats one that would force a swap;
  - older workflows go first (each video finishes as early as possible);
  - wait aging bounds every preference, so a stream of same-model tasks can't starve others.
- Example: video A's encode runs; video B's agents finish. When A's encode ends, B's encode (encoder
  loaded) runs before A's voicing (needs a swap); then both videos are voiced on one TTS load.
- Optional `linger=` keeps an idle model loaded when the workflow DAG shows more of its tasks pending
  upstream. Off by default until load times and run times are measured.
- `cost=` (a callable on the task, evaluated on the worker at prefetch) gives a duration hint for
  longest-first ordering within a workflow. Only tasks that declare it pay for it.

Reordering only covers tasks the worker has prefetched; GPU queues should run with a small prefetch. If
that proves too narrow, priority lanes (several streams per queue) are the follow-up; that changes the
key layout.

## 8. Plans

| # | Plan | Depends on | Needs measurement |
|---|---|---|---|
| 8 | Declarations, `implement`, send by name (done) | — | no |
| ~~9~~ | ~~Python 3.11 / 3.12~~: dropped, see §4 | | |
| 10 | Resource admission, `--resources`, `--gpus` (done; capacity requeue on OOM deferred) | — | defaults only |
| 11 | Workflows (DAG) | — | no |
| 12 | Resident models, residency-aware scoring, `cost=` | 10, 11 | yes: load times, VRAM |
| 13 | Self-tuning concurrency (NVML, optional `gpu` extra), GPU metrics, linger | 12 | yes |

Before Plan 12: one instrumented run of the driving example on the target GPU, recording model load
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
