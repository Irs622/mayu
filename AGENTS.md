# AGENTS.md

Guidance for coding agents (and humans) working on mayu. Read [CONTRIBUTING.md](CONTRIBUTING.md)
for setup and test commands, and the
[architecture spec](docs/specs/2026-10-11-mayu-architecture-design.md) before changing behaviour.

## What mayu is

A Redis-backed Python task queue. The runtime is Rust (`crates/`), the user-facing API is Python
(`python/mayu`), and workers run tasks on parallel OS threads under free-threaded CPython 3.14t,
falling back to process mode on GIL builds. Delivery is **at-least-once**.

## Invariants: don't break these

- **Ack after completion.** A task is acknowledged only after its outcome is recorded. Never ack
  early, never drop a delivery on an error path. A crash must mean "runs again", never "lost".
- **Atomic Redis transitions.** Every state change that touches more than one key (ack + result +
  link callbacks, retry, dead-letter, reclaim, replay) is a single Lua script in
  `crates/mayu-redis/src/lua/`. Don't split one into several round trips.
- **`mayu-core` has no PyO3 or Redis dependency.** Broker behaviour goes behind the `Broker` trait,
  and `MemoryBroker` must stay a faithful in-memory model of `RedisBroker` for tests.
- **Thread safety without the GIL.** Code in `python/mayu` runs on many threads at once with no GIL.
  Shared mutable state needs a lock or must be per-thread. Don't rely on the GIL making
  something atomic.
- **Release the interpreter for blocking Rust calls.** In `crates/mayu-py`, wrap anything that
  blocks or waits on I/O in `py.detach(...)`.
- **No fork.** The supervisor spawns child processes. Tokio and threads don't survive `fork`.
- **Wire format is stable.** The msgpack envelope (`envelope.rs`) and the Redis key layout are read
  by workers running other versions during a rolling deploy. Changes must stay
  backwards-compatible or be called out as breaking in `CHANGELOG.md`.
- **Reserved option names** (`countdown`, `eta`, `queue`, `link`) can't become task parameters.
- **New features are opt-in.** A worker started with existing flags and code behaves exactly as before:
  same scheduling, eviction, defaults, CLI output and metrics. Anything that would change existing behaviour
  (even "better" behaviour behind an existing flag) needs the maintainer's explicit OK before it is planned or
  built. Gate it behind the new option instead, and list any visible additive change in the plan.

## Code quality

- **Match the surrounding code**: naming, comment density, idioms. Comments explain *why*, not
  *what*. Docstrings are one line unless the behaviour is subtle.
- **Small, focused changes.** Don't reformat, rename or "tidy" code you aren't otherwise changing.
  The repo isn't uniformly `cargo fmt`/`ruff` clean yet, and a mass reformat buries the real diff.
  Format the lines you touch.
- **Rust:** return `mayu_core::Error` / `Result` instead of panicking. `unwrap()` is fine in tests
  and on poisoned-lock / truly-impossible paths only. Add no new `unsafe` without a `// SAFETY:`
  comment. New code should be clippy-clean (`cargo clippy --workspace --all-targets`).
- **Python:** keep the public API small and import it from `mayu/__init__.py`. Type-annotate new
  public functions. Lines stay at 120 characters or fewer. User errors raise clear `ValueError` /
  `TypeError` messages that say what to do.
- **Dependencies:** the runtime dependency list is `msgpack` only (plus the `otel` extra). Adding a
  Python or Rust dependency needs a reason in the PR.
- **Performance matters.** Hot paths are dispatch, fetch/ack and the Python call wrapper. Don't add
  per-task allocations, round trips or locks there without measuring (`bench/`).
- **Docs:** user-visible behaviour, CLI flags, env vars and metrics are documented in `README.md`.
  Update it in the same change. Add a line to `CHANGELOG.md` under *Unreleased*.

## Tests

- Every change ships with tests. A bug fix starts with a test that fails without the fix.
- Rust unit tests go next to the code. Broker behaviour is tested against both `MemoryBroker`
  (`crates/mayu-core/tests`)
  and Redis (`crates/mayu-redis/tests`).
- Python tests use the `app`, `queue` and `running` fixtures from `tests/python/conftest.py`. Each
  test gets its own random queue, so tests don't share state.
- Tests must pass under `ft-stress` (`PYTHON_GIL=0`, 4 threads per test). Tests that can't run
  concurrently use the thread-unsafe fixtures listed in `pyproject.toml`.
- Prefer waiting on a result or an event with a timeout over fixed sleeps, which make tests slow and flaky.
- Before saying a change is done, run `cargo test --workspace` and `scripts/matrix.sh ft py313`
  (Redis via `scripts/redis-up.sh`) and report the actual output.

## Commits and PRs

- Commit messages: `area: what changed` (`fix(redis): ...`, `perf(core): ...`, `docs: ...`).
- One logical change per PR. Don't commit `target/`, virtualenvs, `bench/results/` or local planning
  notes (`docs/superpowers/`, `.superpowers/`).
