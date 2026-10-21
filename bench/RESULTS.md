# mayu benchmarks

12th Gen Intel(R) Core(TM) i9-12900H, 20 cores, Redis 8.10.2.

## Method

- Each run pre-fills the queue with N tasks, then starts the workers. Throughput is the steady rate between 5 % and
  95 % of completions, so worker startup and the drain tail are excluded. Wall time includes startup.
- Every system runs at concurrency 64 with its defaults; only results and retries are off. Delivery semantics
  differ: Celery acknowledges a task before running it by default, mayu after it finishes.
- mayu thread mode runs on free-threaded CPython 3.14t; everything else runs on CPython 3.14 with the GIL.
- mayu is a release build (`maturin develop --release`).
- Memory is the peak over the run of the whole worker process tree, sampled every 250 ms. PSS shares pages
  fairly between forked children; RSS counts shared pages once per process.
- RQ runs `SimpleWorker` processes. Its default worker forks a work horse per job and reconnects to Redis each
  time; at this rate that exhausts the ephemeral ports behind Docker's userland proxy and the run cannot finish.
- Each figure is the median of the repeated runs. Reproduce with `bench/setup.sh && bench/all.sh`.

## No-op task throughput

The task body is one Redis `INCR` (the completion counter every system shares).

| system | layout | tasks/s | wall s | peak PSS MB | peak RSS MB | vs Celery |
|---|---|---:|---:|---:|---:|---:|
| mayu-thread | 2 processes × 32 threads (Python 3.14.7t, 0.1.0) | 41231 | 0.7 | 187 | 223 | 31.28× |
| mayu-process | 64 processes (Python 3.14.8, 0.1.0) | 15067 | 2.2 | 1455 | 2441 | 11.43× |
| dramatiq | 2 processes × 32 threads (Python 3.14.8, 2.2.1) | 12393 | 1.8 | 80 | 138 | 9.40× |
| streaq | 2 processes × 32 async tasks (Python 3.14.8, 7.2.0) | 2876 | 7.4 | 112 | 166 | 2.18× |
| rq | 64 SimpleWorker processes, no fork per job (Python 3.14.8, 2.12.0) | 2397 | 8.5 | 538 | 2223 | 1.82× |
| celery | 64 prefork processes (Python 3.14.8, 5.6.3) | 1318 | 16.3 | 1595 | 3089 | 1.00× |

## Realistic task throughput

JSON-decode a 2.6 KB payload, sleep 5 ms (simulated I/O), about 1 ms of pure-Python CPU work, then the `INCR`.

| system | layout | tasks/s | wall s | peak PSS MB | peak RSS MB | vs Celery |
|---|---|---:|---:|---:|---:|---:|
| mayu-thread | 2 processes × 32 threads (Python 3.14.7t, 0.1.0) | 4657 | 1.2 | 189 | 225 | 5.21× |
| mayu-process | 64 processes (Python 3.14.8, 0.1.0) | 3197 | 2.3 | 1456 | 2441 | 3.58× |
| rq | 64 SimpleWorker processes, no fork per job (Python 3.14.8, 2.12.0) | 1715 | 3.1 | 537 | 2224 | 1.92× |
| dramatiq | 2 processes × 32 threads (Python 3.14.8, 2.2.1) | 943 | 5.5 | 81 | 138 | 1.06× |
| celery | 64 prefork processes (Python 3.14.8, 5.6.3) | 894 | 6.8 | 1612 | 3109 | 1.00× |
| streaq | 2 processes × 32 async tasks (Python 3.14.8, 7.2.0) | 858 | 6.3 | 115 | 170 | 0.96× |

## Memory at equal concurrency

Peak PSS during the realistic workload, 64 tasks in flight.

- mayu, 2 processes × 32 threads: **189 MB**
- Celery, 64 prefork processes: **1612 MB**
- Celery uses 8.5× the memory of mayu.

## Mixed Python versions

One 3.14t producer pre-fills the queue, then three worker groups start at the same moment: 3.14t in thread mode,
3.14 and 3.13 (both with the GIL) in process mode. Completions are read from each worker's `/metrics`. Groups on
a shared queue split the tasks by whichever reader is blocked when a task arrives, so the queue is pre-filled
to keep every group busy; the routed layout gives each group its own queue and an equal share of the tasks.

### Shared queue

- 3.14t thread: Python 3.14.7t, 1 process × 16 threads
- 3.14 process: Python 3.14.8, 16 processes
- 3.13 process: Python 3.13.15, 16 processes

| workload | 3.14t thread | 3.14 process | 3.13 process | total tasks/s |
|---|---:|---:|---:|---:|
| noop | 11452 (33 %) | 11913 (33 %) | 11639 (34 %) | 35161 |
| realistic | 1464 (35 %) | 1364 (31 %) | 1469 (34 %) | 4300 |
| peak PSS / RSS MB, noop | 40 / 48 | 272 / 492 | 241 / 475 | |
| peak PSS / RSS MB, realistic | 40 / 48 | 273 / 493 | 242 / 475 | |

### Queue per group

- 3.14t thread: Python 3.14.7t, 1 process × 16 threads
- 3.14 process: Python 3.14.8, 16 processes
- 3.13 process: Python 3.13.15, 16 processes

| workload | 3.14t thread | 3.14 process | 3.13 process | total tasks/s |
|---|---:|---:|---:|---:|
| noop | 11653 (33 %) | 11642 (33 %) | 11609 (33 %) | 34410 |
| realistic | 1428 (33 %) | 1379 (33 %) | 1428 (33 %) | 4212 |
| peak PSS / RSS MB, noop | 40 / 48 | 272 / 492 | 241 / 474 | |
| peak PSS / RSS MB, realistic | 40 / 48 | 272 / 493 | 241 / 475 | |

