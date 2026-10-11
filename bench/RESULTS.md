# mayu benchmarks

12th Gen Intel(R) Core(TM) i9-12900H, 20 cores, Redis 8.10.2.

## Method

- Each run pre-fills the queue with N tasks, then starts the workers. Throughput is the steady rate between 5 % and
  95 % of completions, so worker startup and the drain tail are excluded. Wall time includes startup.
- Every system runs at concurrency 64 with its defaults; only results and retries are off. Delivery semantics
  differ: Celery acknowledges a task before running it by default, mayu after it finishes.
- mayu thread mode runs on free-threaded CPython 3.14t; everything else runs on CPython 3.14 with the GIL.
- Memory is the peak over the run of the whole worker process tree, sampled every 250 ms. PSS shares pages
  fairly between forked children; RSS counts shared pages once per process.
- RQ runs `SimpleWorker` processes. Its default worker forks a work horse per job and reconnects to Redis each
  time; at this rate that exhausts the ephemeral ports behind Docker's userland proxy and the run cannot finish.
- Each figure is the median of the repeated runs. Reproduce with `bench/setup.sh && bench/all.sh`.

## No-op task throughput

The task body is one Redis `INCR` (the completion counter every system shares).

| system | layout | tasks/s | wall s | peak PSS MB | peak RSS MB | vs Celery |
|---|---|---:|---:|---:|---:|---:|
| mayu-thread | 2 processes × 32 threads (Python 3.14.7t, 0.1.0) | 20847 | 1.2 | 268 | 315 | 18.69× |
| dramatiq | 2 processes × 32 threads (Python 3.14.7, 2.2.1) | 11259 | 2.0 | 81 | 137 | 10.10× |
| mayu-process | 64 processes (Python 3.14.7, 0.1.0) | 11073 | 2.8 | 1375 | 2612 | 9.93× |
| streaq | 2 processes × 32 async tasks (Python 3.14.7, 7.2.0) | 2658 | 8.0 | 114 | 170 | 2.38× |
| rq | 64 SimpleWorker processes, no fork per job (Python 3.14.7, 2.12.0) | 2124 | 9.7 | 518 | 2225 | 1.90× |
| celery | 64 prefork processes (Python 3.14.7, 5.6.3) | 1115 | 19.4 | 1599 | 3204 | 1.00× |

## Realistic task throughput

JSON-decode a 2.6 KB payload, sleep 5 ms (simulated I/O), about 1 ms of pure-Python CPU work, then the `INCR`.

| system | layout | tasks/s | wall s | peak PSS MB | peak RSS MB | vs Celery |
|---|---|---:|---:|---:|---:|---:|
| mayu-thread | 2 processes × 32 threads (Python 3.14.7t, 0.1.0) | 3841 | 1.5 | 227 | 274 | 5.25× |
| mayu-process | 64 processes (Python 3.14.7, 0.1.0) | 2882 | 2.6 | 1375 | 2613 | 3.94× |
| rq | 64 SimpleWorker processes, no fork per job (Python 3.14.7, 2.12.0) | 1613 | 3.3 | 515 | 2231 | 2.20× |
| dramatiq | 2 processes × 32 threads (Python 3.14.7, 2.2.1) | 1018 | 5.1 | 82 | 137 | 1.39× |
| streaq | 2 processes × 32 async tasks (Python 3.14.7, 7.2.0) | 877 | 6.2 | 118 | 175 | 1.20× |
| celery | 64 prefork processes (Python 3.14.7, 5.6.3) | 732 | 8.3 | 1610 | 3222 | 1.00× |

## Memory at equal concurrency

Peak PSS during the realistic workload, 64 tasks in flight.

- mayu, 2 processes × 32 threads: **227 MB**
- Celery, 64 prefork processes: **1610 MB**
- Celery uses 7.1× the memory of mayu.

