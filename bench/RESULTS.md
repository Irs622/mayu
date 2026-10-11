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
| dramatiq | 2 processes × 32 threads (Python 3.14.7, 2.2.1) | 11116 | 2.0 | 81 | 137 | 12.62× |
| mayu-process | 64 processes (Python 3.14.7, 0.1.0) | 10829 | 2.8 | 1374 | 2607 | 12.29× |
| mayu-thread | 2 processes × 32 threads (Python 3.14.7t, 0.1.0) | 6398 | 3.3 | 261 | 308 | 7.26× |
| streaq | 2 processes × 32 async tasks (Python 3.14.7, 7.2.0) | 2246 | 9.4 | 114 | 170 | 2.55× |
| rq | 64 SimpleWorker processes, no fork per job (Python 3.14.7, 2.12.0) | 1917 | 10.5 | 515 | 2224 | 2.18× |
| celery | 64 prefork processes (Python 3.14.7, 5.6.3) | 881 | 25.1 | 1599 | 3205 | 1.00× |

## Realistic task throughput

JSON-decode a 2.6 KB payload, sleep 5 ms (simulated I/O), about 1 ms of pure-Python CPU work, then the `INCR`.

| system | layout | tasks/s | wall s | peak PSS MB | peak RSS MB | vs Celery |
|---|---|---:|---:|---:|---:|---:|
| mayu-thread | 2 processes × 32 threads (Python 3.14.7t, 0.1.0) | 3174 | 1.8 | 176 | 222 | 5.71× |
| mayu-process | 64 processes (Python 3.14.7, 0.1.0) | 2588 | 3.0 | 1375 | 2608 | 4.66× |
| rq | 64 SimpleWorker processes, no fork per job (Python 3.14.7, 2.12.0) | 1308 | 4.1 | 518 | 2226 | 2.35× |
| streaq | 2 processes × 32 async tasks (Python 3.14.7, 7.2.0) | 945 | 5.7 | 118 | 175 | 1.70× |
| dramatiq | 2 processes × 32 threads (Python 3.14.7, 2.2.1) | 825 | 6.4 | 82 | 137 | 1.48× |
| celery | 64 prefork processes (Python 3.14.7, 5.6.3) | 556 | 11.0 | 1606 | 3212 | 1.00× |

## Memory at equal concurrency

Peak PSS during the realistic workload, 64 tasks in flight.

- mayu, 2 processes × 32 threads: **176 MB**
- Celery, 64 prefork processes: **1606 MB**
- Celery uses 9.1× the memory of mayu.

