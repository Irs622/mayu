"""The guarantee test (spec §9): every task executes at least once despite kill -9 and Redis restarts."""
import os
import pathlib
import random
import signal
import subprocess
import sys
import time
import uuid

from mayu import App

HERE = pathlib.Path(__file__).parent
N = int(os.environ.get("MAYU_CHAOS_N", "2000"))
CHAOS_SECONDS = float(os.environ.get("MAYU_CHAOS_SECONDS", "20"))
WORKERS = 3


def _spawn(url: str, queue: str) -> subprocess.Popen:
    env = {**os.environ, "MAYU_BROKER": url, "MAYU_DEFAULT_QUEUE": queue, "PYTHONPATH": str(HERE)}
    return subprocess.Popen(
        [sys.executable, "-m", "mayu", "worker", "chaos_app:app", "--threads", "8",
         "--visibility-timeout", "2", "--heartbeat-interval", "0.5",
         "--max-deliveries", "1000", "--shutdown-grace", "5"],
        env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )


def test_no_task_lost_under_kill9_and_redis_restart(chaos_redis, restart_redis):
    queue = f"chaos-{uuid.uuid4().hex[:8]}"
    app = App(broker=chaos_redis, result_ttl=3600, default_queue=queue)

    @app.task(name="chaos.work", max_retries=0)
    def work(i):
        pass

    handles = [work.send(i) for i in range(N)]
    rng = random.Random(1234)
    workers = [_spawn(chaos_redis, queue) for _ in range(WORKERS)]
    kills = restarts = 0
    try:
        start = time.monotonic()
        restart_at = start + min(8.0, CHAOS_SECONDS / 2)
        while time.monotonic() < start + CHAOS_SECONDS:
            time.sleep(rng.uniform(0.3, 1.5))
            victim = rng.randrange(WORKERS)
            workers[victim].kill()
            workers[victim].wait()
            workers[victim] = _spawn(chaos_redis, queue)
            kills += 1
            if restarts == 0 and time.monotonic() >= restart_at:
                restart_redis()
                restarts += 1

        deadline = time.monotonic() + 180
        lost = []
        for i, h in enumerate(handles):
            try:
                assert h.result(timeout=max(0.1, deadline - time.monotonic())) == i
            except TimeoutError:
                lost.append(h.id)
        assert not lost, f"{len(lost)} of {N} tasks lost (kills={kills}, restarts={restarts})"

        settle = time.monotonic() + 30
        while True:
            stats = app.client.queue_stats(queue)
            if stats["stream"] == 0 and stats["pending"] == 0:
                break
            assert time.monotonic() < settle, f"queue did not settle: {stats}"
            time.sleep(0.2)
        assert stats["dead"] == 0 and stats["delayed"] == 0, stats
    finally:
        for w in workers:
            w.send_signal(signal.SIGTERM)
        for w in workers:
            try:
                w.wait(timeout=15)
            except subprocess.TimeoutExpired:
                w.kill()
                w.wait()
    print(f"\nchaos: n={N} kills={kills} restarts={restarts}")
    assert kills >= 5, f"only {kills} kills; chaos phase too short"
    assert restarts == 1
