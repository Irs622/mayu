import subprocess
import time

import pytest

NAME = "mayu-chaos-redis"
PORT = 6391
URL = f"redis://127.0.0.1:{PORT}/0"


def _ping() -> bool:
    r = subprocess.run(["docker", "exec", NAME, "redis-cli", "ping"], capture_output=True, text=True)
    return "PONG" in r.stdout


def _wait_ready(timeout: float = 30) -> None:
    deadline = time.monotonic() + timeout
    while not _ping():
        assert time.monotonic() < deadline, "chaos redis did not come up"
        time.sleep(0.2)


@pytest.fixture(scope="session")
def chaos_redis():
    subprocess.run(["docker", "rm", "-f", NAME], capture_output=True)
    subprocess.run(
        ["docker", "run", "-d", "--rm", "--name", NAME, "-p", f"{PORT}:6379", "redis:8",
         "redis-server", "--appendonly", "yes", "--appendfsync", "always"],
        check=True, capture_output=True,
    )
    _wait_ready()
    yield URL
    subprocess.run(["docker", "rm", "-f", NAME], capture_output=True)


@pytest.fixture
def restart_redis():
    def restart():
        subprocess.run(["docker", "restart", NAME], check=True, capture_output=True)
        _wait_ready()
    return restart
