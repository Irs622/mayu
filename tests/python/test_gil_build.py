"""Process mode on a GIL build of CPython (spec §9 CI matrix)."""
import os
import pathlib
import signal
import subprocess

import pytest

from mayu import App

HERE = pathlib.Path(__file__).parent
ROOT = HERE.parent.parent
GIL_PYTHON = os.environ.get("MAYU_GIL_PYTHON", str(ROOT / ".venv-gil" / "bin" / "python"))

pytestmark = pytest.mark.skipif(not os.path.exists(GIL_PYTHON),
                                reason=f"GIL interpreter not found at {GIL_PYTHON}; set MAYU_GIL_PYTHON or see README Development")


def test_auto_mode_on_gil_build_uses_process_mode(redis_url, queue):
    # The ft-stress leg exports PYTHON_GIL=0, and a GIL build refuses to start with it.
    env = {**{k: v for k, v in os.environ.items() if k != "PYTHON_GIL"},
           "MAYU_BROKER": redis_url, "MAYU_DEFAULT_QUEUE": queue, "PYTHONPATH": str(HERE)}
    proc = subprocess.Popen([GIL_PYTHON, "-m", "mayu", "worker", "sample_app:app",
                             "--processes", "2", "--shutdown-grace", "5"],
                            env=env, stderr=subprocess.PIPE, text=True)
    try:
        producer = App(broker=redis_url, result_ttl=60, default_queue=queue)

        @producer.task(name="sample.whoami")
        def whoami():
            pass

        results = [tuple(h.result(timeout=30)) for h in [whoami.send() for _ in range(4)]]
        assert all(pid != proc.pid for _, pid in results), "tasks run in child processes"
    finally:
        proc.send_signal(signal.SIGTERM)
        _, err = proc.communicate(timeout=30)
    assert proc.returncode == 0
    assert "using process mode with 2 processes" in err
    assert "this Python build has a GIL" in err
