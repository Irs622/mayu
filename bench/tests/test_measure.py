import subprocess
import sys
import time

import psutil
import pytest

from measure import steady_rate, tree_memory


def test_steady_rate_ignores_startup_and_tail():
    samples = [(0.0, 0), (5.0, 0), (6.0, 10), (7.0, 50), (8.0, 90), (9.0, 95), (30.0, 100)]
    # first sample at >= 5 done is (6.0, 10); first at >= 95 is (9.0, 95)
    assert steady_rate(samples, 100) == pytest.approx(85 / 3)


def test_steady_rate_needs_two_distinct_points():
    with pytest.raises(ValueError):
        steady_rate([(0.0, 0), (1.0, 100)], 100)


def test_tree_memory_counts_children():
    code = ("import subprocess, sys, time; "
            "subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(30)']); time.sleep(30)")
    parent = subprocess.Popen([sys.executable, "-c", code])
    try:
        proc = psutil.Process(parent.pid)
        deadline = time.monotonic() + 10
        while not proc.children() and time.monotonic() < deadline:
            time.sleep(0.05)
        assert proc.children()
        rss, pss = tree_memory(parent.pid)
        assert rss > proc.memory_info().rss
        assert pss > 0
    finally:
        for c in psutil.Process(parent.pid).children(recursive=True):
            c.kill()
        parent.kill()
        parent.wait()


def test_tree_memory_of_a_missing_process_is_zero():
    p = subprocess.Popen([sys.executable, "-c", "pass"])
    p.wait()
    assert tree_memory(p.pid) == (0, 0)
