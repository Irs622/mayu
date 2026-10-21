"""GPU monitoring: device visibility, `auto` resources, the sampler, the VRAM guard and the GPU metrics."""
import dataclasses
import logging
import os
import subprocess
import sys
import textwrap
import threading
import time
import urllib.request

import pytest

from mayu import gpu as gpu_mod
from mayu.cli import parse_resources
from mayu.gpu import GIB, TUNE_COOLDOWN, TUNE_MEMORY, TUNE_WINDOW, Gpu, Guard, Sampler, Tuner, UnderDeclared, resolve_auto, visible
from mayu.worker import WorkerRunner

# monkeypatch swaps mayu.gpu attributes that every thread of a pytest-run-parallel run shares
thread_unsafe = pytest.mark.thread_unsafe(reason="patches mayu.gpu for the whole process")
# one caplog is shared by every thread of a parallel run, so a test that reads it can't share it
log_unsafe = pytest.mark.thread_unsafe(reason="reads the log, which every thread of a parallel run writes")

TWO = [(0, "GPU-aaaa1111"), (1, "GPU-bbbb2222")]


@pytest.mark.parametrize("spec,expected", [
    (None, [0, 1]),
    ("", []),
    ("  ", []),
    ("-1", []),
    ("0", [0]),
    ("1,0", [1, 0]),
    ("0,1", [0, 1]),
    (" 1 ", [1]),
    ("GPU-bbbb", [1]),
    ("gpu-aaaa1111,GPU-bbbb2222", [0, 1]),
    ("1,GPU-aaaa", [1, 0]),
    ("0,7,1", [0]),  # the list ends at the first id that names no device, as in CUDA
    ("0,-1,1", [0]),
    ("GPU-cccc", []),
    ("0,0", [0]),
])
def test_visible_devices(spec, expected):
    assert visible(spec, TWO) == expected


@pytest.mark.parametrize("spec,message", [
    ("MIG-1234", "is not supported by mayu's GPU monitoring"),
    ("0,MIG-GPU-aaaa/1/2", "is not supported by mayu's GPU monitoring"),
    ("zero", "is not supported by mayu's GPU monitoring"),
    ("GPU-", "prefix of more than one device"),
])
def test_visible_devices_rejects_what_it_cannot_follow(spec, message):
    with pytest.raises(ValueError, match=message):
        visible(spec, TWO)


class FakeGpus:
    """A backend whose devices the test changes while the sampler reads them."""

    def __init__(self, *gpus):
        self.lock = threading.Lock()
        self.gpus = list(gpus)
        self.fail = False
        self.reads = 0

    def devices(self):
        with self.lock:
            self.reads += 1
            if self.fail:
                raise OSError("NVML is unhappy")
            return [dataclasses.replace(g) for g in self.gpus]

    def set(self, index=0, **fields):
        with self.lock:
            for k, v in fields.items():
                setattr(self.gpus[index], k, v)


def card(index=0, total_gib=8.0, used_gib=0.0, process_gib=0.0, utilization=0.5, **fields):
    return Gpu(index, f"GPU-{index:04d}", "Fake RTX", int(total_gib * GIB), int(used_gib * GIB), utilization,
               process_memory=None if process_gib is None else int(process_gib * GIB), **fields)


@thread_unsafe
def test_auto_sizes_resources_from_the_visible_gpu(monkeypatch):
    monkeypatch.setattr(gpu_mod, "_backend", FakeGpus(card()))
    assert resolve_auto({"gpu": "auto", "vram_gb": "auto", "cpu": 4}) == {"gpu": 1.0, "vram_gb": 7.5, "cpu": 4}
    assert resolve_auto({"vram_gb": "auto"}) == {"vram_gb": 7.5}
    assert resolve_auto({"gpu": 2, "vram_gb": 5.0}) == {"gpu": 2, "vram_gb": 5.0}  # nothing to resolve


@thread_unsafe
def test_auto_sums_the_gpus_and_floors_to_a_tenth(monkeypatch):
    monkeypatch.setattr(gpu_mod, "_backend", FakeGpus(card(0, 8.0), card(1, 11.95)))
    assert resolve_auto({"gpu": "auto", "vram_gb": "auto"}) == {"gpu": 2.0, "vram_gb": 18.9}  # 19.95 - 1.0, floored


@thread_unsafe
def test_auto_without_nvml_or_a_gpu_is_a_clear_error(monkeypatch):
    monkeypatch.setattr(gpu_mod, "_backend", None)  # conftest: no real NVML
    with pytest.raises(ValueError, match=r"vram_gb=auto needs NVIDIA's NVML: pip install 'mayu\[gpu\]'"):
        resolve_auto({"vram_gb": "auto"})
    monkeypatch.setattr(gpu_mod, "_backend", FakeGpus())
    with pytest.raises(ValueError, match="gpu=auto found no visible GPU"):
        resolve_auto({"gpu": "auto"})
    broken = FakeGpus(card())
    broken.fail = True
    monkeypatch.setattr(gpu_mod, "_backend", broken)
    with pytest.raises(ValueError, match="gpu=auto could not read the GPUs"):
        resolve_auto({"gpu": "auto"})


@pytest.mark.parametrize("name", ["cpu", "pool:x"])
def test_auto_is_only_for_gpu_and_vram(name):
    with pytest.raises(ValueError, match=rf"{name}=auto is not supported: only gpu and vram_gb can be 'auto'"):
        resolve_auto({name: "auto"})


def test_parse_resources_takes_auto_for_gpu_and_vram_only():
    assert parse_resources("gpu=auto,vram_gb=auto,cpu=4") == {"gpu": "auto", "vram_gb": "auto", "cpu": 4.0}
    assert parse_resources("vram_gb= auto ") == {"vram_gb": "auto"}
    with pytest.raises(ValueError, match="only gpu and vram_gb can be auto"):
        parse_resources("cpu=auto")
    with pytest.raises(ValueError, match="given twice"):
        parse_resources("gpu=auto,gpu=auto")
    with pytest.raises(ValueError, match="bad --resources entry 'gpu=automatic'"):
        parse_resources("gpu=automatic")


@thread_unsafe
def test_worker_resolves_auto_resources(app, monkeypatch, caplog):
    monkeypatch.setattr(gpu_mod, "_backend", FakeGpus(card()))
    with caplog.at_level(logging.INFO, logger="mayu.worker"):
        w = WorkerRunner(app, threads=1, resources={"gpu": "auto", "vram_gb": "auto"})
    assert w.resources == {"gpu": 1.0, "vram_gb": 7.5}
    assert "resolved resources from the visible GPUs: gpu=1, vram_gb=7.5" in caplog.text


def test_worker_validates_the_monitor_options(app):
    with pytest.raises(ValueError, match="gpu_monitor must be 'auto', 'on' or 'off', got 'sometimes'"):
        WorkerRunner(app, threads=1, gpu_monitor="sometimes")
    for bad in (0, -1, float("nan"), float("inf"), "2", True, None):
        with pytest.raises(ValueError, match="gpu_interval must be a positive number of seconds"):
            WorkerRunner(app, threads=1, gpu_interval=bad)
    with pytest.raises(ValueError, match="--gpu-monitor off turns away"):
        WorkerRunner(app, threads=1, gpu_monitor="off", resources={"gpu": "auto"})
    with pytest.raises(ValueError, match="cpu=auto is not supported"):
        WorkerRunner(app, threads=1, resources={"cpu": "auto"})


def test_worker_validates_autotune(app):
    @app.task(resources={"gpu": 0.25})
    def small(): ...

    with pytest.raises(ValueError, match="autotune must be 'gpu'.*got 'vram_gb'"):
        WorkerRunner(app, threads=1, autotune="vram_gb", resources={"gpu": 1.0})
    with pytest.raises(ValueError, match="--gpu-monitor off turns away"):
        WorkerRunner(app, threads=1, autotune="gpu", gpu_monitor="off", resources={"gpu": 1.0})


@thread_unsafe
def test_autotune_needs_nvml_a_gpu_resource_and_a_task_that_requests_it(app, monkeypatch):
    monkeypatch.setattr(gpu_mod, "_backend", None)
    with pytest.raises(ValueError, match=r"--autotune gpu needs NVIDIA's NVML: pip install 'mayu\[gpu\]'"):
        WorkerRunner(app, threads=1, autotune="gpu", resources={"gpu": 1.0})
    monkeypatch.setattr(gpu_mod, "_backend", FakeGpus())
    with pytest.raises(ValueError, match="--autotune gpu found no readable GPU"):
        WorkerRunner(app, threads=1, autotune="gpu", resources={"gpu": 1.0})
    monkeypatch.setattr(gpu_mod, "_backend", FakeGpus(card()))
    with pytest.raises(ValueError, match="--autotune gpu needs a gpu resource to tune: pass --resources gpu="):
        WorkerRunner(app, threads=1, autotune="gpu", resources={"cpu": 4})
    with pytest.raises(ValueError, match="--autotune gpu needs a served task that requests gpu"):
        WorkerRunner(app, threads=1, autotune="gpu", resources={"gpu": 1.0})

    @app.task(resources={"gpu": 0.25})
    def small(): ...

    @app.task(resources={"gpu": 0.5})
    def medium(): ...

    w = WorkerRunner(app, threads=1, autotune="gpu", resources={"gpu": 1.0})
    assert (w._tuner.capacity, w._tuner._floor, w._tuner._step) == (1.0, 500, 250)  # ceiling, largest, smallest
    assert WorkerRunner(app, threads=1, resources={"gpu": 1.0})._tuner is None


@thread_unsafe
def test_monitor_on_needs_a_backend_and_a_device(app, monkeypatch):
    monkeypatch.setattr(gpu_mod, "_backend", None)
    with pytest.raises(ValueError, match=r"--gpu-monitor on needs NVIDIA's NVML: pip install 'mayu\[gpu\]'"):
        WorkerRunner(app, threads=1, gpu_monitor="on")
    assert WorkerRunner(app, threads=1)._gpu_backend is None  # auto: silently off
    monkeypatch.setattr(gpu_mod, "_backend", FakeGpus())
    with pytest.raises(ValueError, match="--gpu-monitor on found no visible GPU"):
        WorkerRunner(app, threads=1, gpu_monitor="on")
    assert WorkerRunner(app, threads=1)._gpu_backend is None


class TestGuard:
    def test_foreign_use_within_the_slack_costs_nothing(self):
        assert Guard(0.5).observe(0.3) == 0.0
        assert Guard(0.5).observe(0.5) == 0.0

    def test_a_bigger_cut_applies_at_once_rounded_up_to_a_tenth(self):
        g = Guard(0.5)
        assert g.observe(1.7) == 1.2
        assert g.observe(1.71) == 1.3  # 1.21 asked: up to 1.3
        assert g.observe(3.0) == 2.5

    def test_a_smaller_cut_applies_only_after_three_ticks(self):
        g = Guard(0.5)
        assert g.observe(1.7) == 1.2
        assert [g.observe(0.2), g.observe(0.2)] == [1.2, 1.2]
        assert g.observe(0.2) == 0.0

    def test_the_largest_of_the_three_asks_wins(self):
        g = Guard(0.5)
        g.observe(2.5)  # withholds 2.0
        assert [g.observe(1.0), g.observe(1.4), g.observe(1.2)] == [2.0, 2.0, 0.9]

    def test_a_blip_restarts_the_count(self):
        g = Guard(0.5)
        assert g.observe(1.7) == 1.2
        assert [g.observe(0.2), g.observe(0.2)] == [1.2, 1.2]
        assert g.observe(1.7) == 1.2  # the old level again: the two calm ticks no longer count
        assert [g.observe(0.2), g.observe(0.2)] == [1.2, 1.2]
        assert g.observe(0.2) == 0.0

    def test_never_withholds_more_than_the_limit(self):
        assert Guard(0.5, limit_gib=7.5).observe(40.0) == 7.5

    def test_float_noise_does_not_round_up_a_tenth_too_far(self):
        assert Guard(0.5).observe(1.7000000000000002) == 1.2
        assert Guard(0.0).observe(0.30000000000000004) == 0.3


def feed(tuner, util, blocked=0, pressure=False, ticks=TUNE_WINDOW):
    """The deltas `ticks` observations return."""
    return [tuner.observe(util, blocked, pressure) for _ in range(ticks)]


class TestTuner:
    def test_a_saturated_window_shrinks_one_step_and_stops_at_the_floor(self):
        t = Tuner(ceiling=1.0, floor=0.25, step=0.25)
        assert feed(t, 0.99) == [0, 0, 0, 0, -0.25]
        assert t.capacity == 0.75
        assert feed(t, 0.95, ticks=2 * TUNE_WINDOW) == [0, 0, 0, 0, -0.25, 0, 0, 0, 0, -0.25]
        assert t.capacity == 0.25
        assert feed(t, 1.0, ticks=3 * TUNE_WINDOW) == [0] * 3 * TUNE_WINDOW  # at the floor: the biggest task fits

    def test_the_floor_is_the_biggest_task_not_a_multiple_of_the_step(self):
        t = Tuner(ceiling=1.0, floor=0.5, step=0.25)
        feed(t, 0.99, ticks=5 * TUNE_WINDOW)
        assert t.capacity == 0.5

    def test_the_mean_decides_not_one_tick(self):
        t = Tuner(1.0, 0.25, 0.25)
        assert [t.observe(u, 0, False) for u in (1.0, 1.0, 1.0, 0.5, 0.8)] == [0] * 5  # mean 0.86
        assert [t.observe(u, 0, False) for u in (1.0, 1.0, 1.0, 1.0, 0.5)] == [0, 0, 0, 0, -0.25]  # mean 0.9

    def test_an_idle_gpu_with_waiting_tasks_grows_one_step_and_stops_at_the_ceiling(self):
        t = Tuner(1.0, 0.25, 0.25, cooldown=0)
        feed(t, 0.99, ticks=3 * TUNE_WINDOW)
        assert t.capacity == 0.25
        assert feed(t, 0.3, blocked=2, ticks=2 * TUNE_WINDOW) == [0, 0, 0, 0, 0.25, 0, 0, 0, 0, 0.25]
        assert t.capacity == 0.75
        feed(t, 0.3, blocked=2, ticks=TUNE_WINDOW)
        assert t.capacity == 1.0
        assert feed(t, 0.3, blocked=2, ticks=2 * TUNE_WINDOW) == [0] * 2 * TUNE_WINDOW

    def test_nothing_waiting_holds(self):
        t = Tuner(1.0, 0.25, 0.25)
        feed(t, 0.99, ticks=TUNE_WINDOW)
        assert feed(t, 0.1, blocked=0, ticks=3 * TUNE_WINDOW) == [0] * 3 * TUNE_WINDOW
        assert t.capacity == 0.75

    def test_the_middle_band_holds_whatever_waits(self):
        t = Tuner(1.0, 0.25, 0.25)
        feed(t, 0.99, ticks=TUNE_WINDOW)
        assert feed(t, 0.8, blocked=5, ticks=3 * TUNE_WINDOW) == [0] * 3 * TUNE_WINDOW
        assert t.capacity == 0.75

    def test_blocked_counts_at_the_end_of_the_window_only(self):
        t = Tuner(1.0, 0.25, 0.25, cooldown=0)
        feed(t, 0.99, ticks=TUNE_WINDOW)
        assert [t.observe(0.2, 3, False) for _ in range(4)] == [0] * 4
        assert t.observe(0.2, 0, False) == 0  # nothing waits when the window closes
        assert [t.observe(0.2, 0, False) for _ in range(4)] == [0] * 4
        assert t.observe(0.2, 1, False) == 0.25

    def test_vram_pressure_anywhere_in_the_window_stops_growth(self):
        t = Tuner(1.0, 0.25, 0.25, cooldown=0)
        feed(t, 0.99, ticks=TUNE_WINDOW)
        assert [t.observe(0.2, 2, p) for p in (False, True, False, False, False)] == [0] * 5
        assert t.capacity == 0.75
        assert feed(t, 0.2, blocked=2) == [0, 0, 0, 0, 0.25]  # the pressure flag does not outlive its window

    def test_pressure_does_not_stop_shrinking(self):
        t = Tuner(1.0, 0.25, 0.25)
        assert feed(t, 0.99, pressure=True)[-1] == -0.25

    def test_one_decision_per_window_so_a_change_never_steps_twice(self):
        t = Tuner(1.0, 0.25, 0.25)
        deltas = feed(t, 0.99, ticks=TUNE_WINDOW + 1)
        assert deltas[TUNE_WINDOW - 1] == -0.25 and deltas[TUNE_WINDOW] == 0  # the next window starts empty
        assert t.capacity == 0.75
        assert not t.closes_window
        feed(t, 0.99, ticks=TUNE_WINDOW - 2)
        assert t.closes_window

    def test_a_hold_also_restarts_the_window(self):
        t = Tuner(1.0, 0.25, 0.25)
        assert feed(t, 0.8, ticks=TUNE_WINDOW) == [0] * TUNE_WINDOW
        assert feed(t, 0.99, ticks=TUNE_WINDOW)[-1] == -0.25  # the 0.8 ticks are gone, not averaged in

    def test_thousandths_do_not_drift(self):
        t = Tuner(1.0, 0.1, 0.1, cooldown=0)
        for _ in range(9):
            assert feed(t, 0.99)[-1] == -0.1
        assert t.capacity == 0.1
        for _ in range(9):
            assert feed(t, 0.1, blocked=1)[-1] == 0.1
        assert t.capacity == 1.0

    def test_revert_undoes_a_change_that_was_not_applied(self):
        t = Tuner(1.0, 0.25, 0.25)
        delta = feed(t, 0.99)[-1]
        t.revert(delta)
        assert t.capacity == 1.0
        assert feed(t, 0.99)[-1] == -0.25

    def test_after_a_shrink_growth_waits_out_the_cooldown(self):
        t = Tuner(1.0, 0.25, 0.25, cooldown=2)
        assert feed(t, 0.95)[-1] == -0.25  # four tasks saturate the GPU
        assert feed(t, 0.68, blocked=1, ticks=2 * TUNE_WINDOW) == [0] * 2 * TUNE_WINDOW  # three leave it idle
        assert feed(t, 0.68, blocked=1)[-1] == 0.25
        t = Tuner(1.0, 0.25, 0.25)  # the default cooldown
        feed(t, 0.95)
        assert feed(t, 0.3, blocked=1, ticks=TUNE_COOLDOWN * TUNE_WINDOW) == [0] * TUNE_COOLDOWN * TUNE_WINDOW
        assert feed(t, 0.3, blocked=1)[-1] == 0.25

    def test_a_saturated_gpu_keeps_a_level_whose_lower_one_left_it_idle(self):
        # one bursty task keeps the GPU 60% busy, two saturate it: two get more done, so stay at two
        t = Tuner(1.0, 0.25, 0.25, cooldown=0)
        feed(t, 0.99, blocked=4, ticks=3 * TUNE_WINDOW)
        assert t.capacity == 0.25
        assert feed(t, 0.6, blocked=4)[-1] == 0.25
        assert feed(t, 1.0, blocked=4, ticks=4 * TUNE_WINDOW) == [0] * 4 * TUNE_WINDOW
        assert t.capacity == 0.5

    def test_a_lower_level_counts_only_when_it_was_full(self):
        # 40% with nothing waiting says nothing about what that level does under load
        t = Tuner(1.0, 0.25, 0.25, cooldown=0)
        feed(t, 0.99, blocked=4)
        assert t.capacity == 0.75
        feed(t, 0.4, blocked=0)
        feed(t, 0.99, blocked=4, ticks=2 * TUNE_WINDOW)
        assert t.capacity == 0.25  # 0.75 → 0.5 → 0.25: nothing recorded at 0.5 or 0.25 stopped it

    def test_an_old_reading_of_the_lower_level_expires(self):
        now = [0.0]
        t = Tuner(1.0, 0.25, 0.25, cooldown=0, clock=lambda: now[0])
        feed(t, 0.99, blocked=4, ticks=3 * TUNE_WINDOW)
        feed(t, 0.6, blocked=4)
        assert t.capacity == 0.5
        now[0] = TUNE_MEMORY - 1
        assert feed(t, 1.0, blocked=4)[-1] == 0
        now[0] = TUNE_MEMORY + 1  # the workload may have changed since 0.25 was measured
        assert feed(t, 1.0, blocked=4)[-1] == -0.25

    @pytest.mark.parametrize("ceiling,floor,step", [(1.0, 2.0, 0.5), (1.0, 0.5, 0), (1.0, 0, 0.5), (1.0, 0.5, -1)])
    def test_bad_bounds(self, ceiling, floor, step):
        with pytest.raises(ValueError, match="tuner needs"):
            Tuner(ceiling, floor, step)


class TestUnderDeclared:
    def test_fires_after_three_ticks_over_the_margin_then_not_for_five_minutes(self):
        now = [1000.0]
        u = UnderDeclared(lambda: now[0])
        assert [u.observe(8.0, 7.2) for _ in range(2)] == [False, False]
        assert u.observe(8.0, 7.2) is True
        now[0] += 299
        assert [u.observe(8.0, 7.2) for _ in range(5)] == [False] * 5
        now[0] += 2
        assert u.observe(8.0, 7.2) is True

    def test_within_the_margin_never_fires_and_a_calm_tick_restarts_the_count(self):
        u = UnderDeclared(lambda: 0.0)
        assert not any(u.observe(7.6, 7.2) for _ in range(10))  # 0.4 over, margin 0.72
        assert [u.observe(9.0, 7.2), u.observe(9.0, 7.2), u.observe(7.0, 7.2), u.observe(9.0, 7.2)] == [False] * 4
        assert [u.observe(9.0, 7.2), u.observe(9.0, 7.2)] == [False, True]

    def test_the_margin_is_half_a_gib_or_ten_percent(self):
        small, big = UnderDeclared(lambda: 0.0), UnderDeclared(lambda: 0.0)
        assert not any(small.observe(1.4, 1.0) for _ in range(5))  # 0.4 over 1.0: under half a GiB
        assert any(small.observe(1.6, 1.0) for _ in range(5))
        assert not any(big.observe(21.9, 20.0) for _ in range(5))  # 1.9 over 20: under 10 %
        assert any(big.observe(22.1, 20.0) for _ in range(5))


class FakeNative:
    def __init__(self, in_use=0.0):
        self.stats = []
        self.adjusted = []
        self.in_use = in_use
        self.waiting = 0
        self.blocked_calls = 0
        self.fail_adjust = False

    def blocked(self, resource):
        assert resource == "gpu"
        self.blocked_calls += 1
        return self.waiting

    def set_gpu_stats(self, stats):
        self.stats.append(stats)

    def adjust_capacity(self, resource, delta):
        if self.fail_adjust:
            raise RuntimeError("no such resource")
        self.adjusted.append((resource, round(delta, 3)))

    def resource_usage(self):
        return [("vram_gb", 7.5, self.in_use)]


@log_unsafe
def test_a_tick_pushes_the_stats_then_guards_the_vram(caplog):
    fake = FakeGpus(card(used_gib=2.0, process_gib=0.5, power_watts=40.0))
    native = FakeNative()
    sampler = Sampler(native, fake, 1.0, vram_gb=7.5)
    with caplog.at_level(logging.INFO, logger="mayu.gpu"):
        sampler.tick()
        assert native.stats == [[(0, "GPU-0000", 0.5, 2 * GIB, 8 * GIB, GIB // 2, 40.0, None)]]
        assert native.adjusted == [("vram_gb", -1.0)]  # 1.5 foreign, 0.5 slack
        assert "other processes use 1.5 GB of VRAM; this worker now admits up to 6.5" in caplog.text
        sampler.tick()
        assert len(native.adjusted) == 1  # the same level again changes nothing
        fake.set(memory_used=GIB // 2)
        for _ in range(3):
            sampler.tick()
    assert native.adjusted == [("vram_gb", -1.0), ("vram_gb", 1.0)]
    assert "this worker admits up to 7.5 again" in caplog.text


def test_the_guard_needs_a_declared_vram():
    native = FakeNative()
    Sampler(native, FakeGpus(card(used_gib=6.0)), 1.0).tick()
    assert len(native.stats) == 1 and native.adjusted == []


@log_unsafe
def test_the_guard_is_off_when_nvml_cannot_attribute_memory(caplog):
    fake = FakeGpus(card(used_gib=2.0, process_gib=None))
    native = FakeNative()
    sampler = Sampler(native, fake, 1.0, vram_gb=7.5)
    with caplog.at_level(logging.WARNING, logger="mayu.gpu"):
        for _ in range(4):
            sampler.tick()
    assert native.adjusted == [] and len(native.stats) == 4
    assert native.stats[0][0][5] is None
    assert caplog.text.count("the VRAM guard is off") == 1


def test_losing_attribution_gives_back_what_the_guard_withheld():
    fake = FakeGpus(card(used_gib=2.0, process_gib=0.0))
    native = FakeNative()
    sampler = Sampler(native, fake, 1.0, vram_gb=7.5)
    sampler.tick()
    fake.set(process_memory=None)
    sampler.tick()
    assert native.adjusted == [("vram_gb", -1.5), ("vram_gb", 1.5)]


@log_unsafe
def test_a_failed_adjustment_is_retried_on_the_next_tick(caplog):
    class Flaky(FakeNative):
        def adjust_capacity(self, resource, delta):
            if not self.adjusted and not getattr(self, "tried", False):
                self.tried = True
                raise RuntimeError("no")
            super().adjust_capacity(resource, delta)

    native = Flaky()
    sampler = Sampler(native, FakeGpus(card(used_gib=2.0)), 1.0, vram_gb=7.5)
    with caplog.at_level(logging.WARNING, logger="mayu.gpu"):
        sampler.tick()
        sampler.tick()
    assert native.adjusted == [("vram_gb", -1.5)]
    assert "GPU sampler step guard_step failed" in caplog.text


@log_unsafe
def test_device_errors_are_logged_once_a_minute_and_the_tick_is_skipped(caplog):
    now = [0.0]
    fake = FakeGpus(card())
    fake.fail = True
    native = FakeNative()
    sampler = Sampler(native, fake, 1.0, clock=lambda: now[0])
    with caplog.at_level(logging.WARNING, logger="mayu.gpu"):
        for _ in range(5):
            sampler.tick()
        assert caplog.text.count("could not read the GPUs") == 1
        now[0] += 61
        sampler.tick()
        assert caplog.text.count("could not read the GPUs") == 2
    assert native.stats == []
    fake.fail = False
    sampler.tick()
    assert len(native.stats) == 1


@log_unsafe
def test_a_failing_step_does_not_stop_the_others(caplog):
    class NoMetrics(FakeNative):
        def set_gpu_stats(self, stats):
            raise RuntimeError("boom")

    native = NoMetrics()
    with caplog.at_level(logging.WARNING, logger="mayu.gpu"):
        Sampler(native, FakeGpus(card(used_gib=2.0)), 1.0, vram_gb=7.5).tick()
    assert native.adjusted == [("vram_gb", -1.5)]
    assert "GPU sampler step push failed" in caplog.text


@log_unsafe
def test_under_declared_vram_warns_with_both_numbers(caplog):
    fake = FakeGpus(card(used_gib=7.9, process_gib=7.9))
    sampler = Sampler(FakeNative(in_use=7.2), fake, 1.0, vram_gb=7.5)
    with caplog.at_level(logging.WARNING, logger="mayu.gpu"):
        for _ in range(2):
            sampler.tick()
        assert caplog.text == ""  # 0.7 over, margin max(0.5, 0.72)
        fake.set(process_memory=int(8.1 * GIB), memory_used=int(8.1 * GIB))
        for _ in range(3):
            sampler.tick()
    assert ("this worker's processes use 8.1 GB of VRAM but its running tasks and loaded pools declare 7.2; "
            "raise vram_gb= on them or the GPU may run out of memory") in caplog.text


def _tuned(fake, native, ceiling=1.0, floor=0.25, step=0.25, vram_gb=None):
    return Sampler(native, fake, 10.0, vram_gb=vram_gb, tuner=Tuner(ceiling, floor, step, cooldown=0))


@log_unsafe
def test_a_saturated_gpu_runs_fewer_gpu_tasks_and_says_so(caplog):
    fake = FakeGpus(card(utilization=0.97))
    native = FakeNative()
    sampler = _tuned(fake, native)
    with caplog.at_level(logging.INFO, logger="mayu.gpu"):
        for _ in range(TUNE_WINDOW - 1):
            sampler.tick()
        assert native.adjusted == [] and native.blocked_calls == 0  # blocked is only read when the window closes
        sampler.tick()
    assert native.adjusted == [("gpu", -0.25)]
    assert native.blocked_calls == 1
    assert "GPU busy 97% over the last 50 s; running fewer GPU tasks: gpu capacity 1 \u2192 0.75" in caplog.text
    record = next(r for r in caplog.records if "fewer GPU tasks" in r.getMessage())
    assert (record.gpu_utilization, record.gpu_capacity) == (0.97, 0.75)


@log_unsafe
def test_waiting_tasks_grow_capacity_back_after_a_shrink(caplog):
    fake = FakeGpus(card(utilization=0.99))
    native = FakeNative()
    sampler = _tuned(fake, native)
    with caplog.at_level(logging.INFO, logger="mayu.gpu"):
        for _ in range(TUNE_WINDOW):
            sampler.tick()
        fake.set(utilization=0.2)
        native.waiting = 3
        for _ in range(TUNE_WINDOW):
            sampler.tick()
    assert native.adjusted == [("gpu", -0.25), ("gpu", 0.25)]
    assert "GPU only 20% busy over the last 50 s with 3 tasks waiting; running more GPU tasks: " \
           "gpu capacity 0.75 \u2192 1" in caplog.text


def test_the_tuner_averages_the_utilization_over_the_visible_gpus():
    fake = FakeGpus(card(0, utilization=1.0), card(1, utilization=0.5))  # mean 0.75: neither busy nor idle
    native = FakeNative()
    sampler = _tuned(fake, native)
    for _ in range(3 * TUNE_WINDOW):
        sampler.tick()
    assert native.adjusted == []
    fake.set(1, utilization=0.9)  # mean 0.95
    for _ in range(TUNE_WINDOW):
        sampler.tick()
    assert native.adjusted == [("gpu", -0.25)]


def test_vram_pressure_stops_growth_but_not_shrinking():
    fake = FakeGpus(card(utilization=0.99, total_gib=8.0, used_gib=7.7))  # 96 % full
    native = FakeNative()
    native.waiting = 2
    sampler = _tuned(fake, native)
    for _ in range(TUNE_WINDOW):
        sampler.tick()
    assert native.adjusted == [("gpu", -0.25)]
    fake.set(utilization=0.2)
    for _ in range(2 * TUNE_WINDOW):
        sampler.tick()
    assert native.adjusted == [("gpu", -0.25)]  # idle and waiting, but the card is full
    fake.set(memory_used=GIB)
    for _ in range(TUNE_WINDOW):
        sampler.tick()
    assert native.adjusted == [("gpu", -0.25), ("gpu", 0.25)]


def test_a_vram_cut_by_the_guard_counts_as_pressure():
    fake = FakeGpus(card(utilization=0.99, used_gib=2.0, process_gib=0.5))
    native = FakeNative()
    native.waiting = 2
    sampler = _tuned(fake, native, vram_gb=7.5)
    for _ in range(TUNE_WINDOW):
        sampler.tick()
    assert ("vram_gb", -1.0) in native.adjusted and ("gpu", -0.25) in native.adjusted
    fake.set(utilization=0.2)
    for _ in range(2 * TUNE_WINDOW):
        sampler.tick()
    assert ("gpu", 0.25) not in native.adjusted  # 1.5 GiB of foreign use is still withheld


@log_unsafe
def test_a_failed_gpu_adjustment_rolls_the_tuner_back(caplog):
    fake = FakeGpus(card(utilization=0.99))
    native = FakeNative()
    native.fail_adjust = True
    sampler = _tuned(fake, native)
    with caplog.at_level(logging.WARNING, logger="mayu.gpu"):
        for _ in range(TUNE_WINDOW):
            sampler.tick()
    assert sampler._tuner.capacity == 1.0
    assert "GPU sampler step tune failed" in caplog.text
    native.fail_adjust = False
    for _ in range(TUNE_WINDOW):
        sampler.tick()
    assert native.adjusted == [("gpu", -0.25)]


def test_without_a_tuner_the_sampler_never_asks_what_is_blocked():
    native = FakeNative()
    sampler = Sampler(native, FakeGpus(card(utilization=0.99)), 1.0)
    for _ in range(2 * TUNE_WINDOW):
        sampler.tick()
    assert native.blocked_calls == 0 and native.adjusted == []


def test_the_sampler_thread_reads_until_stopped():
    fake = FakeGpus(card())
    native = FakeNative()
    sampler = Sampler(native, fake, 0.01)
    sampler.start()
    deadline = time.monotonic() + 10
    while fake.reads < 3:
        assert time.monotonic() < deadline, "the sampler did not tick"
        time.sleep(0.01)  # bounded poll: no event marks a tick
    assert [t.name for t in threading.enumerate()].count("mayu-gpu") >= 1
    sampler.stop()
    sampler.join()
    assert not sampler._thread.is_alive()


def _scrape(port):
    with urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=5) as r:
        return r.read().decode()


def _series(port, series):
    for line in _scrape(port).splitlines():
        if line.startswith(series + " "):
            return float(line.rsplit(" ", 1)[1])
    return None


def _eventually(predicate, timeout=10.0):
    deadline = time.monotonic() + timeout
    while not predicate():
        assert time.monotonic() < deadline, "condition not reached in time"
        time.sleep(0.01)  # bounded poll: the thing awaited happens on the sampler thread with no event to wait on


MONITORED = dict(metrics_port=0, metrics_host="127.0.0.1", gpu_interval=0.05)
CAPACITY = 'mayu_resource_capacity{resource="vram_gb"}'
LABELS = '{gpu="0",uuid="GPU-0000"}'


@thread_unsafe
def test_metrics_show_the_gpu_families(app, running, monkeypatch):
    monkeypatch.setattr(gpu_mod, "_backend", FakeGpus(card(used_gib=2.0, process_gib=1.0, power_watts=85.5,
                                                          temperature_c=61.0)))
    with running(app, threads=1, resources={"vram_gb": 7.5}, **MONITORED) as w:
        _eventually(lambda: _series(w.metrics_port, f"mayu_gpu_utilization_ratio{LABELS}") is not None)
        port = w.metrics_port
        assert _series(port, f"mayu_gpu_utilization_ratio{LABELS}") == 0.5
        assert _series(port, f"mayu_gpu_memory_used_bytes{LABELS}") == 2.0 * GIB
        assert _series(port, f"mayu_gpu_memory_total_bytes{LABELS}") == 8.0 * GIB
        assert _series(port, f"mayu_gpu_process_memory_bytes{LABELS}") == 1.0 * GIB
        assert _series(port, f"mayu_gpu_power_watts{LABELS}") == 85.5
        assert _series(port, f"mayu_gpu_temperature_celsius{LABELS}") == 61.0


@thread_unsafe
def test_metrics_omit_what_the_gpu_cannot_report(app, running, monkeypatch):
    monkeypatch.setattr(gpu_mod, "_backend", FakeGpus(card(used_gib=2.0, process_gib=None)))
    with running(app, threads=1, **MONITORED) as w:
        _eventually(lambda: _series(w.metrics_port, f"mayu_gpu_utilization_ratio{LABELS}") is not None)
        text = _scrape(w.metrics_port)
    assert "mayu_gpu_memory_used_bytes" in text
    for family in ("mayu_gpu_process_memory_bytes", "mayu_gpu_power_watts", "mayu_gpu_temperature_celsius"):
        assert family not in text


@thread_unsafe
def test_foreign_vram_use_lowers_the_capacity_and_a_task_waits_until_it_is_gone(app, running, monkeypatch):
    fake = FakeGpus(card())
    monkeypatch.setattr(gpu_mod, "_backend", fake)

    @app.task(resources={"vram_gb": 7.0})
    def big():
        return "ran"

    with running(app, threads=2, resources={"vram_gb": 7.5}, **MONITORED) as w:
        port = w.metrics_port
        _eventually(lambda: _series(port, f"mayu_gpu_utilization_ratio{LABELS}") is not None)
        assert _series(port, CAPACITY) == 7.5
        fake.set(memory_used=2 * GIB)  # another process: 2 GiB, the card has 0.5 to spare beyond vram_gb
        _eventually(lambda: _series(port, CAPACITY) == 6.0)
        handle = big.send()
        _eventually(lambda: w.native.blocked("vram_gb") == 1)  # fetched, due, and it does not fit
        fake.set(memory_used=0)  # the other process exits
        assert handle.result(timeout=10) == "ran"
        _eventually(lambda: _series(port, CAPACITY) == 7.5)


@thread_unsafe
def test_a_task_that_fits_the_reduced_capacity_still_runs(app, running, monkeypatch):
    fake = FakeGpus(card(used_gib=2.0))
    monkeypatch.setattr(gpu_mod, "_backend", fake)

    @app.task(resources={"vram_gb": 5.0})
    def small():
        return "ran"

    with running(app, threads=1, resources={"vram_gb": 7.5}, **MONITORED) as w:
        _eventually(lambda: _series(w.metrics_port, CAPACITY) == 6.0)
        assert small.send().result(timeout=10) == "ran"


@thread_unsafe
def test_autotune_lowers_gpu_capacity_while_the_gpu_is_saturated(app, running, monkeypatch, caplog):
    monkeypatch.setattr(gpu_mod, "_backend", FakeGpus(card(utilization=0.99)))

    @app.task(resources={"gpu": 0.25})
    def infer():
        return "ran"

    def gpu_capacity(w):
        return next((cap for name, cap, _ in w.native.resource_usage() if name == "gpu"), None)

    with caplog.at_level(logging.INFO, logger="mayu.gpu"):
        with running(app, threads=4, resources={"gpu": 1.0}, autotune="gpu", **MONITORED) as w:
            assert w._sampler is not None
            _eventually(lambda: gpu_capacity(w) == 0.25)
            assert infer.send().result(timeout=10) == "ran"  # the smallest request still fits
            assert w.native.blocked("gpu") == 0
    assert "running fewer GPU tasks: gpu capacity 0.5 \u2192 0.25" in caplog.text


@thread_unsafe
def test_autotune_gives_capacity_back_when_the_gpu_idles_with_tasks_waiting(app, running, monkeypatch):
    fake = FakeGpus(card(utilization=0.99))
    monkeypatch.setattr(gpu_mod, "_backend", fake)
    release = threading.Event()
    started = threading.Semaphore(0)

    @app.task(resources={"gpu": 0.5})
    def big():
        started.release()
        release.wait(10)

    @app.task(resources={"gpu": 0.25})
    def small():
        return "ran"

    def gpu_capacity(w):
        return next((cap for name, cap, _ in w.native.resource_usage() if name == "gpu"), None)

    with running(app, threads=4, resources={"gpu": 1.0}, autotune="gpu", **MONITORED) as w:
        _eventually(lambda: gpu_capacity(w) == 0.5)  # the floor: the biggest task still fits
        holder = big.send()  # takes the whole 0.5
        assert started.acquire(timeout=10)
        waiting = small.send()
        _eventually(lambda: w.native.blocked("gpu") == 1)
        fake.set(utilization=0.1)  # the GPU is idle after all
        _eventually(lambda: gpu_capacity(w) >= 0.75)
        assert waiting.result(timeout=10) == "ran"  # admitted next to `big` once the capacity grew
        release.set()
        holder.result(timeout=10)


@thread_unsafe
def test_a_worker_that_under_declares_its_vram_is_warned(app, running, monkeypatch, caplog):
    monkeypatch.setattr(gpu_mod, "_backend", FakeGpus(card(used_gib=3.0, process_gib=3.0)))
    with caplog.at_level(logging.WARNING, logger="mayu.gpu"):
        with running(app, threads=1, resources={"vram_gb": 7.5}, **MONITORED):
            _eventually(lambda: "raise vram_gb= on them" in caplog.text)
    assert "use 3.0 GB of VRAM but its running tasks and loaded pools declare 0.0" in caplog.text


@thread_unsafe
def test_the_sampler_thread_follows_the_worker(app, running, monkeypatch):
    monkeypatch.setattr(gpu_mod, "_backend", FakeGpus(card()))
    with running(app, threads=1, **MONITORED) as w:
        assert w._sampler is not None and w._sampler._thread.is_alive()
        sampler = w._sampler
    assert not sampler._thread.is_alive()


@thread_unsafe
def test_monitoring_off_or_unavailable_starts_no_sampler(app, running, monkeypatch):
    fake = FakeGpus(card())
    monkeypatch.setattr(gpu_mod, "_backend", fake)
    with running(app, threads=1, gpu_monitor="off", **MONITORED) as w:
        assert w._sampler is None
        assert _series(w.metrics_port, f"mayu_gpu_utilization_ratio{LABELS}") is None
    assert fake.reads == 0
    monkeypatch.setattr(gpu_mod, "_backend", None)  # no NVML: auto turns itself off
    with running(app, threads=1, **MONITORED) as w:
        assert w._sampler is None


def test_monitor_off_never_imports_pynvml(redis_url, queue):
    # pynvml is installed in the dev environment, so a missing import proves the worker never asked for it
    script = textwrap.dedent("""
        import sys, threading
        from mayu import App
        from mayu.worker import WorkerRunner

        app = App(broker=sys.argv[1], default_queue=sys.argv[2])

        @app.task
        def noop(): ...

        w = WorkerRunner(app, threads=1, queues=[sys.argv[2]], gpu_monitor="off")
        w.start()
        names = sorted(t.name for t in threading.enumerate())
        w.stop()
        w.join()
        assert "mayu-gpu" not in names, names
        assert "pynvml" not in sys.modules
        assert "mayu.gpu" in sys.modules and not hasattr(sys.modules["mayu.gpu"], "pynvml")
        print("ok")
    """)
    r = subprocess.run([sys.executable, "-c", script, redis_url, queue], env=os.environ, capture_output=True,
                       text=True, timeout=60)
    assert r.returncode == 0 and r.stdout.strip() == "ok", r.stdout + r.stderr


def _real_gpu():
    try:
        return gpu_mod.Nvml()
    except Exception:
        return None


@pytest.mark.skipif(_real_gpu() is None, reason="needs the nvidia-ml-py extra and an NVIDIA GPU")
def test_the_real_device_reports_sane_values():
    nvml = _real_gpu()
    gpus = nvml.devices()
    if not gpus:
        pytest.skip("CUDA_VISIBLE_DEVICES hides every GPU")
    for g in gpus:
        assert g.uuid.startswith("GPU-") and g.name
        assert 0 < g.memory_total and 0 <= g.memory_used <= g.memory_total
        assert 0.0 <= g.utilization <= 1.0
        assert g.process_memory is None or 0 <= g.process_memory <= g.memory_used
        assert g.power_watts is None or g.power_watts > 0
        assert g.temperature_c is None or 0 < g.temperature_c < 150
    assert [g.index for g in gpus] == sorted(g.index for g in gpus)
