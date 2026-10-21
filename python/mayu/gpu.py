"""NVIDIA GPU monitoring: sizes `--resources`, feeds the GPU metrics and keeps the VRAM ledger honest.

Needs the optional `gpu` extra (`nvidia-ml-py`). This is the only module that imports `pynvml`, and only inside
functions, so a worker that doesn't monitor GPUs never loads it.
"""
import dataclasses
import logging
import math
import os
import sys
import threading
import time
from collections.abc import Callable, Sequence
from typing import Protocol

log = logging.getLogger("mayu.gpu")

GIB = 2**30
AUTO_HEADROOM_GB = 0.5  # left out of `vram_gb=auto` per device: the CUDA context and the desktop
GUARD_SETTLE = 3  # ticks a smaller VRAM cut must hold before the guard gives capacity back
UNDER_TICKS = 3  # consecutive over-margin ticks before the under-declared warning
UNDER_MARGIN_GIB = 0.5
UNDER_MARGIN_RATIO = 0.10
UNDER_EVERY = 300.0  # seconds between two under-declared warnings
WARN_EVERY = 60.0  # seconds between two logs of the same sampler error
AUTO_RESOURCES = ("gpu", "vram_gb")
TUNE_WINDOW = 5  # ticks per tuning decision (10 s at the default interval)
TUNE_HIGH = 0.90  # mean utilization at or above which the GPU is saturated: more concurrency only adds latency
TUNE_LOW = 0.70  # mean utilization below which the GPU has room, if tasks are waiting for it
TUNE_COOLDOWN = 6  # windows after a shrink before the tuner may grow again, so it can't flap between two steps
TUNE_MEMORY = 300.0  # seconds a level's measured utilization stays trusted; the workload may change after that
VRAM_PRESSURE = 0.95  # share of the cards' memory in use above which the tuner won't add concurrency


@dataclasses.dataclass
class Gpu:
    """One visible device. `index` is NVML's; `process_memory` is what this worker's process tree uses (bytes), None
    when NVML can't attribute it."""

    index: int
    uuid: str
    name: str
    memory_total: int
    memory_used: int
    utilization: float  # 0-1
    power_watts: float | None = None
    temperature_c: float | None = None
    process_memory: int | None = None


class Backend(Protocol):
    def devices(self) -> list[Gpu]: ...


def visible(spec: str | None, available: Sequence[tuple[int, str]]) -> list[int]:
    """NVML indexes `CUDA_VISIBLE_DEVICES=spec` makes visible, in its order; `available` is [(index, uuid), ...].

    Integer ids match NVML's order, which is CUDA's only with CUDA_DEVICE_ORDER=PCI_BUS_ID. As in CUDA, the list
    ends at the first id that names no device."""
    if spec is None:
        return [i for i, _ in available]
    if spec.strip() in ("", "-1"):
        return []
    out: list[int] = []
    for item in (s.strip() for s in spec.split(",")):
        index = _match(item, spec, available)
        if index is None or index in out:
            break
        out.append(index)
    return out


def _match(item: str, spec: str, available: Sequence[tuple[int, str]]) -> int | None:
    if item == "-1":
        return None
    if item.isascii() and item.isdigit():
        return int(item) if any(i == int(item) for i, _ in available) else None
    if item.upper().startswith("GPU-"):
        found = [i for i, uuid in available if uuid.upper().startswith(item.upper())]
        if len(found) > 1:
            raise ValueError(f"CUDA_VISIBLE_DEVICES={spec} names GPU {item!r}, a prefix of more than one device; "
                             f"use a longer UUID prefix or the device index")
        return found[0] if found else None
    raise ValueError(f"CUDA_VISIBLE_DEVICES={spec} is not supported by mayu's GPU monitoring; use GPU indexes "
                     f"or GPU-... UUIDs, or turn monitoring off with --gpu-monitor off")


def _process_tree(pid: int) -> set[int]:
    """`pid` and its descendants (Linux, via /proc); elsewhere just `pid`."""
    if not sys.platform.startswith("linux"):
        return {pid}
    children: dict[int, list[int]] = {}
    try:
        names = os.listdir("/proc")
    except OSError:
        return {pid}
    for name in names:
        if not name.isdigit():
            continue
        try:
            with open(f"/proc/{name}/stat", "rb") as f:
                data = f.read()
            ppid = int(data[data.rindex(b")") + 1:].split()[1])
        except (OSError, ValueError, IndexError):
            continue  # the process is gone, or isn't ours to read
        children.setdefault(ppid, []).append(int(name))
    tree, todo = set(), [pid]
    while todo:
        p = todo.pop()
        if p not in tree:
            tree.add(p)
            todo.extend(children.get(p, ()))
    return tree


class Nvml:
    """The real backend. Raises if `pynvml` is missing or NVML can't initialise."""

    def __init__(self):
        import pynvml

        pynvml.nvmlInit()  # once; NVML stays initialised for the life of the process
        self._nvml = pynvml
        self._all = []  # (index, handle, uuid, name)
        for i in range(pynvml.nvmlDeviceGetCount()):
            handle = pynvml.nvmlDeviceGetHandleByIndex(i)
            self._all.append((i, handle, _text(pynvml.nvmlDeviceGetUUID(handle)),
                              _text(pynvml.nvmlDeviceGetName(handle))))

    def devices(self) -> list[Gpu]:
        nv = self._nvml
        by_index = {i: (handle, uuid, name) for i, handle, uuid, name in self._all}
        shown = visible(os.environ.get("CUDA_VISIBLE_DEVICES"), [(i, uuid) for i, _, uuid, _ in self._all])
        out, listed = [], {}
        for i in shown:
            handle, uuid, name = by_index[i]
            mem = nv.nvmlDeviceGetMemoryInfo(handle)
            gpu = Gpu(i, uuid, name, int(mem.total), int(mem.used),
                      nv.nvmlDeviceGetUtilizationRates(handle).gpu / 100.0,
                      self._optional(lambda: nv.nvmlDeviceGetPowerUsage(handle) / 1000.0),
                      self._optional(lambda: float(nv.nvmlDeviceGetTemperature(handle, nv.NVML_TEMPERATURE_GPU))))
            listed[i] = self._processes(handle)
            out.append(gpu)
        if any(procs for procs in listed.values()):  # scanning /proc is the costly part: only when there is a need
            tree = _process_tree(os.getpid())
            for gpu in out:
                gpu.process_memory = self._ours(listed[gpu.index], tree)
        else:
            for gpu in out:
                gpu.process_memory = 0 if listed[gpu.index] is not None else None
        return out

    def _optional(self, read: Callable[[], float]) -> float | None:
        try:
            return read()
        except self._nvml.NVMLError:
            return None  # not supported on this device or driver

    def _processes(self, handle) -> dict[int, int | None] | None:
        """pid -> bytes (None when unattributable) over compute and graphics processes; None if neither is readable."""
        nv, out, readable = self._nvml, {}, False
        for read in (nv.nvmlDeviceGetComputeRunningProcesses, nv.nvmlDeviceGetGraphicsRunningProcesses):
            try:
                procs = read(handle)
            except nv.NVMLError:
                continue
            readable = True
            for p in procs:
                used = p.usedGpuMemory
                out[p.pid] = None if used is None or used >= 2**63 else int(used)  # dedup by pid
        return out if readable else None

    @staticmethod
    def _ours(procs: dict[int, int | None] | None, tree: set[int]) -> int | None:
        if procs is None:
            return None
        mine = [used for pid, used in procs.items() if pid in tree]
        return None if any(used is None for used in mine) else sum(mine)


def _text(value) -> str:
    return value.decode() if isinstance(value, bytes) else str(value)


_backend: Backend | None = None
_backend_lock = threading.Lock()


def _create_nvml() -> Backend | None:
    try:
        return Nvml()
    except Exception as e:  # ImportError, NVMLError, OSError: any of them means "no GPU monitoring here"
        log.debug("NVML is not available: %r", e)
        return None


def backend() -> Backend | None:
    """The process-wide backend, created on first use; None when `pynvml` can't be imported or NVML won't start."""
    global _backend
    with _backend_lock:
        if _backend is None:
            _backend = _create_nvml()
        return _backend


def _tenth_up(x: float) -> float:
    return math.ceil(round(x * 10, 6)) / 10


def _tenth_down(x: float) -> float:
    return math.floor(round(x * 10, 6)) / 10


def has_auto(resources) -> bool:
    return isinstance(resources, dict) and any(isinstance(v, str) and v == "auto" for v in resources.values())


def resolve_auto(resources: dict) -> dict:
    """Replaces `"auto"` for `gpu` and `vram_gb` with what the visible GPUs have; other entries pass through."""
    wanted = [n for n, v in resources.items() if isinstance(v, str) and v == "auto"]
    if not wanted:
        return resources
    for name in wanted:
        if name not in AUTO_RESOURCES:
            raise ValueError(f"{name}=auto is not supported: only {' and '.join(AUTO_RESOURCES)} can be 'auto'; "
                             f"give {name} a number")
    be = backend()
    if be is None:
        raise ValueError(f"{wanted[0]}=auto needs NVIDIA's NVML: pip install 'mayu[gpu]'")
    try:
        gpus = be.devices()
    except Exception as e:
        raise ValueError(f"{wanted[0]}=auto could not read the GPUs: {e!r}; give it a number instead") from e
    if not gpus:
        raise ValueError(f"{wanted[0]}=auto found no visible GPU (CUDA_VISIBLE_DEVICES="
                         f"{os.environ.get('CUDA_VISIBLE_DEVICES', '<unset>')}); give it a number instead")
    out = dict(resources)
    if "gpu" in wanted:
        out["gpu"] = float(len(gpus))
    if "vram_gb" in wanted:
        vram = _tenth_down(sum(g.memory_total for g in gpus) / GIB - AUTO_HEADROOM_GB * len(gpus))
        if vram <= 0:
            raise ValueError(f"vram_gb=auto: the visible GPUs have too little memory to leave {AUTO_HEADROOM_GB} GiB "
                             f"each; give vram_gb a number instead")
        out["vram_gb"] = vram
    return out


class Guard:
    """How much of the VRAM capacity to withhold because other processes use the card.

    `slack` is what the GPUs have beyond the declared `vram_gb`; foreign use within it costs nothing. A bigger cut
    applies at once, a smaller one only after GUARD_SETTLE ticks in a row asked for less, so a flapping desktop
    app can't thrash admission. Never withholds more than `limit`."""

    def __init__(self, slack_gib: float, limit_gib: float | None = None):
        self.slack = slack_gib
        self.limit = limit_gib
        self.withheld = 0.0
        self._lower: list[float] = []

    def observe(self, foreign_gib: float) -> float:
        cut = _tenth_up(max(0.0, foreign_gib - self.slack))
        if self.limit is not None:
            cut = min(cut, self.limit)
        if cut >= self.withheld:
            self.withheld = cut
            self._lower.clear()
        else:
            self._lower.append(cut)
            if len(self._lower) >= GUARD_SETTLE:
                self.withheld = max(self._lower[-GUARD_SETTLE:])
                self._lower.clear()
        return self.withheld


class UnderDeclared:
    """Decides when to warn that this worker's processes use more VRAM than the tasks and pools declare."""

    def __init__(self, clock: Callable[[], float] = time.monotonic):
        self._clock = clock
        self._count = 0
        self._last: float | None = None

    def observe(self, used_gib: float, declared_gib: float) -> bool:
        if used_gib <= declared_gib + max(UNDER_MARGIN_GIB, UNDER_MARGIN_RATIO * declared_gib):
            self._count = 0
            return False
        self._count += 1
        if self._count < UNDER_TICKS:
            return False
        now = self._clock()
        if self._last is not None and now - self._last < UNDER_EVERY:
            return False
        self._last = now
        return True


class Tuner:
    """Adaptive concurrency for the `gpu` resource: how much capacity to add or remove after each window of ticks.

    `ceiling` is the configured capacity (where it starts), `step` the smallest task request and `floor` the largest
    (the biggest task must still fit). Capacity is tracked in thousandths, like the dispatcher's, so repeated steps
    can't drift. A saturated GPU shrinks only if one step less was also seen saturated (or never measured): bursty
    tasks can leave the GPU 60% busy one at a time and 100% two at a time, and two then get more done. After a
    shrink it waits `cooldown` windows before growing, so a shrink to a level not measured yet can't flap."""

    def __init__(self, ceiling: float, floor: float, step: float, cooldown: int = TUNE_COOLDOWN,
                 clock: Callable[[], float] = time.monotonic):
        self._ceiling, self._floor, self._step = round(ceiling * 1000), round(floor * 1000), round(step * 1000)
        if self._step < 1 or self._floor < 1 or self._floor > self._ceiling:
            raise ValueError(f"tuner needs 0 < step, 0 < floor <= ceiling, got ceiling={ceiling} floor={floor} "
                             f"step={step}")
        self._cap = self._ceiling
        self._cooldown = cooldown
        self._wait = 0  # windows left before growth is allowed again
        self._clock = clock
        self._seen: dict[int, tuple[float, float]] = {}  # level -> (window mean, when) while tasks were waiting
        self._utils: list[float] = []
        self._pressure = False
        self.window_mean = 0.0  # the mean utilization the last decision was based on

    @property
    def capacity(self) -> float:
        return self._cap / 1000

    @property
    def closes_window(self) -> bool:
        """True when the next `observe` ends a window, i.e. is the only one that needs `blocked`."""
        return len(self._utils) == TUNE_WINDOW - 1

    def observe(self, util: float, blocked: int, vram_pressure: bool) -> float:
        """One tick's mean utilization (0-1), the number of due tasks waiting on `gpu` and whether VRAM is tight.
        Returns the capacity change to apply, 0.0 except at the end of a window."""
        self._utils.append(util)
        self._pressure = self._pressure or vram_pressure
        if len(self._utils) < TUNE_WINDOW:
            return 0.0
        self.window_mean = mean = sum(self._utils) / len(self._utils)
        pressure = self._pressure
        self._utils.clear()  # one decision per window, whether or not it changes anything
        self._pressure = False
        cooling, self._wait = self._wait > 0, max(0, self._wait - 1)
        now = self._clock()
        if blocked > 0:  # the level was used in full, so its mean says what it can do
            self._seen[self._cap] = (mean, now)
        below = self._seen.get(self._cap - self._step)
        below_idle = below is not None and below[0] < TUNE_HIGH and now - below[1] <= TUNE_MEMORY
        if mean >= TUNE_HIGH and self._cap - self._step >= self._floor and not below_idle:
            delta = -self._step
            self._wait = self._cooldown
        elif (mean < TUNE_LOW and blocked > 0 and not pressure and not cooling
              and self._cap + self._step <= self._ceiling):
            delta = self._step
        else:
            return 0.0
        self._cap += delta
        return delta / 1000

    def revert(self, delta: float) -> None:
        """Takes back a change `observe` returned that the worker could not apply."""
        self._cap -= round(delta * 1000)


class Sampler:
    """The `mayu-gpu` thread. Every `interval` seconds: read the GPUs, push them to the metrics, run the VRAM guard
    and the under-declared check, then the tuner hook. All its state lives on that thread (tests call `tick`)."""

    def __init__(self, native, be: Backend, interval: float, vram_gb: float | None = None,
                 clock: Callable[[], float] = time.monotonic, tuner: Tuner | None = None):
        self.native = native
        self.backend = be
        self.interval = interval
        self.vram_gb = vram_gb or None  # the configured capacity; the guard and the warning need a declared amount
        self._clock = clock
        self._tuner = tuner
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._guard: Guard | None = None
        self._guard_off = False
        self._under = UnderDeclared(clock)
        self._withheld = 0.0  # what the guard has taken out of the native capacity
        self._warned: dict[str, float] = {}

    def start(self) -> None:
        self._thread = threading.Thread(target=self._run, name="mayu-gpu", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()

    def join(self, timeout: float = 5.0) -> None:
        if self._thread is not None:
            self._thread.join(timeout)

    def _run(self) -> None:
        while True:
            self.tick()
            if self._stop.wait(self.interval):
                return

    def tick(self) -> None:
        try:
            gpus = self.backend.devices()
        except Exception as e:
            self._warn("read", "could not read the GPUs, skipping this sample: %r", e)
            return
        for step in (self._push, self._guard_step, self._under_declared_step, self._tune):
            try:
                step(gpus)
            except Exception as e:
                self._warn(step.__name__, "GPU sampler step %s failed: %r", step.__name__.strip("_"), e)

    def _warn(self, key: str, message: str, *args) -> None:
        """Logs at most once a minute per `key`, so a broken NVML can't flood the log."""
        now = self._clock()
        last = self._warned.get(key)
        if last is None or now - last >= WARN_EVERY:
            self._warned[key] = now
            log.warning(message, *args)

    def _push(self, gpus: list[Gpu]) -> None:
        self.native.set_gpu_stats([(g.index, g.uuid, g.utilization, g.memory_used, g.memory_total, g.process_memory,
                                    g.power_watts, g.temperature_c) for g in gpus])

    def _guard_step(self, gpus: list[Gpu]) -> None:
        if self.vram_gb is None or self._guard_off or not gpus:
            return
        if any(g.process_memory is None for g in gpus):
            self._guard_off = True
            log.warning("NVML cannot tell which VRAM this worker uses, so the VRAM guard is off; "
                        "other processes' GPU memory is not subtracted from vram_gb")
            self._apply(0.0, quiet=True)  # don't keep a cut the guard can no longer revise
            return
        if self._guard is None:
            total = sum(g.memory_total for g in gpus) / GIB
            self._guard = Guard(max(0.0, total - self.vram_gb), self.vram_gb)
        foreign = max(0, sum(g.memory_used - g.process_memory for g in gpus)) / GIB
        self._apply(self._guard.observe(foreign), foreign)

    def _apply(self, target: float, foreign: float = 0.0, quiet: bool = False) -> None:
        delta = self._withheld - target  # positive gives capacity back
        if round(delta * 1000) == 0:
            return
        try:
            self.native.adjust_capacity("vram_gb", delta)
        except Exception:
            if self._guard is not None:
                self._guard.withheld = self._withheld  # the change didn't happen; the guard must not think it did
            raise
        self._withheld = target
        admits = self.vram_gb - target
        if quiet:
            return
        if target > 0:
            log.info("other processes use %.1f GB of VRAM; this worker now admits up to %.1f", foreign, admits,
                     extra={"foreign_vram_gb": round(foreign, 1), "vram_gb": admits})
        else:
            log.info("other processes' VRAM use is back within what the GPUs have to spare; this worker admits "
                     "up to %.1f again", admits, extra={"foreign_vram_gb": round(foreign, 1), "vram_gb": admits})

    def _under_declared_step(self, gpus: list[Gpu]) -> None:
        if self.vram_gb is None or not gpus or any(g.process_memory is None for g in gpus):
            return
        used = sum(g.process_memory for g in gpus) / GIB
        declared = next((in_use for name, _, in_use in self.native.resource_usage() if name == "vram_gb"), 0.0)
        if self._under.observe(used, declared):
            log.warning("this worker's processes use %.1f GB of VRAM but its running tasks and loaded pools declare "
                        "%.1f; raise vram_gb= on them or the GPU may run out of memory", used, declared)

    def _tune(self, gpus: list[Gpu]) -> None:
        """Runs after the guard, so it sees what the guard withheld."""
        tuner = self._tuner
        if tuner is None or not gpus:
            return
        util = sum(g.utilization for g in gpus) / len(gpus)
        total = sum(g.memory_total for g in gpus)
        pressure = self._withheld > 0 or (total > 0 and sum(g.memory_used for g in gpus) >= VRAM_PRESSURE * total)
        blocked = self.native.blocked("gpu") if tuner.closes_window else 0  # it costs a pass over the held tasks
        delta = tuner.observe(util, blocked, pressure)
        if delta == 0:
            return
        try:
            self.native.adjust_capacity("gpu", delta)
        except Exception:
            tuner.revert(delta)  # the change didn't happen; the tuner must not think it did
            raise
        busy, window = round(tuner.window_mean * 100), round(TUNE_WINDOW * self.interval)
        was, now = tuner.capacity - delta, tuner.capacity
        extra = {"gpu_utilization": round(tuner.window_mean, 2), "gpu_capacity": now, "blocked": blocked}
        if delta < 0:
            log.info("GPU busy %d%% over the last %d s; running fewer GPU tasks: gpu capacity %.4g \u2192 %.4g",
                     busy, window, was, now, extra=extra)
        else:
            log.info("GPU only %d%% busy over the last %d s with %d tasks waiting; running more GPU tasks: "
                     "gpu capacity %.4g \u2192 %.4g", busy, window, blocked, was, now, extra=extra)
