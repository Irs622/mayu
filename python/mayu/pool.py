"""Model pools: a fixed number of instances of an expensive object, built once per worker and lent to tasks."""
import dataclasses
import math
import re
import threading

_BAD_NAME = re.compile(r"[,=\s]")


def check_pool_name(name) -> str:
    """Pool names are parsed from ``--pools gemma=2,tts=3``, so they cannot hold ``,``, ``=`` or whitespace."""
    if not isinstance(name, str):
        raise TypeError(f"pool name must be a string, got {type(name).__name__}")
    if not name or _BAD_NAME.search(name):
        raise ValueError(f"bad pool name {name!r}: use a non-empty name without ',', '=' or whitespace")
    return name


class Use:
    """A task parameter's default that asks for an instance of the named pool; see ``App.use``."""

    __slots__ = ("name",)

    def __init__(self, name: str):
        self.name = check_pool_name(name)

    def __repr__(self) -> str:
        return f"app.use({self.name!r})"

    # Equal by name so that ``implement`` accepts a declaration and an implementation that both say app.use("x").
    def __eq__(self, other):
        return self.name == other.name if isinstance(other, Use) else NotImplemented

    def __hash__(self) -> int:
        return hash((Use, self.name))


@dataclasses.dataclass(frozen=True)
class PoolContext:
    """What a pool's factory is told about the instance it builds."""

    name: str
    index: int  # 0 <= index < size; a rebuilt instance keeps the index of the one it replaces
    size: int


def check_discard_on(discard_on) -> tuple:
    classes = discard_on if isinstance(discard_on, tuple) else (discard_on,)
    if not all(isinstance(c, type) and issubclass(c, BaseException) for c in classes):
        raise TypeError(f"discard_on must be an exception class or a tuple of them, got {discard_on!r}")
    return classes


def check_linger(linger) -> float | None:
    """`linger=` of a lazy pool: seconds an idle instance is kept while a workflow still needs it, or None."""
    if linger is None:
        return None
    if isinstance(linger, bool) or not isinstance(linger, (int, float)):
        raise TypeError(f"linger must be a number of seconds like 30, or None; got {linger!r}")
    if math.isnan(linger) or math.isinf(linger):
        raise ValueError(f"linger must be a finite number of seconds above 0, or None; got {linger!r}")
    if linger <= 0:
        raise ValueError(f"linger must be above 0 seconds (use None to turn it off); got {linger!r}")
    return float(linger)


class Pool:
    """A pool made with ``app.pool``; ``@pool.close`` registers the function that unloads an instance.

    ``@pool.park`` / ``@pool.unpark`` / ``@pool.stage`` register the hooks of a pool declared with ``parked=``
    (lazy mode only)."""

    def __init__(self, app, name: str, factory, resources: dict[str, float], discard_on: tuple,
                 linger: float | None = None, parked: dict[str, float] | None = None):
        if not callable(factory):
            raise TypeError(f"the factory of pool {name!r} must be callable, got {factory!r}")
        self.app = app
        self.name = name
        self.factory = factory
        self.resources = resources
        self.discard_on = discard_on
        self.linger = linger  # lazy mode only
        self.parked = parked  # lazy mode only: what one parked instance holds, or None if the pool cannot park
        self.closer = None
        self.parker = None
        self.unparker = None
        self.stager = None
        self._lock = threading.Lock()

    def __repr__(self) -> str:
        return f"<Pool {self.name!r}>"

    def close(self, fn):
        """Registers ``fn(instance)``, called when an instance is discarded and when the worker stops.

        On a pool with ``parked=`` it also gets instances in the parked state, so it must handle both."""
        return self._register("closer", "close", fn)

    def park(self, fn):
        """Registers ``fn(instance)``, which moves an idle instance out of its scarce memory (say GPU to host RAM)."""
        return self._register("parker", "park", fn)

    def unpark(self, fn):
        """Registers ``fn(instance)``, which moves a parked instance back, in place."""
        return self._register("unparker", "unpark", fn)

    def stage(self, fn):
        """Registers ``fn(ctx)``, which builds an instance straight into the parked state (it must not use the GPU)."""
        return self._register("stager", "stage", fn)

    def _register(self, attr: str, what: str, fn):
        if not callable(fn):
            raise TypeError(f"the {what} function of pool {self.name!r} must be callable, got {fn!r}")
        with self._lock:
            if getattr(self, attr) is not None:
                raise ValueError(f"pool {self.name!r} already has a {what} function")
            setattr(self, attr, fn)
        return fn
