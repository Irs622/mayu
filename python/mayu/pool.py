"""Model pools: a fixed number of instances of an expensive object, built once per worker and lent to tasks."""
import dataclasses
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


class Pool:
    """A pool made with ``app.pool``; ``@pool.close`` registers the function that unloads an instance."""

    def __init__(self, app, name: str, factory, resources: dict[str, float], discard_on: tuple):
        if not callable(factory):
            raise TypeError(f"the factory of pool {name!r} must be callable, got {factory!r}")
        self.app = app
        self.name = name
        self.factory = factory
        self.resources = resources
        self.discard_on = discard_on
        self.closer = None
        self._lock = threading.Lock()

    def __repr__(self) -> str:
        return f"<Pool {self.name!r}>"

    def close(self, fn):
        """Registers ``fn(instance)``, called when an instance is discarded and when the worker stops."""
        if not callable(fn):
            raise TypeError(f"the close function of pool {self.name!r} must be callable, got {fn!r}")
        with self._lock:
            if self.closer is not None:
                raise ValueError(f"pool {self.name!r} already has a close function")
            self.closer = fn
        return fn
