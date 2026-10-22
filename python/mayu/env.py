"""Per-task environments: ``app.env(...)`` declares an interpreter that runs some tasks in a child process.

This module holds what the worker (parent) and ``mayu.runner`` (child) share: `Env` and the wire protocol, which is
length-prefixed msgpack over two pipes. Messages are maps with a type in ``t``:

  child -> parent  hello (protocol, mayu, python, gil, pid), ready, fatal (message), done (id, o, ...),
                   progress (id, done, total), opdone (id, ok, error)
  parent -> child  start (store_result, allow_pickle), run (id, task, ct, payload, pools, tp), op (id, op, pool,
                   index, size), stop

The child only hosts pool instances, keyed by ``(pool, index)``; the worker keeps all the bookkeeping and drives
every step. ``op`` is ``build`` or ``stage`` (with ``size``), or ``close``, ``park`` or ``unpark``, and is answered
by ``opdone``. ``run`` names the instances the task uses as ``pools`` = ``{param: [pool, index]}`` and carries the
parent's traceparent ``tp`` (or None). ``done`` carries ``o``: ``ok`` (result, links), ``retry`` (ms, error) or
``fail`` (error, traceback), and ``broken``, the params whose instance the task broke.
"""
import dataclasses
import importlib.metadata
import os
import platform
import struct
import sys

import msgpack

from .pool import _BAD_NAME

PROTOCOL = 2
_LEN = struct.Struct(">I")


def check_env_name(name) -> str:
    """Env names are parsed from ``--envs legacy=/py313/bin/python``, so they cannot hold ``,``, ``=`` or whitespace."""
    if not isinstance(name, str):
        raise TypeError(f"env name must be a string, got {type(name).__name__}")
    if not name or _BAD_NAME.search(name):
        raise ValueError(f"bad env name {name!r}: use a non-empty name without ',', '=' or whitespace")
    return name


@dataclasses.dataclass(frozen=True)
class Env:
    """An interpreter that runs the tasks and pools declared with ``env=``; see ``App.env``."""

    name: str
    python: str  # this host's default; ``--envs`` overrides it per worker
    imports: tuple[str, ...] = ()  # modules the child imports after the app

    def __post_init__(self):
        check_env_name(self.name)
        if not isinstance(self.python, str) or not self.python.strip():
            raise ValueError(f"env {self.name!r} needs python=, the path of the interpreter that runs its tasks, "
                             f"like python='/opt/venvs/py313/bin/python'")
        if isinstance(self.imports, str) or not all(isinstance(m, str) and m for m in self.imports):
            raise TypeError(f"imports of env {self.name!r} must be a list of module names like ['myproj.legacy'], "
                            f"got {self.imports!r}")
        object.__setattr__(self, "imports", tuple(self.imports))

    def __repr__(self) -> str:
        return f"<Env {self.name!r} python={self.python!r}>"


def env_name(app, env) -> str | None:
    """The name an ``env=`` option stands for: an `Env` of this app or the name of one (resolved at worker start)."""
    if env is None:
        return None
    if isinstance(env, Env):
        if app.envs.get(env.name) is not env:
            raise ValueError(f"env {env.name!r} was not made by this app's app.env(...)")
        return env.name
    if isinstance(env, str):
        return check_env_name(env)
    raise TypeError(f"env must be an Env from app.env(...) or its name, got {type(env).__name__}")


# -- wire ----------------------------------------------------------------------------------------------------------


def write_frame(f, msg: dict) -> None:
    data = msgpack.packb(msg, use_bin_type=True)
    f.write(_LEN.pack(len(data)) + data)
    f.flush()


def read_frame(f) -> dict | None:
    """The next message, or None at EOF (a frame cut short counts as EOF: the peer died mid-write)."""
    head = f.read(_LEN.size)
    if len(head) < _LEN.size:
        return None
    (n,) = _LEN.unpack(head)
    data = f.read(n)
    if len(data) < n:
        return None
    return msgpack.unpackb(data, raw=False)


def hello() -> dict:
    return {"t": "hello", "protocol": PROTOCOL, "mayu": importlib.metadata.version("mayu"),
            "python": platform.python_version(), "gil": bool(getattr(sys, "_is_gil_enabled", lambda: True)()),
            "pid": os.getpid()}


def _minor(version: str) -> str:
    return ".".join(version.split(".")[:2])


def check_hello(theirs: dict, mine: dict | None = None) -> str | None:
    """Why the child cannot serve this parent, or None. Python may differ (that is the point); mayu may not."""
    mine = hello() if mine is None else mine
    if theirs.get("protocol") != mine["protocol"]:
        return (f"the env's mayu speaks protocol {theirs.get('protocol')} but this worker speaks {mine['protocol']}; "
                f"install the same mayu ({mine['mayu']}) in the env's interpreter")
    if _minor(str(theirs.get("mayu"))) != _minor(mine["mayu"]):
        return (f"the env's interpreter has mayu {theirs.get('mayu')} but this worker runs mayu {mine['mayu']}; "
                f"install the same major.minor version in both")
    return None
