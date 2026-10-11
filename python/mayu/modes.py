"""Choosing between thread mode (free-threaded Python) and process mode (GIL builds)."""
import re
import sys
import warnings

from .cli import load_app

# CPython: "The global interpreter lock (GIL) has been enabled to load module 'X', which has not
# declared that it can run safely without the GIL. ..."
GIL_WARNING = re.compile(r"enabled to load module '([^']+)'")


def gil_enabled() -> bool:
    return getattr(sys, "_is_gil_enabled", lambda: True)()


def import_app_detecting_gil(path: str):
    """Imports the app; returns it and the modules whose import re-enabled the GIL."""
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always", RuntimeWarning)
        app = load_app(path)
    offenders = []
    for w in caught:
        m = GIL_WARNING.search(str(w.message))
        if m and m.group(1) not in offenders:
            offenders.append(m.group(1))
    return app, offenders


def resolve_mode(requested: str, gil_on: bool) -> str:
    if requested in ("thread", "process"):
        return requested
    return "process" if gil_on else "thread"
