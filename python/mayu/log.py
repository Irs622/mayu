"""Logging for mayu: text or JSON lines on stderr, the same shape for Python and the Rust core."""
import datetime as dt
import json
import logging
import sys

from . import _native

FORMATS = ("text", "json")
LEVELS = ("DEBUG", "INFO", "WARNING", "ERROR")
_RUST_LEVEL = {"DEBUG": "debug", "INFO": "info", "WARNING": "warn", "ERROR": "error"}
# Attributes every LogRecord has; anything else on a record came from `extra=`.
_STANDARD = frozenset(vars(logging.LogRecord("", 0, "", 0, "", (), None))) | {"message", "asctime"}

logger = logging.getLogger("mayu")
_configured: tuple[str, str] | None = None  # (format, level) of the last configure_logging, for env children


class JsonFormatter(logging.Formatter):
    """One JSON object per line, with the keys the Rust core uses (`WARNING` is spelled `WARN`)."""

    def format(self, record: logging.LogRecord) -> str:
        ts = dt.datetime.fromtimestamp(record.created, dt.UTC)
        out = {
            "timestamp": ts.isoformat(timespec="microseconds").replace("+00:00", "Z"),
            "level": "WARN" if record.levelname == "WARNING" else record.levelname,
            "message": record.getMessage(),
            "target": record.name,
        }
        for k, v in vars(record).items():
            if k not in _STANDARD and not k.startswith("_"):
                out[k] = v
        if record.exc_info:
            out["exception"] = self.formatException(record.exc_info)
        return json.dumps(out, default=str)


class _TextFormatter(logging.Formatter):
    def __init__(self) -> None:
        super().__init__("mayu: %(message)s")


def configure_logging(format: str = "text", level: str = "INFO") -> None:
    """Sends mayu's logs (Python and Rust) to stderr. The worker CLI calls this; call it yourself
    when running `WorkerRunner` from your own code. Repeated calls replace mayu's handler; the Rust
    side keeps the first configuration."""
    level = level.upper()
    if format not in FORMATS:
        raise ValueError(f"log format must be one of {FORMATS}, got {format!r}")
    if level not in LEVELS:
        raise ValueError(f"log level must be one of {LEVELS}, got {level!r}")
    for h in list(logger.handlers):
        if getattr(h, "_mayu", False):
            logger.removeHandler(h)
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(JsonFormatter() if format == "json" else _TextFormatter())
    handler._mayu = True
    logger.addHandler(handler)
    logger.setLevel(level)
    logger.propagate = False
    _native.init_logging(format, _RUST_LEVEL[level])
    global _configured
    _configured = (format, level)
