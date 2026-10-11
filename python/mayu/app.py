import os
import threading

from . import _native
from .serializers import PICKLE, get_serializer
from .task import Task


class App:
    def __init__(self, broker: str | None = None, *, result_ttl: int | None = None,
                 default_queue: str = "default", serializer: str = "msgpack", max_retries: int = 3):
        self.broker_url = os.environ.get("MAYU_BROKER") or broker
        if not self.broker_url:
            raise ValueError("no broker URL: pass broker= or set MAYU_BROKER")
        env_ttl = os.environ.get("MAYU_RESULT_TTL")
        if env_ttl is not None:
            result_ttl = int(env_ttl) or None
        if result_ttl is not None and result_ttl <= 0:
            raise ValueError("result_ttl must be a positive number of seconds")
        self.result_ttl = result_ttl
        self.default_queue = os.environ.get("MAYU_DEFAULT_QUEUE") or default_queue
        self.serializer = get_serializer(os.environ.get("MAYU_SERIALIZER") or serializer)
        self.allow_pickle = self.serializer.content_type == PICKLE
        self.max_retries = max_retries
        self.registry: dict[str, Task] = {}
        self._client = None
        self._client_lock = threading.Lock()

    @property
    def client(self) -> "_native.Client":
        if self._client is None:
            with self._client_lock:
                if self._client is None:
                    self._client = _native.Client(self.broker_url)
        return self._client

    def queues(self) -> list[str]:
        return sorted({self.default_queue, *(t.queue for t in self.registry.values())})

    def task(self, fn=None, /, *, name: str | None = None, queue: str | None = None,
             max_retries: int | None = None, retry_on=(),
             soft_time_limit: float | None = None, hard_time_limit: float | None = None, threadsafe: bool = True,
             rate_limit=None):
        def register(f):
            t = Task(
                self, f,
                name=name or f"{f.__module__}.{f.__qualname__}",
                queue=queue or self.default_queue,
                max_retries=self.max_retries if max_retries is None else max_retries,
                retry_on=tuple(retry_on),
                soft_time_limit=soft_time_limit,
                hard_time_limit=hard_time_limit,
                threadsafe=threadsafe,
                rate_limit=rate_limit,
            )
            if t.name in self.registry:
                raise ValueError(f"task {t.name!r} is already registered")
            self.registry[t.name] = t
            return t

        return register(fn) if fn is not None else register
