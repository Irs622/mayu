import datetime as dt
import os
import threading

from . import _native
from .env import Env, env_name
from .handle import TaskHandle
from .pool import Pool, Use, check_discard_on, check_linger, check_pool_name
from .serializers import PICKLE, get_serializer
from .task import Task, _enqueue, check_resources
from .workflow import Workflow

_TASK_OPTIONS = frozenset({"name", "queue", "max_retries", "retry_on", "soft_time_limit", "hard_time_limit",
                           "threadsafe", "rate_limit", "resources", "env"})


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
        self.pools: dict[str, Pool] = {}
        self.envs: dict[str, Env] = {}
        self._client = None
        self._client_lock = threading.Lock()
        self._registry_lock = threading.Lock()

    @property
    def client(self) -> "_native.Client":
        if self._client is None:
            with self._client_lock:
                if self._client is None:
                    self._client = _native.Client(self.broker_url)
        return self._client

    def queues(self, *, implemented: bool = False) -> list[str]:
        """Every queue the app uses, or with ``implemented=True`` only those whose tasks this process can run."""
        if not implemented:
            return sorted({self.default_queue, *(t.queue for t in self.registry.values())})
        # A queue holding a stub belongs to the workers that implement it, the default queue included. A task with
        # an env is implemented in the env's interpreter, so a stub of it here is as good as the code.
        stubbed = {t.queue for t in self.registry.values() if not (t.implemented or t.env is not None)}
        runnable = (t.queue for t in self.registry.values() if t.implemented or t.env is not None)
        return sorted({self.default_queue, *runnable} - stubbed)

    def task(self, fn=None, /, *, name: str | None = None, queue: str | None = None,
             max_retries: int | None = None, retry_on=(),
             soft_time_limit: float | None = None, hard_time_limit: float | None = None, threadsafe: bool = True,
             rate_limit=None, resources=None, env=None):
        options = dict(name=name, queue=queue, max_retries=max_retries, retry_on=retry_on,
                       soft_time_limit=soft_time_limit, hard_time_limit=hard_time_limit, threadsafe=threadsafe,
                       rate_limit=rate_limit, resources=resources, env=env)
        return self._register(fn, options, implemented=True)

    def declare(self, fn=None, /, **options):
        """Register a task's name, options and signature without its code; attach it later with ``implement``."""
        return self._register(fn, options, implemented=False)

    def _register(self, fn, options: dict, implemented: bool):
        def register(f):
            t = Task(
                self, f,
                name=options.get("name") or f"{f.__module__}.{f.__qualname__}",
                queue=options.get("queue") or self.default_queue,
                max_retries=self.max_retries if options.get("max_retries") is None else options["max_retries"],
                retry_on=tuple(options.get("retry_on", ())),
                soft_time_limit=options.get("soft_time_limit"),
                hard_time_limit=options.get("hard_time_limit"),
                threadsafe=options.get("threadsafe", True),
                rate_limit=options.get("rate_limit"),
                resources=options.get("resources"),
                env=options.get("env"),
                implemented=implemented,
            )
            with self._registry_lock:
                if t.name in self.registry:
                    raise ValueError(f"task {t.name!r} is already registered")
                self.registry[t.name] = t
            return t

        unknown = set(options) - _TASK_OPTIONS
        if unknown:
            raise TypeError(f"unknown task option(s) {sorted(unknown)}; valid options: {sorted(_TASK_OPTIONS)}")
        options["env"] = env_name(self, options.get("env"))  # checked here, not when the decorator is applied
        return register(fn) if fn is not None else register

    def env(self, name: str, /, *, python: str, imports: list[str] = ()) -> Env:
        """Declares an interpreter that runs the tasks and pools made with ``env=`` in a child process.

        ``python`` is this host's default path (``--envs`` overrides it per worker); the child imports the app, then
        each module in ``imports`` (for implementations whose libraries only exist in that interpreter)."""
        env = Env(name, python, imports)
        with self._registry_lock:
            if name in self.envs:
                raise ValueError(f"env {name!r} is already registered")
            self.envs[name] = env
        return env

    def pool(self, name: str, /, *, discard_on=(), linger: float | None = None, parked: dict | None = None,
             env=None, **resources):
        """Decorator registering ``fn(ctx)`` as the factory of a pool; resources are per instance.

        ``linger`` (lazy mode only) keeps an idle instance for that many seconds while a running workflow still
        has a node waiting for it. ``parked`` (lazy mode only) is what one parked instance holds, like
        ``{"ram_gb": 6.6}``: instead of closing an idle instance to make room, the worker moves it there with the
        pool's ``@park`` hook and moves it back with ``@unpark``; ``@stage`` can build one straight into that state.
        Eager mode ignores ``linger`` and ``parked``. With ``env=`` the factory runs in that env's child process
        (eager mode only), never in the worker."""
        check_pool_name(name)
        linger = check_linger(linger)
        if parked is not None:
            parked = check_resources(f"parked of pool {name!r}", parked)
            if not parked:
                raise ValueError(f"parked of pool {name!r} names no resource; give what a parked instance holds, "
                                 f"like parked={{'ram_gb': 6.6}}, or leave it out")
        resources = check_resources(f"resources of pool {name!r}", resources)
        discard_on = check_discard_on(discard_on)
        env = env_name(self, env)
        if env is not None and (linger is not None or parked is not None):
            raise ValueError(f"pool {name!r} is in env {env!r}, whose pools are eager only; "
                             f"linger and parked are for lazy pools")

        def register(factory):
            pool = Pool(self, name, factory, resources, discard_on, linger, parked, env)
            with self._registry_lock:
                if name in self.pools:
                    raise ValueError(f"pool {name!r} is already registered")
                self.pools[name] = pool
            return pool

        return register

    def use(self, name: str) -> Use:
        """A task parameter's default that is filled with an instance of the pool ``name`` on the worker."""
        return Use(name)

    def send(self, name: str, /, *args, countdown: float | None = None, eta: dt.datetime | None = None,
             queue: str | None = None, link=None, **kwargs) -> TaskHandle:
        """Enqueue a task by name; if it is registered here its arguments are validated, otherwise they are not."""
        task = self.registry.get(name)
        if task is not None:
            return task.send(*args, countdown=countdown, eta=eta, queue=queue, link=link, **kwargs)
        return _enqueue(self, name, queue or self.default_queue, self.max_retries, args, kwargs, countdown, eta, link)

    def workflow(self, name: str, *, ttl: float = 7 * 86400) -> Workflow:
        """A builder for a DAG of tasks sent as one unit; its state expires ``ttl`` seconds after the last progress."""
        return Workflow(self, name, ttl)
