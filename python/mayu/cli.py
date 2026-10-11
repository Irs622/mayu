import argparse
import importlib
import os
import sys

from .app import App


def load_app(path: str) -> App:
    module_name, sep, attr = path.partition(":")
    if not sep or not module_name or not attr:
        raise ValueError(f"app path must look like 'package.module:app', got {path!r}")
    if os.getcwd() not in sys.path:
        sys.path.insert(0, os.getcwd())
    obj = getattr(importlib.import_module(module_name), attr)
    if not isinstance(obj, App):
        raise TypeError(f"{path!r} is a {type(obj).__name__}, not a mayu.App")
    return obj


def _cmd_worker(args) -> int:
    from .worker import WorkerRunner

    try:
        app = load_app(args.app)
    except (ImportError, AttributeError, ValueError, TypeError) as e:
        print(f"mayu: cannot load app {args.app!r}: {e}", file=sys.stderr)
        return 2
    if getattr(sys, "_is_gil_enabled", lambda: True)():
        print("mayu: warning: the GIL is enabled; threads will not run Python code in parallel. "
              "Use a free-threaded build (python3.14t).", file=sys.stderr)
    queues = [q for q in args.queues.split(",") if q] if args.queues else None
    try:
        runner = WorkerRunner(app, threads=args.threads, queues=queues,
                              prefetch_multiplier=args.prefetch_multiplier,
                              shutdown_grace=args.shutdown_grace,
                              visibility_timeout=args.visibility_timeout,
                              heartbeat_interval=args.heartbeat_interval,
                              max_deliveries=args.max_deliveries,
                              max_zombie_threads=args.max_zombie_threads,
                              max_tasks_per_process=args.max_tasks_per_process,
                              max_memory_per_process=args.max_memory_per_process)
    except ValueError as e:
        print(f"mayu: {e}", file=sys.stderr)
        return 2
    reason = runner.run()
    if reason is not None:
        sys.stdout.flush()
        sys.stderr.flush()
        os.execv(sys.executable, [sys.executable, "-m", "mayu", *args.argv])
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="mayu")
    sub = parser.add_subparsers(dest="cmd", required=True)
    w = sub.add_parser("worker", help="run a worker process")
    w.add_argument("app", help="import path of the App, e.g. myproject.tasks:app")
    w.add_argument("--threads", type=int, default=os.cpu_count() or 4)
    w.add_argument("--queues", default=None, help="comma-separated; default: every queue the app's tasks use")
    w.add_argument("--prefetch-multiplier", type=int, default=2)
    w.add_argument("--shutdown-grace", type=float, default=30.0)
    w.add_argument("--visibility-timeout", type=float, default=30.0,
                   help="seconds before a silent worker's tasks are reclaimed by others")
    w.add_argument("--heartbeat-interval", type=float, default=5.0)
    w.add_argument("--max-deliveries", type=int, default=5,
                   help="dead-letter a task after this many crash-redeliveries")
    w.add_argument("--max-zombie-threads", type=int, default=4,
                   help="recycle the worker when more threads than this are stuck past their hard limit")
    w.add_argument("--max-tasks-per-process", type=int, default=None)
    w.add_argument("--max-memory-per-process", type=float, default=None, metavar="MB")
    w.set_defaults(func=_cmd_worker)
    argv = sys.argv[1:] if argv is None else list(argv)
    args = parser.parse_args(argv)
    args.argv = argv
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
