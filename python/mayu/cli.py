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


def _build_runner(app, args, threads: int):
    from .worker import WorkerRunner

    queues = [q for q in args.queues.split(",") if q] if args.queues else None
    return WorkerRunner(app, threads=threads, queues=queues,
                        prefetch_multiplier=args.prefetch_multiplier,
                        shutdown_grace=args.shutdown_grace,
                        visibility_timeout=args.visibility_timeout,
                        heartbeat_interval=args.heartbeat_interval,
                        max_deliveries=args.max_deliveries,
                        max_zombie_threads=args.max_zombie_threads,
                        max_tasks_per_process=args.max_tasks_per_process,
                        max_memory_per_process=args.max_memory_per_process,
                        metrics_port=args.metrics_port, metrics_host=args.metrics_host)


def _cmd_worker(args) -> int:
    from .modes import gil_enabled, import_app_detecting_gil, resolve_mode
    from .supervisor import RESTART_EXIT_CODE, ProcessSupervisor, watch_parent

    try:
        app, offenders = import_app_detecting_gil(args.app)
    except (ImportError, AttributeError, ValueError, TypeError) as e:
        print(f"mayu: cannot load app {args.app!r}: {e}", file=sys.stderr)
        return 2
    gil = gil_enabled()
    mode = resolve_mode(args.mode, gil)
    if mode == "process" and args.threads not in (None, 1):
        print("mayu: --threads applies to thread mode; process mode runs one task per process "
              "(use --processes)", file=sys.stderr)
        return 2
    threads = 1 if mode == "process" else (args.threads or os.cpu_count() or 4)
    processes = args.processes or ((os.cpu_count() or 2) if mode == "process" else 1)
    if processes < 1:
        print("mayu: --processes must be >= 1", file=sys.stderr)
        return 2
    if args.metrics_port is not None and not args._child and processes > 1:
        if args.metrics_port == 0:
            print("mayu: --metrics-port 0 cannot be used with several processes; pass a port",
                  file=sys.stderr)
            return 2
        if args.metrics_port + processes - 1 > 65535:
            print("mayu: --metrics-port + --processes exceeds 65535", file=sys.stderr)
            return 2

    if args._child:
        try:
            runner = _build_runner(app, args, threads)
        except ValueError as e:
            print(f"mayu: {e}", file=sys.stderr)
            return 2
        watch_parent(runner.stop)
        try:
            reason = runner.run()
        except (RuntimeError, ValueError) as e:
            print(f"mayu: cannot start worker: {e}", file=sys.stderr)
            return 1
        sys.stdout.flush()
        sys.stderr.flush()
        if reason is not None:
            os._exit(RESTART_EXIT_CODE)  # the supervisor replaces us; skip waiting on zombie threads
        return 0

    if args.mode == "auto" and mode == "process":
        why = (f"re-enabled by importing {', '.join(offenders)}" if offenders
               else "this Python build has a GIL")
        print(f"mayu: the GIL is enabled ({why}); using process mode with {processes} processes",
              file=sys.stderr, flush=True)
    elif mode == "thread" and gil:
        print("mayu: warning: the GIL is enabled; threads will not run Python code in parallel. "
              "Use a free-threaded build (python3.14t) or --mode process.", file=sys.stderr)

    if mode == "thread" and processes == 1:
        try:
            runner = _build_runner(app, args, threads)
        except ValueError as e:
            print(f"mayu: {e}", file=sys.stderr)
            return 2
        try:
            reason = runner.run()
        except (RuntimeError, ValueError) as e:
            print(f"mayu: cannot start worker: {e}", file=sys.stderr)
            return 1
        if reason is not None:
            sys.stdout.flush()
            sys.stderr.flush()
            os.execv(sys.executable, [sys.executable, "-m", "mayu", *args.argv])
        return 0

    try:
        _build_runner(app, args, threads)  # validate runner options; does not connect to Redis
    except ValueError as e:
        print(f"mayu: {e}", file=sys.stderr)
        return 2
    child = [sys.executable, "-m", "mayu", *args.argv, "--mode", mode, "--processes", "1", "--_child"]
    if mode == "process":
        child += ["--threads", "1", "--max-zombie-threads", "0"]
    per_child = None
    if args.metrics_port is not None:
        base = args.metrics_port
        per_child = lambda i: ["--metrics-port", str(base + i)]  # noqa: E731
    return ProcessSupervisor(child, processes, shutdown_grace=args.shutdown_grace,
                             per_child_args=per_child).run()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="mayu")
    sub = parser.add_subparsers(dest="cmd", required=True)
    w = sub.add_parser("worker", help="run a worker process")
    w.add_argument("app", help="import path of the App, e.g. myproject.tasks:app")
    w.add_argument("--threads", type=int, default=None,
                   help="threads per process (thread mode; default: CPU count)")
    w.add_argument("--mode", choices=("auto", "thread", "process"), default="auto",
                   help="auto: thread mode if the GIL is off after importing the app, else process mode")
    w.add_argument("--processes", type=int, default=None,
                   help="worker processes (default: 1 in thread mode, CPU count in process mode)")
    w.add_argument("--_child", action="store_true", help=argparse.SUPPRESS)
    w.add_argument("--queues", default=None, help="comma-separated, highest priority first; default: every queue the app's tasks use, at equal priority")
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
    w.add_argument("--metrics-port", type=int, default=os.environ.get("MAYU_METRICS_PORT") or None,
                   help="serve Prometheus metrics on this port (process i of --processes uses port+i)")
    w.add_argument("--metrics-host", default="0.0.0.0", help="address to serve metrics on")
    w.set_defaults(func=_cmd_worker)
    argv = sys.argv[1:] if argv is None else list(argv)
    args = parser.parse_args(argv)
    args.argv = argv
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
