# Changelog

All notable changes to mayu are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses
[Semantic Versioning](https://semver.org/).

## [Unreleased]

## [0.1.0] - 2026-10-12

First public release.

- Redis broker with at-least-once delivery, crash recovery, poison-task detection, dead letters and replay.
- Thread mode for free-threaded CPython 3.14t and process mode for GIL builds, with a supervising parent.
- Retries with backoff, countdown/ETA, soft and hard time limits, rate limits, queue priority and `link=` callbacks.
- Sync and `async def` tasks.
- Prometheus metrics, JSON logging, OpenTelemetry tracing and the `mayu inspect` / `mayu dead` CLI.
- Benchmarks against Celery, RQ, Dramatiq and streaQ.

[Unreleased]: https://github.com/Siddid-Soni/mayu/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/Siddid-Soni/mayu/releases/tag/v0.1.0
