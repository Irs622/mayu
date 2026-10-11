#!/usr/bin/env bash
# Builds .venv-bench (3.14 with the GIL: Celery, RQ, Dramatiq, streaQ, mayu process mode) and adds the
# bench client libraries to .venv (3.14t: mayu thread mode).
set -euo pipefail
cd "$(dirname "$0")/.."
[ -x .venv-bench/bin/python ] || uv venv --python 3.14+gil .venv-bench
uv pip install -q --python .venv-bench/bin/python -r bench/requirements.txt
VIRTUAL_ENV=$PWD/.venv-bench .venv-bench/bin/maturin develop --uv -q
uv pip install -q --python .venv/bin/python -r bench/requirements-ft.txt
echo "bench environments ready"
