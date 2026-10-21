#!/usr/bin/env bash
# Builds .venv-bench (3.14 with the GIL: Celery, RQ, Dramatiq, streaQ, mayu process mode) and adds the
# bench client libraries to .venv (3.14t: mayu thread mode). mayu is a release build in each of them, and in
# .venv-gil and .venv-313 (the mixed-version run) when scripts/matrix.sh has created them.
# scripts/matrix.sh rebuilds debug builds into the test venvs, so run this again before benchmarking.
set -euo pipefail
cd "$(dirname "$0")/.."
[ -x .venv-bench/bin/python ] || uv venv --python 3.14+gil .venv-bench
uv pip install -q --python .venv-bench/bin/python -r bench/requirements.txt
uv pip install -q --python .venv/bin/python -r bench/requirements-ft.txt
for venv in .venv-bench .venv .venv-gil .venv-313; do
  [ -x "$venv/bin/python" ] || continue
  VIRTUAL_ENV=$PWD/$venv "$venv/bin/maturin" develop --release --uv -q
done
echo "bench environments ready"
