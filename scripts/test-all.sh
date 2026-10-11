#!/usr/bin/env bash
# Full verification: Rust, both Python builds, reconnect and chaos suites. Needs Docker.
set -euo pipefail
cd "$(dirname "$0")/.."
docker exec mayu-redis redis-cli ping >/dev/null 2>&1 || scripts/redis-up.sh
cargo test --workspace
cargo test -p mayu-redis --test reconnect -- --ignored
VIRTUAL_ENV=$PWD/.venv .venv/bin/maturin develop --uv
.venv/bin/pytest -q
if [ -x .venv-gil/bin/python ]; then
  VIRTUAL_ENV=$PWD/.venv-gil .venv-gil/bin/maturin develop --uv
  .venv-gil/bin/pytest tests/python -q
else
  echo "skipping GIL build: .venv-gil missing (see README)" >&2
fi
.venv/bin/pytest tests/chaos -q
