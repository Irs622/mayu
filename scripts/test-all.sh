#!/usr/bin/env bash
# Full verification: Rust, the local CI matrix (scripts/matrix.sh), reconnect and chaos suites. Needs Docker and uv.
set -euo pipefail
cd "$(dirname "$0")/.."
docker exec mayu-redis redis-cli ping >/dev/null 2>&1 || scripts/redis-up.sh
cargo test --workspace
cargo test -p mayu-redis --test reconnect -- --ignored
scripts/matrix.sh
.venv/bin/pytest tests/chaos -q
