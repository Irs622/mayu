#!/usr/bin/env bash
# The CI matrix, run locally (Linux legs only; macOS is not covered).
#   ft     3.14t thread mode           (.venv)
#   gil    3.14 process mode           (.venv-gil)
#   py313  3.13 process mode           (.venv-313)
# Usage: scripts/matrix.sh [leg...]   (default: every leg). Needs uv and Docker.
set -euo pipefail
cd "$(dirname "$0")/.."

ALL_LEGS=(ft gil py313)
DEPS=(maturin pytest msgpack opentelemetry-sdk)

venv_for() {
  case $1 in
    ft) echo .venv ;;
    gil) echo .venv-gil ;;
    py313) echo .venv-313 ;;
  esac
}

python_for() {
  case $1 in
    ft) echo 3.14t ;;
    gil) echo 3.14+gil ;;
    py313) echo 3.13 ;;
  esac
}

run_leg() {
  local v; v=$(venv_for "$1")
  case $1 in
    ft | gil | py313) "$v/bin/pytest" tests/python -q ;;
  esac
}

legs=("$@")
[ ${#legs[@]} -gt 0 ] || legs=("${ALL_LEGS[@]}")
for leg in "${legs[@]}"; do
  [ -n "$(venv_for "$leg")" ] || { echo "unknown leg: $leg (legs: ${ALL_LEGS[*]})" >&2; exit 2; }
done

docker exec mayu-redis redis-cli ping >/dev/null 2>&1 || scripts/redis-up.sh

failed=()
for leg in "${legs[@]}"; do
  v=$(venv_for "$leg")
  echo "== $leg ($(python_for "$leg"), $v)"
  [ -x "$v/bin/python" ] || uv venv --python "$(python_for "$leg")" "$v"
  uv pip install -q --python "$v/bin/python" "${DEPS[@]}"
  VIRTUAL_ENV=$PWD/$v "$v/bin/maturin" develop --uv -q
  run_leg "$leg" || failed+=("$leg")
done

if [ ${#failed[@]} -gt 0 ]; then
  echo "failed legs: ${failed[*]}" >&2
  exit 1
fi
echo "all legs passed: ${legs[*]}"
