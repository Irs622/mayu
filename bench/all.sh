#!/usr/bin/env bash
# Every system x workload, REPEATS times, then bench/RESULTS.md. Needs Docker and bench/setup.sh.
# Env: REPEATS (3), NOOP_N (20000), REAL_N (5000), CONCURRENCY (64), SYSTEMS (all six, space-separated).
set -euo pipefail
cd "$(dirname "$0")/.."
REPEATS=${REPEATS:-3}
NOOP_N=${NOOP_N:-20000}
REAL_N=${REAL_N:-5000}
CONCURRENCY=${CONCURRENCY:-64}
SYSTEMS=${SYSTEMS:-"mayu-thread mayu-process celery rq dramatiq streaq"}

docker exec mayu-redis redis-cli ping >/dev/null 2>&1 || scripts/redis-up.sh
mkdir -p bench/results
out=bench/results/latest.jsonl
: > "$out"

py_for() { if [ "$1" = mayu-thread ]; then echo .venv/bin/python; else echo .venv-bench/bin/python; fi; }

for workload in noop realistic; do
  n=$NOOP_N
  [ "$workload" = realistic ] && n=$REAL_N
  for system in $SYSTEMS; do
    for i in $(seq "$REPEATS"); do
      echo "== $system $workload ($i/$REPEATS, n=$n)" >&2
      "$(py_for "$system")" bench/run.py "$system" "$workload" --n "$n" --concurrency "$CONCURRENCY" --out "$out" >/dev/null
    done
  done
done
.venv-bench/bin/python bench/report.py "$out" > bench/RESULTS.md
echo "wrote bench/RESULTS.md" >&2
