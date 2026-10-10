#!/usr/bin/env bash
# Starts a throwaway Redis for tests on port 6390.
set -euo pipefail
docker rm -f mayu-redis >/dev/null 2>&1 || true
docker run -d --rm --name mayu-redis -p 6390:6379 redis:8 >/dev/null
until docker exec mayu-redis redis-cli ping 2>/dev/null | grep -q PONG; do sleep 0.2; done
echo "redis ready at redis://127.0.0.1:6390/0"
