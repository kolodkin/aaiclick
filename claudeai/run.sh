#!/usr/bin/env bash
# Start (or `stop`) the local distributed-backend services the CI test jobs
# use: ClickHouse on :8123, PostgreSQL on :5432 and MinIO (S3) on :9100, then
# apply migrations. Each claudeai/setup_<svc>.sh reuses a running server,
# installs under .cache/ when missing, then starts it. Processes do not
# survive a container resume, so run this each session.
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$here/_guard.sh"
. "$here/env.sh"

# One entry per claudeai/setup_<svc>.sh; stopped in reverse order.
services=(clickhouse postgres minio)
cache="$here/../.cache"

case "${1:-start}" in
  stop)
    for ((i = ${#services[@]} - 1; i >= 0; i--)); do "$here/setup_${services[i]}.sh" stop || true; done
    echo "[run] all services stopped"
    exit 0
    ;;
  start) ;;
  *) echo "usage: $0 [start|stop]" >&2; exit 2 ;;
esac

for svc in "${services[@]}"; do "$here/setup_$svc.sh"; done

mkdir -p "$cache"

(cd "$here/.." && uv run --frozen python -m aaiclick migrate upgrade head >"$cache/migrate.log" 2>&1) \
  || { echo "[migrate] error: see $cache/migrate.log" >&2; exit 1; }
echo "[migrate] schema at head"
echo "[run] all services up — source claudeai/env.sh to point tests at them"
