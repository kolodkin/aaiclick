#!/usr/bin/env bash
# NYC Taxi Pipeline — distributed mode: ClickHouse + PostgreSQL servers at
# AAICLICK_CH_URL / AAICLICK_SQL_URL, with a worker executing the tasks.
#
# Usage: ./nyc_taxi_pipeline.sh [--set KEY=VALUE ...]
set -euo pipefail
cd "$(dirname "$0")"
PYTHON="${PYTHON:-uv run python}"

: "${AAICLICK_CH_URL:?set to a ClickHouse server, e.g. clickhouse://default:pass@localhost:8123/default}"
: "${AAICLICK_SQL_URL:?set to PostgreSQL, e.g. postgresql+asyncpg://aaiclick:secret@localhost:5432/aaiclick}"
$PYTHON -m aaiclick migrate upgrade head

$PYTHON -m aaiclick background start & BG_PID=$!
$PYTHON -m aaiclick execution-worker start & WORKER_PID=$!
trap 'kill $WORKER_PID $BG_PID 2>/dev/null || true; wait 2>/dev/null || true' EXIT

$PYTHON -m aaiclick run-job nyc_taxi_pipeline.nyc_taxi_pipeline --progress --timeout 180 "$@"
