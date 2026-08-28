#!/bin/bash
# NYC Taxi Pipeline example: run the analysis job on a worker.
#
# Loads NYC taxi data from Parquet URL, runs parallel analysis tasks,
# and prints a summary report.
#
# Extra arguments are forwarded to `run-job`, so job kwargs are overridable:
#
# Usage: ./nyc_taxi_pipeline.sh
#        ./nyc_taxi_pipeline.sh --set limit=500000

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

PYTHON="${PYTHON:-uv run python}"

echo "## NYC Taxi Pipeline"
echo

$PYTHON -m aaiclick background start &
BACKGROUND_PID=$!
$PYTHON -m aaiclick execution-worker start &
WORKER_PID=$!
# Give the worker a chance to flush its output before the script exits.
stop_workers() {
    kill $WORKER_PID $BACKGROUND_PID 2>/dev/null || true
    wait $WORKER_PID $BACKGROUND_PID 2>/dev/null || true
}
trap stop_workers EXIT

# A dotted entrypoint needs no prior registration, and --progress blocks until
# the job is terminal, prints the task table on every change, and exits
# non-zero on failure — no job-id scraping, poll loop, or status branching.
$PYTHON -m aaiclick run-job nyc_taxi_pipeline.nyc_taxi_pipeline --progress --timeout 180 "$@"
