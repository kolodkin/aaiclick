#!/bin/bash
# Basic worker example: run a job on a worker and follow its progress.
#
# Expected output: prints every 0.5 seconds for 3 seconds (6 ticks total)
#
# Usage: ./basic_worker.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

PYTHON="${PYTHON:-uv run python}"

echo "## Basic Worker Example"
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
# non-zero on failure — no job-id scraping, poll loop, or fixed sleep.
$PYTHON -m aaiclick run-job basic_worker.periodic_print_job --progress --timeout 60
