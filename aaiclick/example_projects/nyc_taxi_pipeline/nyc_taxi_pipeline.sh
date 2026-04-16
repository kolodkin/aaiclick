#!/bin/bash
# NYC Taxi Pipeline example: register a job and run a worker to execute it.
#
# Loads NYC taxi data from Parquet URL, runs parallel analysis tasks,
# and prints a summary report.
#
# Usage: ./nyc_taxi_pipeline.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

PYTHON="${PYTHON:-uv run python}"

echo "=== NYC Taxi Pipeline ==="
echo

# Step 1: Register the job and capture its ID
echo "Registering job..."
REGISTER_OUTPUT=$($PYTHON -m nyc_taxi_pipeline)
echo "$REGISTER_OUTPUT"
JOB_ID=$(echo "$REGISTER_OUTPUT" | grep -oP 'ID: \K[0-9]+')
echo

# Step 2: Start background cleanup worker
echo "Starting background cleanup worker..."
$PYTHON -m aaiclick background start &
BACKGROUND_PID=$!
echo "Background worker started (PID: $BACKGROUND_PID)"
echo

# Step 3: Start worker in background
echo "Starting worker..."
$PYTHON -m aaiclick worker start &
WORKER_PID=$!
echo "Worker started (PID: $WORKER_PID)"
echo

# Step 4: Poll job status until completed or failed
echo "Waiting for pipeline execution..."
MAX_WAIT=180
ELAPSED=0
while [ $ELAPSED -lt $MAX_WAIT ]; do
    sleep 5
    ELAPSED=$((ELAPSED + 5))
    JOB_STATUS=$($PYTHON -m aaiclick job get "$JOB_ID" 2>/dev/null | grep "Status:" | awk '{print $2}')
    if [ "$JOB_STATUS" = "COMPLETED" ] || [ "$JOB_STATUS" = "FAILED" ]; then
        break
    fi
done
echo

# Step 5: Show job stats
echo "Job stats:"
$PYTHON -m aaiclick job stats "$JOB_ID"
echo

# Step 6: Stop workers
echo "Stopping workers..."
kill $WORKER_PID 2>/dev/null || true
kill $BACKGROUND_PID 2>/dev/null || true
wait $WORKER_PID 2>/dev/null || true
wait $BACKGROUND_PID 2>/dev/null || true

echo
if [ "$JOB_STATUS" = "COMPLETED" ]; then
    echo "=== Pipeline completed successfully ==="
elif [ "$JOB_STATUS" = "FAILED" ]; then
    echo "=== Pipeline FAILED ==="
    exit 1
else
    echo "=== Pipeline timed out (status: ${JOB_STATUS:-unknown}) ==="
    exit 1
fi
