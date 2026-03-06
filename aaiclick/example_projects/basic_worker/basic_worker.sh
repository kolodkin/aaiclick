#!/bin/bash
# Basic worker example: register a job and run a worker to execute it.
#
# Expected output: prints every 0.5 seconds for 3 seconds (6 ticks total)
#
# Usage: ./basic_worker.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "=== Basic Worker Example ==="
echo

# Step 1: Register the job
echo "Registering job..."
uv run python "$SCRIPT_DIR/basic_worker_register.py"
echo

# Step 2: Start background cleanup worker
echo "Starting background cleanup worker..."
uv run python -m aaiclick background start &
BACKGROUND_PID=$!
echo "Background worker started (PID: $BACKGROUND_PID)"
echo

# Step 3: Start worker in background
echo "Starting worker..."
uv run python -m aaiclick worker start &
WORKER_PID=$!
echo "Worker started (PID: $WORKER_PID)"
echo

# Step 4: Wait for task execution
echo "Waiting 5 seconds for task execution..."
sleep 5

# Step 5: Stop workers
echo
echo "Stopping workers..."
kill $WORKER_PID 2>/dev/null || true
kill $BACKGROUND_PID 2>/dev/null || true

echo
echo "=== Example completed ==="
