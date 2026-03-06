#!/bin/bash
# NYC Taxi Pipeline example: register a job and run a worker to execute it.
#
# Loads NYC taxi data from Parquet URL, runs parallel analysis tasks,
# and prints a summary report.
#
# Usage: ./nyc_taxi_pipeline.sh

set -e

echo "=== NYC Taxi Pipeline ==="
echo

# Step 1: Register the job
echo "Registering job..."
uv run python -m aaiclick.example_projects.nyc_taxi_pipeline
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
# Pipeline has 10 tasks including URL data loading and parallel analysis
echo "Waiting for pipeline execution..."
sleep 120

# Step 5: Stop workers
echo
echo "Stopping workers..."
kill $WORKER_PID 2>/dev/null || true
kill $BACKGROUND_PID 2>/dev/null || true

echo
echo "=== Pipeline completed ==="
