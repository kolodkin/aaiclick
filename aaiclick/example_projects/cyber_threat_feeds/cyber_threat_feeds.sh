#!/bin/bash
# Cyber Threat Feeds Pipeline example: load CISA KEV + Shodan CVEDB,
# analyze vulnerabilities, and produce a threat intelligence report.
#
# Usage: ./cyber_threat_feeds.sh

set -e

REPORT_LOG="tmp/cyber_threat_report.log"
mkdir -p tmp

echo "=== Cyber Threat Feeds Pipeline ==="
echo

# Step 1: Register the job and capture its ID
echo "Registering job..."
REGISTER_OUTPUT=$(uv run python -m aaiclick.example_projects.cyber_threat_feeds)
echo "$REGISTER_OUTPUT"
JOB_ID=$(echo "$REGISTER_OUTPUT" | grep -oP 'ID: \K[0-9]+')
echo

# Step 2: Start background cleanup worker
echo "Starting background cleanup worker..."
uv run python -m aaiclick background start &
BACKGROUND_PID=$!
echo "Background worker started (PID: $BACKGROUND_PID)"
echo

# Step 3: Start worker in background, capturing output to log file
echo "Starting worker..."
uv run python -m aaiclick worker start > "$REPORT_LOG" 2>&1 &
WORKER_PID=$!
echo "Worker started (PID: $WORKER_PID)"
echo

# Step 4: Poll job status until completed or failed
echo "Waiting for pipeline execution..."
MAX_WAIT=300
ELAPSED=0
while [ $ELAPSED -lt $MAX_WAIT ]; do
    sleep 5
    ELAPSED=$((ELAPSED + 5))
    JOB_STATUS=$(uv run python -m aaiclick job get "$JOB_ID" 2>/dev/null | grep "Status:" | awk '{print $2}')
    if [ "$JOB_STATUS" = "COMPLETED" ] || [ "$JOB_STATUS" = "FAILED" ]; then
        break
    fi
done
echo

# Step 5: Show job stats
echo "Job stats:"
uv run python -m aaiclick job stats "$JOB_ID"
echo

# Step 6: Stop workers
echo "Stopping workers..."
kill $WORKER_PID 2>/dev/null || true
kill $BACKGROUND_PID 2>/dev/null || true
wait $WORKER_PID 2>/dev/null || true
wait $BACKGROUND_PID 2>/dev/null || true

# Step 7: Display report from log
echo
echo "=== Threat Report Output ==="
cat "$REPORT_LOG"

# Step 8: Write to GitHub Actions step summary if available
if [ -n "$GITHUB_STEP_SUMMARY" ]; then
    cat "$REPORT_LOG" >> "$GITHUB_STEP_SUMMARY"
fi

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
