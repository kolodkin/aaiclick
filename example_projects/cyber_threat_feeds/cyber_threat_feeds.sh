#!/bin/bash
# Cyber Threat Feeds Pipeline example: load CISA KEV + Shodan CVEDB + FIRST
# EPSS, analyze vulnerabilities, and produce a threat intelligence report.
#
# Extra arguments are forwarded to `run-job`, so job kwargs are overridable:
#
# Usage: ./cyber_threat_feeds.sh
#        ./cyber_threat_feeds.sh --set shodan_limit=1000

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

PYTHON="${PYTHON:-uv run python}"

WORKER_LOG="tmp/cyber_threat_worker.log"
export AAICLICK_REPORT_FILE="tmp/cyber_threat_report.md"
mkdir -p tmp

echo "## Cyber Threat Feeds Pipeline"
echo

$PYTHON -m aaiclick background start &
BACKGROUND_PID=$!
$PYTHON -m aaiclick execution-worker start > "$WORKER_LOG" 2>&1 &
WORKER_PID=$!

# Runs on EXIT so the worker log and report are shown for a failed run too,
# while `set -e` still propagates run-job's exit code.
finish() {
    kill $WORKER_PID $BACKGROUND_PID 2>/dev/null || true
    wait $WORKER_PID $BACKGROUND_PID 2>/dev/null || true
    echo
    echo "### Worker Log"
    echo
    cat "$WORKER_LOG"
    echo
    echo "### Threat Report Output"
    echo
    cat "$AAICLICK_REPORT_FILE" 2>/dev/null || echo "(no report produced)"
}
trap finish EXIT

# A dotted entrypoint needs no prior registration, and --progress blocks until
# the job is terminal, prints the task table on every change, and exits
# non-zero on failure — no job-id scraping, poll loop, or status branching.
$PYTHON -m aaiclick run-job cyber_threat_feeds.cyber_threat_pipeline --progress --timeout 300 "$@"
