#!/bin/bash
# chdb vs aaiclick benchmark: compare native chdb SQL against aaiclick Object API.
#
# Measures ingest, aggregations, group-by, filter, sort on 1M rows (10 runs).
# Requires: pip install rich (or see requirements.txt)
#
# Usage: ./chdb_benchmark.sh
#        ./chdb_benchmark.sh --rows 500000 --runs 5

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

PYTHON="${PYTHON:-uv run python}"

echo "=== chdb vs aaiclick Benchmark ==="
echo

# Use in-memory chdb session for fair comparison with native benchmark
export AAICLICK_CH_URL="chdb://:memory:"

$PYTHON -m chdb_benchmark "$@"
