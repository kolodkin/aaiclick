#!/usr/bin/env bash
# chdb Benchmark — local mode: aaiclick's Object API vs native chdb SQL on an
# in-memory chdb session. No servers.
#
# Usage: ./chdb_benchmark.sh [--rows N] [--runs N]
set -euo pipefail
cd "$(dirname "$0")"
PYTHON="${PYTHON:-uv run python}"

export AAICLICK_CH_URL="chdb://:memory:"
$PYTHON -m chdb_benchmark "$@"
