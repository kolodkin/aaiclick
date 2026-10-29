#!/usr/bin/env bash
# Cyber Threat Feeds — local mode: runs the whole job in this process on
# embedded chdb + SQLite under ~/.aaiclick. No servers, no workers.
#
# Usage: ./cyber_threat_feeds.sh [--params '{"shodan_limit": 1000}']
set -euo pipefail
cd "$(dirname "$0")"
PYTHON="${PYTHON:-uv run python}"

unset AAICLICK_CH_URL AAICLICK_SQL_URL
$PYTHON -m aaiclick setup >/dev/null
$PYTHON -m cyber_threat_feeds "$@"
