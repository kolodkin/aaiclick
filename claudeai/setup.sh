#!/usr/bin/env bash
# Install the local distributed-backend services (ClickHouse binary, PostgreSQL
# cluster) under .cache/. One-time per container; claudeai/run.sh starts them.
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$here/_guard.sh"

"$here/setup_clickhouse.sh"
"$here/setup_postgres.sh"
