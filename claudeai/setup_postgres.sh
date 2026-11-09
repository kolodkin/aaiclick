#!/usr/bin/env bash
# Init a PostgreSQL cluster under .cache/postgres/data (idempotent). Uses the
# system postgres binaries; initdb refuses root, so it runs as `postgres`.
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$here/_guard.sh"

pg_bin="$(ls -d /usr/lib/postgresql/*/bin | sort -V | tail -1)"
dir="$here/../.cache/postgres"
if [ -f "$dir/data/PG_VERSION" ]; then
  echo "[postgres] cluster present: $dir/data"
  exit 0
fi
mkdir -p "$dir"
chown postgres: "$dir"
echo secret > "$dir/pwfile"
chmod 644 "$dir/pwfile"
runuser -u postgres -- "$pg_bin/initdb" -D "$dir/data" -U aaiclick --pwfile="$dir/pwfile" -A scram-sha-256 >"$dir/initdb.log"
rm "$dir/pwfile"
echo "[postgres] cluster initialized at $dir/data"
