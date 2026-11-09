#!/usr/bin/env bash
# Start (or `stop`) the local distributed-backend services the CI test jobs
# use: ClickHouse on :8123 and PostgreSQL on :5432, then apply migrations.
# Binaries/clusters come from claudeai/setup_*.sh (run here if missing);
# processes do not survive a container resume, so run this each session.
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$here/_guard.sh"
. "$here/env.sh"

cache="$here/../.cache"
ch_bin="$cache/clickhouse/clickhouse"
pg_bin="$(ls -d /usr/lib/postgresql/*/bin | sort -V | tail -1)"
pg_dir="$cache/postgres"
export PGPASSWORD=secret

stop() {
  runuser -u postgres -- "$pg_bin/pg_ctl" -D "$pg_dir/data" -m fast stop 2>/dev/null || true
  pkill -f "clickhouse server --config-file" || true
  echo "[run] all services stopped"
}

case "${1:-start}" in
  stop) stop; exit 0 ;;
  start) ;;
  *) echo "usage: $0 [start|stop]" >&2; exit 2 ;;
esac

[ -x "$ch_bin" ] || "$here/setup_clickhouse.sh"
[ -f "$pg_dir/data/PG_VERSION" ] || "$here/setup_postgres.sh"

if curl -fs localhost:8123/ping >/dev/null; then
  echo "[clickhouse] already up"
else
  # Minimal server config: the CI credentials, data under .cache/clickhouse/data.
  cat >"$cache/clickhouse/config.xml" <<'XML'
<clickhouse>
  <logger><level>warning</level><console>1</console></logger>
  <listen_host>127.0.0.1</listen_host>
  <http_port>8123</http_port>
  <tcp_port>9000</tcp_port>
  <path>data/</path>
  <profiles><default/></profiles>
  <quotas><default/></quotas>
  <users>
    <default>
      <password>click123</password>
      <networks><ip>::/0</ip></networks>
      <profile>default</profile>
      <quota>default</quota>
    </default>
  </users>
</clickhouse>
XML
  (cd "$cache/clickhouse" && nohup ./clickhouse server --config-file=config.xml >"$cache/clickhouse.log" 2>&1 &)
  for _ in $(seq 1 60); do curl -fs localhost:8123/ping >/dev/null && break; sleep 1; done
  curl -fs localhost:8123/ping >/dev/null || { echo "[clickhouse] error: not ready; see $cache/clickhouse.log" >&2; exit 1; }
fi
echo "[clickhouse] up on :8123"

if "$pg_bin/pg_isready" -h localhost -q; then
  echo "[postgres] already up"
else
  runuser -u postgres -- "$pg_bin/pg_ctl" -D "$pg_dir/data" -l "$pg_dir/postgres.log" -o "-k $pg_dir" start >/dev/null \
    || { echo "[postgres] error: failed to start; see $pg_dir/postgres.log" >&2; exit 1; }
fi
psql -h localhost -U aaiclick -d postgres -tAc "SELECT 1 FROM pg_database WHERE datname = 'aaiclick'" | grep -q 1 \
  || "$pg_bin/createdb" -h localhost -U aaiclick aaiclick
echo "[postgres] up on :5432"

(cd "$here/.." && uv run --frozen --extra distributed python -m aaiclick migrate upgrade head >"$cache/migrate.log" 2>&1) \
  || { echo "[migrate] error: see $cache/migrate.log" >&2; exit 1; }
echo "[migrate] schema at head"
echo "[run] all services up — source claudeai/env.sh to point tests at them"
