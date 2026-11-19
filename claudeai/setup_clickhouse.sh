#!/usr/bin/env bash
# Ensure a local ClickHouse server is running on :8123 with the CI credentials.
# Reuses one already up; otherwise downloads the static binary into
# .cache/clickhouse/ (once) and starts it. `stop` stops it. Safe to rerun.
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$here/_guard.sh"
# The server's S3 identity is the AWS pair env.sh exports (MinIO's), lent to
# queries by users.d/s3.xml.
. "$here/env.sh"

cache="$here/../.cache"
dir="$cache/clickhouse"
pidfile="$dir/clickhouse.pid"
up() { curl -fs localhost:8123/ping >/dev/null; }

if [ "${1:-}" = "stop" ]; then
  if [ -f "$pidfile" ] && pid="$(cat "$pidfile")" && kill "$pid" 2>/dev/null; then
    # Wait for the exit, so a start right after does not see the dying server as up.
    for _ in $(seq 1 30); do kill -0 "$pid" 2>/dev/null || break; sleep 1; done
  fi
  rm -f "$pidfile"
  echo "[clickhouse] stopped"
  exit 0
fi

if up; then
  echo "[clickhouse] already up on :8123"
  exit 0
fi

mkdir -p "$dir/users.d"
if [ ! -x "$dir/clickhouse" ]; then
  # The release branch the CI images use (clickhouse/clickhouse-server:26.10 in
  # .github/workflows/_test-reusable.yaml); bump the two together.
  version="${CLICKHOUSE_VERSION:-26.10}"
  # Same build selection as the clickhouse.com installer: the default amd64
  # build needs AVX2, older CPUs get the compat build.
  build=amd64
  grep -q avx2 /proc/cpuinfo || build=amd64compat
  curl -fL --progress-bar -o "$dir/clickhouse.tmp" "https://builds.clickhouse.com/$version/$build/clickhouse"
  chmod +x "$dir/clickhouse.tmp"
  mv "$dir/clickhouse.tmp" "$dir/clickhouse"
  echo "[clickhouse] installed $("$dir/clickhouse" --version)"
fi

# Minimal server config with the CI credentials, laid out like the stock
# image: users.xml plus users.d/, which gets the same S3 profile file the CI
# blob job mounts (docker/clickhouse/users.d/s3.xml).
cp "$here/../docker/clickhouse/users.d/s3.xml" "$dir/users.d/s3.xml"
cat >"$dir/config.xml" <<XML
<clickhouse>
  <logger><level>warning</level><console>1</console></logger>
  <listen_host>127.0.0.1</listen_host>
  <http_port>8123</http_port>
  <tcp_port>9000</tcp_port>
  <path>$dir/data/</path>
  <user_directories><users_xml><path>users.xml</path></users_xml></user_directories>
</clickhouse>
XML
cat >"$dir/users.xml" <<XML
<clickhouse>
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
nohup "$dir/clickhouse" server --config-file="$dir/config.xml" --pid-file="$pidfile" \
  >"$cache/clickhouse.log" 2>&1 &
disown
for _ in $(seq 1 60); do up && break; sleep 1; done
up || { echo "[clickhouse] error: not ready; see $cache/clickhouse.log" >&2; exit 1; }
echo "[clickhouse] up on :8123"
