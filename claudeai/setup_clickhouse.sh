#!/usr/bin/env bash
# Download the static ClickHouse binary into .cache/clickhouse/ (idempotent).
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$here/_guard.sh"

dir="$here/../.cache/clickhouse"
if [ -x "$dir/clickhouse" ]; then
  echo "[clickhouse] binary present: $dir/clickhouse"
  exit 0
fi
mkdir -p "$dir"
# Same build selection as the clickhouse.com installer: the default amd64
# build needs AVX2, older CPUs get the compat build.
build=amd64
grep -q avx2 /proc/cpuinfo || build=amd64compat
curl -fL --progress-bar -o "$dir/clickhouse.tmp" "https://builds.clickhouse.com/master/$build/clickhouse"
chmod +x "$dir/clickhouse.tmp"
mv "$dir/clickhouse.tmp" "$dir/clickhouse"
echo "[clickhouse] installed $("$dir/clickhouse" --version)"
