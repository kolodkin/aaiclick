#!/usr/bin/env bash
# Ensure a local Azurite (Azure Blob emulator) is running on :10000 for
# aaiclick/data/object/test_blob.py. Reuses one already up; otherwise installs
# the npm package into .cache/azurite/ (once) and starts it. `stop` stops it.
# The tests create their container themselves.
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$here/_guard.sh"
. "$here/_stop.sh"

cache="$here/../.cache"
dir="$cache/azurite"
pidfile="$dir/azurite.pid"
up() { (exec 3<>/dev/tcp/127.0.0.1/10000) 2>/dev/null; }

if [ "${1:-}" = "stop" ]; then
  stop_pidfile azurite "$pidfile"
  exit 0
fi

if up; then
  echo "[azurite] already up on :10000"
  exit 0
fi

mkdir -p "$dir/data"
if [ ! -x "$dir/node_modules/.bin/azurite-blob" ]; then
  # Same version as .github/actions/start-azurite and test_blob.py; bump together.
  version="${AZURITE_VERSION:-3.34.0}"
  npm install --silent --no-audit --no-fund --prefix "$dir" "azurite@$version" >"$cache/azurite-install.log" 2>&1 \
    || { echo "[azurite] error: npm install failed; see $cache/azurite-install.log" >&2; exit 1; }
  echo "[azurite] installed azurite@$version"
fi

nohup "$dir/node_modules/.bin/azurite-blob" --blobHost 127.0.0.1 --blobPort 10000 \
  --location "$dir/data" --loose --skipApiVersionCheck >"$cache/azurite.log" 2>&1 &
echo $! >"$pidfile"
disown
for _ in $(seq 1 30); do up && break; sleep 1; done
up || { echo "[azurite] error: not ready; see $cache/azurite.log" >&2; exit 1; }
echo "[azurite] up on :10000"
