#!/usr/bin/env bash
# Build the `aaiclick` meta wheel + sdist pinned to aaiclick-client==VERSION.
# Usage: meta/build.sh VERSION OUT_DIR
set -euo pipefail
version="$1"
out="$(realpath -m "$2")"
here="$(cd "$(dirname "$0")" && pwd)"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

cp "$here/pyproject.toml" "$work/"
cp -L "$here/README.md" "$work/"
sed -i -e "s/^version = \"0\.0\.0\"/version = \"${version}\"/" \
  -e "s/\"aaiclick-client\[all\]\"/\"aaiclick-client[all]==${version}\"/" "$work/pyproject.toml"
uv build "$work" --out-dir "$out"

wheel="$(ls "$out"/aaiclick-"${version}"-*.whl)"
unzip -p "$wheel" "aaiclick-${version}.dist-info/METADATA" \
  | grep -qxF "Requires-Dist: aaiclick-client[all]==${version}" \
  || { echo "meta wheel $wheel does not pin aaiclick-client[all]==${version}" >&2; exit 1; }
