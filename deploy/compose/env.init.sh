#!/bin/sh
# Write .env from .env.template: one random value per change-me-... token.
set -eu
cd "$(dirname "$0")"
[ -e .env ] && { echo ".env already exists; delete it to regenerate" >&2; exit 1; }
umask 077
trap 'rm -f .env.tmp .env.tmp.bak' EXIT
cp .env.template .env.tmp
# Longest first, so a token that prefixes another can't clip it.
for token in $(grep -o 'change-me-[A-Za-z0-9][A-Za-z0-9-]*' .env.template | sort -u | awk '{ print length, $0 }' | sort -rn | cut -d' ' -f2); do
  # A plain assignment, so set -e aborts if openssl fails instead of writing an empty secret.
  value=$(openssl rand -hex 24)
  sed -i.bak "s/$token/$value/g" .env.tmp
done
mv .env.tmp .env
echo "Wrote .env (keep it out of git)"
grep '^AAICLICK_ADMIN_PASSWORD=' .env | sed 's/^AAICLICK_ADMIN_PASSWORD=/Admin login: admin \/ /'
