#!/bin/sh
# Write .env from .env.template: one random value per change-me-... token.
set -eu
cd "$(dirname "$0")"
[ -e .env ] && { echo ".env already exists; delete it to regenerate" >&2; exit 1; }
umask 077
cp .env.template .env.tmp
# Longest first, so a token that prefixes another can't clip it.
for token in $(grep -o 'change-me-[A-Za-z0-9][A-Za-z0-9-]*' .env.template | sort -u | awk '{ print length, $0 }' | sort -rn | cut -d' ' -f2); do
  sed -i.bak "s/$token/$(openssl rand -hex 24)/g" .env.tmp
done
rm -f .env.tmp.bak
mv .env.tmp .env
echo "Wrote .env (keep it out of git)"
grep '^AAICLICK_ADMIN_PASSWORD=' .env | sed 's/^AAICLICK_ADMIN_PASSWORD=/Admin login: admin \/ /'
