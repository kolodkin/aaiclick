---
name: claudeai-run
description: Start the local ClickHouse, PostgreSQL and MinIO (S3) services the distributed-backend and blob-storage tests need by running claudeai/run.sh. Use at the start of a new session, whenever the services are down (e.g. after a container suspend/resume), or when a test or spike needs an S3 endpoint.
disable-model-invocation: true
---

# claudeai-run Skill

Brings up the local distributed-backend services. Binaries and on-disk state are
installed once by `claudeai/setup.sh` and survive a container resume, but the processes
don't — so each session must restart them. That's what `claudeai/run.sh` does.

Local mode (chdb + SQLite, the default URLs) needs none of this.

## Preflight: web environment only

These services are meant only for the Claude Code web (cloud) environment. Before
running anything, verify you're there — abort the skill if not:

```bash
[ "${CLAUDE_CODE_REMOTE:-}" = "true" ] || { echo "Not the Claude Code web environment — skipping claudeai-run."; exit 1; }
```

`CLAUDE_CODE_REMOTE=true` is set only in the remote/web container (`CLAUDECODE=1`
is set for any Claude Code session, local included, so don't gate on that). The
`claudeai/*.sh` scripts enforce the same guard via `claudeai/_guard.sh`; a
deliberate local run can override with `CLAUDEAI_ALLOW_LOCAL=1`.

## Start

```bash
claudeai/run.sh
```

Idempotently starts (reusing anything already up), with the same credentials as
the CI services in `.github/workflows/_test-reusable.yaml`:

1. **ClickHouse** — `:8123` (`default`/`click123`)
2. **PostgreSQL** — `:5432` (`aaiclick`/`secret`, db `aaiclick`)
3. **MinIO** — `:9100` (`minioadmin`/`minioadmin`, bucket `aaiclick-test`);
   built from source by `claudeai/setup_minio.sh` (~2 min once — MinIO no
   longer publishes binaries, images or `mc`)
4. **Migrations** — `aaiclick migrate upgrade head` against both

Prints `[run] all services up` on success. A failure exits non-zero with a
`[<svc>] error:` line pointing at its log under `.cache/`.

## Use

```bash
source claudeai/env.sh   # AAICLICK_CH_URL, AAICLICK_SQL_URL, AAICLICK_JWT_SECRET, AAICLICK_S3_CREDENTIALS, AWS_*
uv run --frozen --extra distributed --extra test pytest aaiclick/
```

Blob URLs carry no credentials: ClickHouse and chdb read `AWS_ACCESS_KEY_ID` /
`AWS_SECRET_ACCESS_KEY` from the environment by default. MinIO needs
bucket-style `http://localhost:9100/bucket/key` URLs; `s3://` resolves to AWS.
List a bucket without `mc`:

```bash
curl --aws-sigv4 aws:amz:us-east-1:s3 --user minioadmin:minioadmin http://localhost:9100/aaiclick-test/
```

## Stop

```bash
claudeai/run.sh stop
```

## If a service won't start

`run.sh` runs `claudeai/setup.sh` itself when the binary or cluster is missing
(fresh container). To start over, stop the services, delete `.cache/clickhouse`,
`.cache/postgres` or `.cache/minio`, then re-run.

Per-service logs: `.cache/clickhouse.log`, `.cache/postgres/postgres.log`,
`.cache/minio.log`, `.cache/migrate.log`.
