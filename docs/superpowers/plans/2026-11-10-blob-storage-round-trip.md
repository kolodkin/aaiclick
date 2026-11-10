# Blob Storage Round-Trip Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task (inline, per CLAUDE.md). Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `Object.export("http(s)://…|s3://…")` writes server-side to a bucket and `open_object_from_url(url)` opens it as a read-only Object queried in place.

**Architecture:** A located Object is a temp `ENGINE = S3(url, format)` pointer table; `Schema.location` / `Schema.format` mark it. Export to a URL runs `INSERT INTO FUNCTION s3()` on both backends. `AAICLICK_S3_CREDENTIALS=server` makes the remote client send `s3_allow_server_credentials_in_user_queries=1` as a connection default; SQL never carries credentials.

**Tech Stack:** ClickHouse / chdb `s3()` + `S3` engine, clickhouse-connect, `moto[server]` (tests).

**Spec:** `docs/superpowers/specs/2026-11-10-blob-storage-round-trip-design.md`

## Global Constraints

- No credentials in any SQL aaiclick renders.
- URL schemes accepted: `http`, `https`, `s3`.
- Located objects are temp only; `insert` / `insert_from_url` raise `ValueError`.
- Imports at top of file; no `Any`; `Literal` for the credentials mode; no `global`.
- Tests follow the `python-testing-style` skill; docs follow `markdown-style`.

## Review Focus

- URL with a single quote → must be escaped via `quote_sql_literal`, not break SQL (test in Task 3).
- `export` to an existing key without `overwrite` → raises, object unchanged (Task 2).
- URL with no recognizable extension and no `format=` → `ValueError` before any query (Task 3).
- `AAICLICK_S3_CREDENTIALS=bogus` → `ValueError` naming the allowed value (Task 1).
- View constraints (`where`, `limit`) honored on URL export (Task 2).

---

### Task 1: `AAICLICK_S3_CREDENTIALS` connection setting

**Files:** Modify `aaiclick/backend.py`, `aaiclick/data/object_context/clickhouse_client.py`; Test `aaiclick/test_backend.py` (create if absent, else extend).

**Interfaces — Produces:**
- `S3_CREDENTIALS_SERVER = "server"`, `S3CredentialsMode = Literal["server"]`
- `get_s3_credentials() -> S3CredentialsMode | None` — reads env, `None` when unset/empty, `ValueError` otherwise-invalid.
- `s3_client_settings() -> dict[str, int]` — `{"s3_allow_server_credentials_in_user_queries": 1}` for `server`, else `{}`.

- [ ] Test: parametrized `(env value, expected)` — unset → `None`/`{}`, `"server"` → mode/setting dict, `"bogus"` → `ValueError` matching `server`.
- [ ] Implement; `create_clickhouse_client` passes `settings=s3_client_settings()`.
- [ ] Run tests, commit.

### Task 2: test S3 fixture + export to URL

**Files:** Modify `pyproject.toml` (`moto[server]>=5` in `test`), `uv.lock`, `aaiclick/data/object_context/ch_client.py`, `aaiclick/data/object/object.py`; Create `aaiclick/data/object/test_blob.py`.

**Interfaces — Produces:**
- `export_query_to_url(query: str, url: str, fmt: str, overwrite: bool = False) -> str` in `ch_client.py` — `INSERT INTO FUNCTION s3(<quoted url>, '<fmt>') <query>` with `settings={"s3_truncate_on_insert": int(overwrite)}`; returns `url`.
- `validate_blob_url(url: str) -> None` in `aaiclick/data/sql_utils.py` — scheme in `{http, https, s3}` and non-empty netloc, else `ValueError`.
- `Object.export(path: str, overwrite: bool = False) -> str` — `"://" in path` routes to `export_query_to_url` after `validate_blob_url`; `overwrite=True` with a local path is ignored (local export already overwrites).
- Fixture `bucket` (module scope) in `test_blob.py`: starts `ThreadedMotoServer(ip_address="0.0.0.0", port=0)`, creates bucket `aaiclick-test` with `boto3`, sets `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` = `testing` in `os.environ` (restored on teardown), yields `http://{AAICLICK_TEST_FILESERVER_HOST or localhost}:{port}/aaiclick-test`.

- [ ] Tests: export Parquet then read back with `boto3` + `pyarrow` equals `SAMPLE`; export of `view(where=…, limit=…)` honors constraints; second export to same key raises, with `overwrite=True` succeeds; `export("ftp://x/y.parquet")` raises `ValueError`.
- [ ] Implement, run on chdb, commit.

### Task 3: located Object + `open_object_from_url`

**Files:** Modify `aaiclick/data/models.py`, `aaiclick/data/object_context/object_context.py`, `aaiclick/data/object/url.py`, `aaiclick/data/object/object.py`, `aaiclick/data/object/__init__.py`, `aaiclick/data/__init__.py`, `aaiclick/__init__.py`; Test `aaiclick/data/object/test_blob.py`.

**Interfaces — Produces:**
- `Schema.location: str | None = None`, `Schema.format: str | None = None`.
- `create_object`: `schema.location` set → engine clause `ENGINE = S3(<quoted location>, '<format>')`; `name` given → `ValueError("located objects cannot be named yet")`.
- `open_object_from_url(url: str, columns: list[str] | None = None, format: str | None = None, column_types: dict[str, ColumnInfo] | None = None) -> Object` in `url.py`; `format` defaults via the input-format extension lookup (add `input_format_for_extension` to `formats.py` if `format_for_extension` only covers output); explicit `format` validated against `INPUT_FORMATS`.
- `Object.insert` / `Object.insert_from_url`: `ValueError("located objects are read-only")` when `self._schema.location`.

- [ ] Tests: parametrized export → open round trip over `.parquet`, `.csv`, `.tsv`, `.jsonl`, `.orc`, `.avro` (`data()` equals `SAMPLE`); `where("score > 15").sum()`-style aggregation on a located object; single `columns=["id"]` gives array; `insert` and `insert_from_url` raise; URL containing `'` round-trips; extensionless URL without `format` raises `ValueError`; `name=` via `create_object(Schema(location=…), name="x")` raises.
- [ ] Implement, run full `aaiclick/data` suite on chdb, commit.

### Task 4: distributed backend wiring

**Files:** Modify `.github/workflows/_test-reusable.yaml` (clickhouse service env: `AWS_ACCESS_KEY_ID: testing`, `AWS_SECRET_ACCESS_KEY: testing`; test steps env: `AAICLICK_S3_CREDENTIALS: server`), `claudeai/env.sh` (`AAICLICK_S3_CREDENTIALS=server`), `claudeai/run.sh` (server pair becomes `testing`/… only if needed — keep MinIO pair; tests use moto with any pair).

- [ ] Run `test_blob.py` against local ClickHouse + Postgres via `claudeai/run.sh` + `env.sh`; fix what differs from chdb.
- [ ] Commit.

### Task 5: docs

**Files:** Modify `docs/user_guide/object.md` (Export: URL targets, `overwrite`; new "Blob storage" section: `open_object_from_url`, read-only rules, credentials + `AAICLICK_S3_CREDENTIALS`), `docs/designs/future.md` (entry reduced to deferred pieces); Move `docs/drafts/blob_storage_support.md` → `docs/designs/blob_storage.md` trimmed to shipped behavior; module docstring link in `url.py`; Delete this plan and the spec.

- [ ] Write docs, run `mkdocs build --strict` if configured, commit.
