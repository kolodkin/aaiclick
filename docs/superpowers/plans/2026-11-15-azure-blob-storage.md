# Azure Blob Storage Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `az://container/path` URLs for `open_object_from_url()` and `Object.export()`, credentials from a ClickHouse named collection (server) or `AZURE_STORAGE_CONNECTION_STRING` (chdb).

**Architecture:** One new module renders every Azure source as `azureBlobStorage(<collection>, container=..., blob_path=..., format=...)`; the three existing S3 call sites (open, engine clause, export) branch on the `az` scheme into it. On chdb the collection is created from the env var with `CREATE NAMED COLLECTION IF NOT EXISTS` before first use.

**Tech Stack:** Python, chdb 4.4.0, ClickHouse server 26.10, Azurite (npx), `azure-storage-blob` (dev).

**Spec:** `docs/superpowers/specs/2026-11-15-azure-blob-storage-design.md`

## Global Constraints

- No secret in any per-query SQL, the operation log, or `table_registry.schema_doc`; only the chdb `CREATE NAMED COLLECTION` statement carries it, never oplog-recorded.
- Collection holds only endpoint + credentials — never `container`, `blob_path`, `format`.
- `connection` is a bare identifier (`str.isidentifier()`), inlined into SQL.
- `s3` / `S3` rendering unchanged.
- Imports at file top; `Literal` over enums; no `__all__`; no `global` (CLAUDE.md).
- Azurite dev connection string (tests only):
  `DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://<host>:<port>/devstoreaccount1;`

## Review Focus

- Blob path with a quote (`az://c/it's.parquet`) — must render via `quote_sql_literal`, not break SQL (test in Task 3).
- Glob in an `az://` path (`az://c/part-*.parquet`) passes validation and reads all matches (Task 3).
- `connection` on an `s3://` URL or local path → `ValueError`, not a silent ignore (Task 2).
- `az://` with no path (`az://container`) or empty container → `ValueError` (Task 2).
- chdb with `AZURE_STORAGE_CONNECTION_STRING` unset → `ValueError` naming the variable, before ClickHouse is called (Task 3).

---

### Task 1: Verify ClickHouse accepts collection + keyword keys

**Files:** none committed (scratchpad script).

- [ ] **Step 1:** Start Azurite (`npx -y -p azurite azurite-blob --location <tmp> --blobHost 0.0.0.0 --blobPort 10000 --loose --skipApiVersionCheck`), create container `t` with `azure-storage-blob`.
- [ ] **Step 2:** On chdb run: `CREATE NAMED COLLECTION IF NOT EXISTS az_t AS connection_string = '<dev string>'`; `INSERT INTO FUNCTION azureBlobStorage(az_t, container='t', blob_path='x.parquet', format='Parquet') SELECT number FROM numbers(3)`; `CREATE TABLE p (number UInt64) ENGINE = AzureBlobStorage(az_t, container='t', blob_path='x.parquet', format='Parquet')`; `SELECT * FROM p`; `DESCRIBE azureBlobStorage(az_t, container='t', blob_path='x.parquet', format='Parquet')`.
  Expected: rows 0,1,2 from both the table and the function.
- [ ] **Step 3:** Repeat on `clickhouse/clickhouse-server:26.10` (docker) with the collection defined in a `config.d` XML instead of `CREATE`. If docker is unavailable locally, push the Task 4 CI job first and use it as the check.
- [ ] **Step 4:** If any form is rejected, stop and report to the user (spec: "record the gap and stop for a decision").

### Task 2: URL and connection validation

**Files:**
- Modify: `aaiclick/data/url_utils.py`
- Test: `aaiclick/data/object/test_blob_azure.py` (create)

**Interfaces:**
- Produces: `AZ_SCHEME = "az"`; `BLOB_URL_SCHEMES = ("http", "https", "s3", "az")`; `validate_connection(url: str, connection: str | None) -> None`; `AzUrl(NamedTuple): container: str; blob_path: str`; `parse_az_url(url: str) -> AzUrl`.

- [ ] **Step 1: Write failing tests** (no Azurite needed):

```python
@pytest.mark.parametrize("url, connection, match", [
    ("az://container", "lake", "blob path"),
    ("az:///path.parquet", "lake", "container"),
    ("az://c/p.parquet?sig=abc", "lake", "query parameters"),
    ("s3://b/k.parquet", "lake", "connection"),
    ("/tmp/x.parquet", "lake", "connection"),
    ("az://c/p.parquet", "bad name", "connection"),
])
async def test_open_rejects_bad_azure_arguments(ctx, url, connection, match):
    with pytest.raises(ValueError, match=match):
        await open_object_from_url(url, connection=connection)

def test_parse_az_url():
    assert parse_az_url("az://reports/2026/daily.parquet") == AzUrl("reports", "2026/daily.parquet")
```

- [ ] **Step 2:** `uv run pytest aaiclick/data/object/test_blob_azure.py -v` → FAIL (`connection` unknown / import error).
- [ ] **Step 3:** Implement. `validate_blob_url` accepts `az`, requires netloc and non-empty path for it; scheme error message lists `az`. `validate_connection`: `connection` set with non-`az` URL → `ValueError("connection applies only to az:// URLs")`; not `isidentifier()` → `ValueError("Invalid connection name ...")`. Add the `connection` kwarg to `open_object_from_url` and call `validate_connection` there (rendering comes in Task 3).
- [ ] **Step 4:** Tests PASS. **Step 5:** Commit `feat: validate az:// URLs and connection names`.

### Task 3: Render, open and export on chdb

**Files:**
- Create: `aaiclick/data/azure.py`
- Modify: `aaiclick/data/models.py` (`Schema.connection`), `aaiclick/data/object/url.py`, `aaiclick/data/object_context/object_context.py:340-370`, `aaiclick/data/object_context/ch_client.py:170-195`, `aaiclick/data/object/object.py:677-725`
- Modify: `pyproject.toml` (dev: `azure-storage-blob`), `uv.lock`
- Test: `aaiclick/data/object/test_blob_azure.py`

**Interfaces:**
- Consumes: `parse_az_url`, `validate_connection` (Task 2).
- Produces:
  - `AZURE_CONNECTION_ENV = "AZURE_STORAGE_CONNECTION_STRING"`; `DEFAULT_AZURE_CONNECTION = "aaiclick_azure"`.
  - `azure_source(url: str, fmt: str, connection: str) -> str` → `"azureBlobStorage(lake, container='c', blob_path='p', format='F')"` (literals via `quote_sql_literal`); the engine clause uses the same args as `ENGINE = AzureBlobStorage(...)`.
  - `async ensure_azure_connection(connection: str | None) -> str` → resolved name. chdb: reads env (missing → `ValueError` naming `AZURE_STORAGE_CONNECTION_STRING`), runs `CREATE NAMED COLLECTION IF NOT EXISTS`, `None` → `DEFAULT_AZURE_CONNECTION`. Server: `None` → `ValueError("an az:// URL needs connection= on a ClickHouse server")`, else returns the name.
  - `Schema.connection: str | None = None`.
  - `export_query_to_url(query, url, fmt, overwrite=False, settings=None, connection=None)`; reserved set gains `azure_truncate_on_insert`, `azure_create_new_file_on_insert`; Azure path sets `azure_truncate_on_insert = int(overwrite)`.
  - `Object.export(..., connection: str | None = None)`; `open_object_from_url(..., connection=None)` stores the resolved name in `Schema.connection`.

- [ ] **Step 1: Write failing tests.** Module fixture `azurite` (free port, `npx` subprocess, wait for port, create container `aaiclick-test`; skip if `npx` missing), autouse on chdb: `monkeypatch.setenv(AZURE_CONNECTION_ENV, <dev string>)`. Tests, mirroring `test_blob.py`:
  - `test_export_open_round_trip` parametrized over `.parquet`, `.csv`, `.jsonl` — `open_object_from_url(await obj.export(url, connection=C), connection=C).data() == SAMPLE_TABLE` (CSV per existing expectations).
  - `test_export_to_existing_key_needs_overwrite` — `match="already exists"` (or ClickHouse's Azure wording, pinned on first run), then `overwrite=True` replaces.
  - `test_export_honors_view_constraints` — `{"id": [2], "name": ["bob"], "score": [20.0]}`.
  - `test_open_glob` — two exports `part-1`, `part-2`; `az://.../part-*.parquet` sums both.
  - `test_open_url_with_quote_in_key` — key `it's.parquet` round-trips.
  - `test_export_rejects_reserved_azure_settings` parametrized `azure_truncate_on_insert`, `azure_create_new_file_on_insert` → `match="cannot set"`.
  - `test_located_azure_object_survives_task_boundary` — as `test_located_object_stays_read_only_across_a_task_boundary`, plus rehydrated `schema.connection == C`.
  - `test_oplog_has_no_account_key` — after export + open + `.data()`, no oplog `sql` row and no `schema_doc` contains `"AccountKey"`.
  - `test_chdb_missing_env_var` (chdb only) — `monkeypatch.delenv`; `match="AZURE_STORAGE_CONNECTION_STRING"`.
  - `test_server_needs_connection` (server only) — `connection=None` → `match="needs connection"`.
- [ ] **Step 2:** Run → FAIL.
- [ ] **Step 3:** Implement per Interfaces. `create_object` validates `schema.connection` with the location (`validate_connection`). Engine clause branches: `az` → `ENGINE = AzureBlobStorage(...)`, other URL → `S3(...)` as now. Open: `DESCRIBE (SELECT ... FROM {azure_source(...)} LIMIT 0)`.
- [ ] **Step 4:** `uv run pytest aaiclick/data/object/test_blob_azure.py aaiclick/data/object/test_blob.py -v` → all PASS (S3 unchanged).
- [ ] **Step 5:** Commit `feat: Azure Blob Storage open and export`.

### Task 4: Server backend in CI

**Files:**
- Create: `docker/clickhouse/config.d/azurite.xml` (collection `azurite_test`, `connection_string` pointing at `http://azurite:10000/devstoreaccount1` — test-only dev key)
- Modify: `.github/workflows/_test-reusable.yaml` (`test-blob-dist`)
- Modify: `aaiclick/data/object/test_blob_azure.py` (server: use `connection="azurite_test"`; Azurite host from `AAICLICK_TEST_AZURITE_HOST`)

- [ ] **Step 1:** In `test-blob-dist`: create a docker network; run `mcr.microsoft.com/azure-storage/azurite` (`azurite-blob --blobHost 0.0.0.0 --loose`) on it as `azurite`, `-p 10000:10000`; attach ClickHouse to the network and mount `config.d/azurite.xml`. Run `test_blob.py test_blob_azure.py --deselect aaiclick/data/object/test_blob_azure.py::test_chdb_missing_env_var` under the existing no-SKIPPED check (the chdb-only test is `skipif` on a server, so deselect it rather than weaken the grep).
- [ ] **Step 2:** Push; run `devpowers:check-pr` / `action-check test` → `Blob storage dist` green with the Azure tests PASSED.
- [ ] **Step 3:** Commit `ci: Azurite for blob storage dist`.

### Task 5: Docs

**Files:** `docs/designs/blob_storage.md`, `docs/user_guide/object.md`, `docs/designs/future.md`; delete this plan and the spec.

- [ ] **Step 1:** `blob_storage.md`: "Azure" section — URL form, credentials-by-backend table, collection contract, `config.d` example, testing (Azurite). Follow `markdown-style`.
- [ ] **Step 2:** `object.md` "Blob Storage": `az://` + `connection=` example with `# →` style kept.
- [ ] **Step 3:** `future.md`: "GCS and Azure" → "GCS"; add deferred "Azure server identity" (no opt-in guard; docs disagree; needs a real Azure check).
- [ ] **Step 4:** `git rm` spec and plan; fix references. Commit `docs: Azure Blob Storage`; push; `check-pr`.
