Azure Blob Storage — Design
---

# Intent

A specific team's data lake lives in Azure Blob Storage. aaiclick must read
files from it and export results back to it, exactly as it does for S3 today
(`docs/designs/blob_storage.md`): plain files (Parquet, CSV, every format the
S3 path handles), opened as read-only located Objects and written by
`Object.export()`. Production runs on a ClickHouse server; chdb is for local
work.

# Goals

- `az://container/path` URLs for `open_object_from_url()` and
  `Object.export()`, on chdb and a ClickHouse server.
- Credentials per backend, mirroring S3 (env on chdb, server-side on a
  server):
  - **ClickHouse server**: an operator-defined named collection, named by
    `connection="lake"`.
  - **chdb**: `AZURE_STORAGE_CONNECTION_STRING` in the aaiclick process
    environment — the Azure SDK convention.
- The same job code runs on both backends.
- Tested end to end against Azurite, Microsoft's Azure Storage emulator.

# Non-goals

- Server managed / workload identity (no `connection`, server's own Azure
  identity) — Azure has no opt-in guard like S3's, and the ClickHouse docs
  disagree on whether the table function supports it. Goes to `future.md`.
- Raw connection strings passed by callers.
- Delta, Iceberg, Paimon; GCS; `connection=` for S3; writable or named
  located Objects.

# Anti-goals

- A secret (account key, SAS, connection string) in any SQL aaiclick sends
  per query, in the operation log, or in `table_registry.schema_doc`.

# Constraints

- ClickHouse's Azure function / engine take the endpoint, container and blob
  path as separate arguments, unlike `s3(url, ...)`; aaiclick splits its URL.
- Named collections (ClickHouse docs): `azureBlobStorage(lake, container=...,
  blob_path=..., format=...)` adds keys per query. Overriding a key the
  collection *stores* needs `SHOW NAMED COLLECTIONS SECRETS`; adding one it
  does not store needs nothing.
- chdb 4.4.0 ships `AzureBlobStorage`; a chdb ↔ Azurite Parquet round-trip via
  a connection string was verified by hand. chdb has
  `azure_truncate_on_insert` and `azure_create_new_file_on_insert`.
- CI ClickHouse server image: `clickhouse/clickhouse-server:26.10`.

# Approaches

| Approach | Trade-off | Verdict |
|---|---|---|
| A — `az://account/container/path`, optional `connection=` | Adds server identity; account redundant with a collection | Dropped with server identity |
| B — native `https://account.blob.core.windows.net/...` | `http(s)` already means S3-compatible; Azurite's `127.0.0.1:10000` cannot be sniffed | Rejected |
| C — `az://container/path` + a named collection | One URL form; secrets only in the collection | **Picked** |

A raw caller-supplied connection string was considered and rejected: it would
land in the operation log and registry, both shared in the viewer.

# Design

## API

```python
url = await daily.export("az://reports/2026/daily.parquet", connection="lake")
daily = await open_object_from_url("az://reports/2026/daily.parquet", connection="lake")
```

- `open_object_from_url(url, columns=None, format=None, column_types=None,
  connection=None)` and `Object.export(path, overwrite=False, format=None,
  ch_settings=None, connection=None)`; the lazy-object `export` delegates
  through `*args, **kwargs` already.
- `connection` is a ClickHouse named-collection name: a bare identifier
  (inlined into SQL, same check as `validate_format_name`). `connection` with
  a non-`az` URL or a local path raises `ValueError`.
- `export` returns the URL it was given; the next job passes the same
  `connection`.

## Credentials by backend

| Backend | `connection="lake"` | `connection=None` |
|---|---|---|
| ClickHouse server | Collection `lake` from the server's `config.d` | `ValueError`: an `az://` URL needs `connection` |
| chdb | Collection `lake`, created from the env var | Collection from the env var |

- **Server**: the operator defines the collection in `config.d`; the key never
  passes through aaiclick. **Collection contract**: it stores only the
  endpoint and credentials (`connection_string`, or `storage_account_url` +
  `account_name` / `account_key`) — never `container`, `blob_path` or
  `format`, which aaiclick always sends.
- **chdb**: on first `az://` use in a process, aaiclick runs
  `CREATE NAMED COLLECTION IF NOT EXISTS <name> AS connection_string = '...'`
  from `AZURE_STORAGE_CONNECTION_STRING`, for the requested name (or a fixed
  default name when `connection=None`). This one statement carries the
  secret: it runs on the in-process chdb, is never oplog-recorded and never
  stored. Missing env var → `ValueError` naming it.
  `shortcut: every connection name maps to the one env var, add per-name env
  vars when a local job needs two accounts.`
- Job code passing `connection="lake"` therefore runs unchanged on both.

## Schema

`Schema` gains `connection: str | None = None`, beside `location` / `format`
— a name, never a secret. Serialised into `table_registry.schema_doc`, so a
located Object rehydrated across a task boundary keeps it. `create_object`
validates it with the location.

## URL validation (`aaiclick/data/url_utils.py`)

- `BLOB_URL_SCHEMES` gains `az`; error messages listing schemes include it.
- `az` URL: netloc is the container, non-empty path is the blob path (globs
  pass through). Userinfo and `key=value` query rejected as for every blob URL.

## Rendering

One helper renders `azureBlobStorage(<collection>, container='c',
blob_path='p', format='F')` from `(url, format, connection)` for all three
call sites:

- `open_object_from_url` — `DESCRIBE (SELECT ... FROM azureBlobStorage(...))`.
- `object_context.py` engine clause — `ENGINE = AzureBlobStorage(...)`.
- `ch_client.export_query_to_url` — `INSERT INTO FUNCTION
  azureBlobStorage(...) <query>` with `azure_truncate_on_insert =
  int(overwrite)`. Reserved settings gain `azure_truncate_on_insert` and
  `azure_create_new_file_on_insert`.

`s3` / `S3` rendering is unchanged.

## Tests

- Azurite fixture: `npx -y -p azurite azurite-blob` on a free port; container
  created with `azure-storage-blob` (new dev dependency).
- chdb: `AZURE_STORAGE_CONNECTION_STRING` set to Azurite's well-known dev
  string via `monkeypatch`.
- Server (`Blob storage dist` job): Azurite as a second container; a
  `config.d` file defining the test collection, mounted into ClickHouse.
- Cover, in the shape of the S3 tests: export, overwrite, view constraints,
  export → open round trip per extension, reserved settings, read-only across
  a task boundary (connection survives `schema_doc`), URL / connection
  validation, server `connection=None` error, chdb missing env var error.
- The oplog `sql` of an Azure export / open contains no `AccountKey`.

## Docs

- `docs/designs/blob_storage.md`: Azure section — URL form, credentials by
  backend, collection contract, testing.
- `docs/user_guide/object.md` — "Blob Storage": `az://` and `connection=`.
- `docs/designs/future.md`: drop Azure from "GCS and Azure"; add Azure server
  identity as a deferred item with the reasons above.

# Left to the builder

- One test module or two; Azurite fixture plumbing on both backends.
- First, on the CI server image: does `ENGINE = AzureBlobStorage(lake,
  container=..., blob_path=..., format=...)` take a collection plus keyword
  keys? Then on chdb. If not, record the gap and stop for a decision.
- The default collection name used on chdb when `connection=None`.
- Whether `with_url_retry` needs Azure transient errors added.
- Compression: ClickHouse's suffix detection, as for S3.
