Azure Blob Storage — Design
---

# Intent

A specific team's data lake lives in Azure Blob Storage. aaiclick must read
files from it and export results back to it, exactly as it does for S3 today
(`docs/designs/blob_storage.md`): plain files (Parquet, CSV, every format the
S3 path handles), opened as read-only located Objects and written by
`Object.export()`. No Delta / Iceberg.

# Goals

- `az://` URLs for `open_object_from_url()` and `Object.export()`, on chdb and
  a ClickHouse server.
- Two credential paths, both keeping secrets out of rendered SQL:
  - **Named collection** (first use case, build and test first): the operator
    defines a ClickHouse named collection holding the endpoint and key / SAS;
    aaiclick names it with `connection="lake"`.
  - **Server identity** (follow-up in the same work): no `connection`;
    ClickHouse uses its managed / workload identity.
- Tested end to end against Azurite, Microsoft's Azure Storage emulator.

# Non-goals

- Delta, Iceberg, Paimon (chdb has the engines; separate item in `future.md`).
- `connection=` for S3 — stays under "Per-object credentials" in `future.md`.
- GCS.
- Writable located Objects (rejected for S3, same reasoning).
- Named located Objects.

# Anti-goals

- A secret (account key, SAS, connection string) anywhere in SQL aaiclick
  renders — the operation log and ClickHouse's query log must stay clean.
- aaiclick reading, storing or forwarding Azure secrets itself.

# Constraints

- ClickHouse's Azure function / engine take the endpoint, container and blob
  path as separate arguments, unlike `s3(url, ...)`; aaiclick splits its URL.
- Named collections (ClickHouse docs): `azureBlobStorage(lake, blob_path=...,
  format=...)` overrides keys per query. Keys: `connection_string` or
  `storage_account_url` (+ `account_name`, `account_key`), `container`,
  `blob_path`, `format`, `compression`, `structure`. Overriding a key the
  collection *stores* needs `SHOW NAMED COLLECTIONS SECRETS`; adding a key it
  does not store needs nothing.
- Server identity: the engine docs list managed and workload identity when
  only an endpoint / `storage_account_url` is given; the table-function docs
  say a key is required. Unverified — Azurite cannot exercise it.
- No documented Azure analogue of `s3_allow_server_credentials_in_user_queries`.
- chdb 4.4.0 ships `AzureBlobStorage`; a chdb ↔ Azurite Parquet round-trip via
  a connection string was verified by hand.
- chdb has `azure_truncate_on_insert` and `azure_create_new_file_on_insert`.

# Approaches

| Approach | Trade-off | Verdict |
|---|---|---|
| A — `az://account/container/path`, optional `connection=` | One string per file; both credential paths | Picked, with C |
| B — native `https://account.blob.core.windows.net/...` | `http(s)` already means S3-compatible; Azurite's `127.0.0.1:10000` host cannot be sniffed | Rejected |
| C — `az://container/path`, `connection=` required | Simplest; no server identity alone | Picked, with A |

A and C share the `az` scheme. **`connection=` decides the URL grammar**
(user's choice, over an `az://container@account/path` form):

| `connection` | URL | Renders |
|---|---|---|
| `"lake"` | `az://container/path` | `azureBlobStorage(lake, container='container', blob_path='path', format='F')` |
| `None` | `az://account/container/path` | `azureBlobStorage('https://account.blob.core.windows.net', 'container', 'path', 'F')` |

A URL used in the wrong mode fails as ClickHouse's "container not found"
(or missing-credentials) error; aaiclick does not guess.

# Design

## API

```python
# Named collection (C) — first use case
url = await daily.export("az://reports/2026/daily.parquet", connection="lake")
daily = await open_object_from_url("az://reports/2026/daily.parquet", connection="lake")

# Server identity (A)
daily = await open_object_from_url("az://acct/reports/2026/daily.parquet")
```

- `open_object_from_url(url, columns=None, format=None, column_types=None,
  connection=None)` and `Object.export(path, overwrite=False, format=None,
  ch_settings=None, connection=None)`; the lazy-object `export` delegates
  through `*args, **kwargs` already.
- `connection` is a ClickHouse named-collection name: a bare identifier
  (inlined into SQL, same check as `validate_format_name`). `connection` with
  a non-`az` URL or a local path raises `ValueError`.
- **Collection contract** (my call): the collection stores only the endpoint
  and credentials (`connection_string`, or `storage_account_url` +
  `account_name` / `account_key`) — never `container`, `blob_path` or
  `format`, which aaiclick always sends and would otherwise need
  `SHOW NAMED COLLECTIONS SECRETS` to override. Documented; the CI collection
  follows it.
- `export` returns the URL it was given; the connection is not encoded in it.
  The next job passes the same `connection` — the operator's config, like the
  S3 server identity.

## Schema

`Schema` gains `connection: str | None = None`, beside `location` / `format`.
It is serialised into `table_registry.schema_doc` with the rest, so a located
Object rehydrated across a task boundary keeps it. `create_object` validates
it with the location.

## URL validation (`aaiclick/data/url_utils.py`)

- `BLOB_URL_SCHEMES` gains `az`.
- `az` URL: netloc required (container in C, account in A); a non-empty
  path; A needs at least `account/container/path`. Userinfo and `key=value`
  query rejected as for every blob URL — a SAS belongs in the collection.
- Error messages listing schemes include `az`.

## Rendering

One helper renders the Azure source arguments from `(url, format,
connection)` for all three call sites, so the grammar lives in one place:

- `open_object_from_url` — `DESCRIBE (SELECT ... FROM azureBlobStorage(...))`.
- `object_context.py` engine clause — `ENGINE = AzureBlobStorage(...)`.
- `ch_client.export_query_to_url` — `INSERT INTO FUNCTION
  azureBlobStorage(...) <query>` with `azure_truncate_on_insert =
  int(overwrite)`. The reserved-settings set gains
  `azure_truncate_on_insert` and `azure_create_new_file_on_insert`.

`s3` / `S3` rendering is unchanged.

## Tests

`aaiclick/data/object/test_blob.py` (or a sibling `test_blob_azure.py`):

- Module fixture starts Azurite (`npx -y -p azurite azurite-blob`, free port)
  and creates a container with `azure-storage-blob` (new dev dependency).
- A named collection `azurite` holds Azurite's well-known dev
  connection string. chdb: `CREATE NAMED COLLECTION` in the fixture. Server:
  a `config.d` file mounted in the `Blob storage dist` job, Azurite as a
  second container reachable from ClickHouse.
- Cover, via the existing S3 tests' shape: export, overwrite, view
  constraints, export → open round trip per extension, reserved settings,
  read-only across a task boundary (connection survives `schema_doc`),
  URL / connection validation.
- Server identity: no end-to-end test. Rendering is covered only through
  validation errors; the first real check is on Azure.

## Docs

- `docs/designs/blob_storage.md`: Azure section — URL grammar table, both
  credential paths, testing.
- `docs/user_guide/object.md` — "Blob Storage": `az://` and `connection=`.
- `docs/designs/future.md`: drop Azure from "GCS and Azure"; note per-object
  credentials exist for Azure only; record the unverified server-identity path.

# Left to the builder

- One test module or two; fixture plumbing for Azurite on both backends.
- Whether `ENGINE = AzureBlobStorage(lake, ...)` accepts a named collection
  with keyword overrides on chdb and the server — verify first; if not,
  record the gap and stop for a decision.
- Exact four-argument form for server identity (with or without explicit
  `'auto'` compression) — pick what ClickHouse parses.
- Whether `with_url_retry` needs Azure transient errors added.
- `compression` handling: leave to ClickHouse's suffix detection as for S3.
