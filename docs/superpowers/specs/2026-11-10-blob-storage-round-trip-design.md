Blob Storage Round-Trip
---

First slice of `docs/drafts/blob_storage_support.md`: a job exports its result
to a bucket and the next job opens it read-only, in place, on any cluster.

# Intent

Ephemeral compute and cross-job sharing with files as the contract and no
object-store secrets in aaiclick. Goals: `export` to an S3-compatible URL from
both backends; `open_object_from_url` over that URL as a read-only Object
queried in place. Non-goals for this slice (stay in `future.md`): named /
reopenable located objects, `copy()` sugar, GCS / Azure, `url()` fallback,
writable located objects.

# Approaches

| Approach                                  | Trade-off                                                              |
|-------------------------------------------|------------------------------------------------------------------------|
| **1. Export + located read-only Object**  | ~150 lines across `Schema`, table creation, guards; queries in place    |
| 2. Export + copy-on-open via `s3()`       | ~40 lines, but every open ingests — the thing the draft exists to avoid |

Chosen: 1.

# Credentials (spiked)

- chdb 26.7 and ClickHouse 26.11 both read `AWS_ACCESS_KEY_ID` /
  `AWS_SECRET_ACCESS_KEY` from the server process environment; SQL carries
  none.
- The 26.x server refuses ambient credentials in user queries unless the
  session setting `s3_allow_server_credentials_in_user_queries=1` is set.
  Every statement touching an S3 table needs it (CREATE, DESCRIBE, SELECT).
- **`AAICLICK_S3_CREDENTIALS=server`** opts in: a getter in
  `aaiclick/backend.py` validates it and `create_clickhouse_client` passes the
  setting as a connection default, so no SQL carries it. Unset: public buckets
  only. Any other value: `ValueError`. chdb ignores the variable — the
  embedded engine already uses the aaiclick process's own `AWS_*`.
- Credentials are given to the ClickHouse server process (env pair or IAM
  role), never to aaiclick. Deferred modes: `user` (keys in SQL, needs oplog
  redaction) and `collection:<name>` (named collection; verified to work on
  26.11 without the setting, but needs `users.xml` grants).
- MinIO / moto use bucket-style `http://host:port/bucket/key`; `s3://` resolves
  to AWS. Both are accepted; `url()` is not involved.

# Design

**`Schema`** gains `location: str | None = None` and `format: str | None =
None`. They ride into `table_registry.schema_doc` unchanged, so
`read_table_schema` sees them.

**Table creation** (`create_object`): when `schema.location` is set the engine
clause is `ENGINE = S3('<location>', '<format>')`, no `ORDER BY`, `engine` /
`order_by` ignored. Temp lifecycle only: the `t_` pointer is dropped at context
exit, the bucket untouched. `name` / `scope` with a location raise
`ValueError` for now.

**`open_object_from_url(url, columns=None, format=None, column_types=None)`**
in `aaiclick/data/object/url.py`: validates scheme in `{http, https, s3}`,
format from the extension via `format_for_extension` unless given, schema from
`DESCRIBE s3(url, fmt)` (all columns unless `columns`), fieldtype rules as
`create_object_from_url` (one column → array `value`, several → dict). No
`aai_id`. Returns the located Object.

**Read-only guard**: `Object.insert` and `insert_from_url` raise `ValueError`
when `schema.location` is set.

**`Object.export(path, overwrite=False)`**: a `://` target goes to
`export_query_to_url` (next to `export_query_to_file` in `ch_client.py`),
`INSERT INTO FUNCTION s3(url, fmt) <select>` on both backends, with
`s3_truncate_on_insert` = `overwrite`. Existing key without `overwrite` surfaces
ClickHouse's error. Returns the URL. Local paths unchanged.

# Tests

- `aaiclick/data/object/test_blob.py`, module-scoped in-process
  `moto.server.ThreadedMotoServer` on `0.0.0.0`, URL host from
  `AAICLICK_TEST_FILESERVER_HOST` (the `fileserver` pattern). `moto[server]`
  joins the `test` extra. No skip gate.
- CI: `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` added to the `clickhouse`
  service env in `_test-reusable.yaml` (moto accepts any pair); the test
  process sets the same pair for chdb.
- Cases: export → open round trip per format that is both input and output
  (Parquet, CSVWithNames, TSVWithNames, JSONEachRow, ORC, Avro); `where` +
  aggregation on a located object; `insert` raises; overwrite guard; bad
  scheme; `name=` with location raises; local export unchanged.

# Docs

`docs/user_guide/object.md` export section + a "Blob storage" section; the
draft moves to `docs/designs/blob_storage.md` trimmed to what shipped, its
deferred pieces listed in `future.md`; `claudeai-run` skill already covers
MinIO.
