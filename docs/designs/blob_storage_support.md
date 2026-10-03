Blob Storage Support
---

An `Object` can be backed by files in object storage — S3, GCS, Azure Blob —
instead of a ClickHouse MergeTree table. ClickHouse's object-store table
engines do the work: the Object keeps its table name, operators and views
keep producing SQL, and only the `ENGINE` clause changes. The MergeTree
default stays as it is; blob storage is opt-in per object.

Delivered in two phases:

| Phase | Scope                                                              | Goal served                      |
|-------|--------------------------------------------------------------------|----------------------------------|
| 1     | Read-only Object over an existing path — zero-copy external reads  | Query external data in place     |
| 2     | Writable location: aaiclick-owned objects whose files live in a bucket | Ephemeral compute, cross-job sharing |

Out of scope for both: replacing MergeTree for temp objects, a second query
engine (DuckDB or similar), and lake formats (Iceberg, Delta) — ClickHouse
reads them through the same engine family, so they can be added later as
schemes.

---

# Concept: Location

A *located* Object's table is an `S3` / `AzureBlobStorage` engine table
pointing at a URL instead of a server-local MergeTree. Everything above
the engine clause is unchanged:

- `Object.table`, `scope`, `persistent` and the task-ref wire shape in
  `aaiclick/data/object/refs.py` see only a table name.
- Operators, views, joins and aggregations run SQL against the table.
  Computed results land in temp tables as today.
- `table_registry.schema_doc` stores the location and format alongside the
  schema, so the pointer can be rebuilt on any server.

`Schema` gains two optional fields, `location` (URL) and `format`
(ClickHouse format name, default `Parquet`). `get_engine_clause()` renders
the engine from them.

| URL scheme           | Table engine        | Table function (schema inference) |
|----------------------|---------------------|-----------------------------------|
| `s3://`              | `S3`                | `s3()`                            |
| `gs://`              | `S3` (HMAC keys)    | `gcs()`                           |
| `az://`              | `AzureBlobStorage`  | `azureBlobStorage()`              |
| `http(s)://`         | none — copies via `url()` | `url()`                      |

Globs (`*`, `?`, `{a,b}`, `{1..9}`) and Hive-style partition discovery come
through untouched.

# Credentials

aaiclick never handles object-store secrets. URLs carry none, and the SQL
text — which reaches the operation log and ClickHouse's query log — stays
clean. ClickHouse resolves credentials server-side through, in order of
preference:

1. Named collections in the server config (per bucket or per provider).
2. Environment credentials (`use_environment_credentials`) or the instance
   IAM role / workload identity.
3. Anonymous access for public buckets.

The deploy templates gain a commented named-collection example; chdb reads
the same settings from its session config.

---

# Phase 1 — Read-Only Source

```python
# Existing external data — schema inferred, nothing copied
events = await open_object_from_url("s3://vendor/events/2026/*.parquet")
total = await events.where("country = 'US'").sum()
```

`open_object_from_url` is the located counterpart of
`create_object_from_url`: same URL validation, same `DESCRIBE`-based schema
inference and `columns` / `column_types` overrides, but it creates the
engine table over the path instead of copying rows into MergeTree. The
existing function keeps its copy semantics; `copy()` on a located Object is
the explicit way to ingest.

Rules for a Phase 1 Object:

- **Read-only.** `insert` and `insert_from_url` raise. Enforced in Python
  on `Schema.location`, independent of whether the path is a glob.
- **Temp by default.** The table is a `t_` pointer dropped at context exit;
  nothing in the bucket is touched. `name` / `scope` work as for any
  Object, and a named one is reopenable by name on another server because
  `schema_doc` carries the location.
- **No `aai_id`.** External data has none; aaiclick already supports
  objects without it.
- **File order.** Rows come back in file order; there is no `ORDER BY`.

Changes: `Schema` fields, `get_engine_clause()`, a scheme table next to
`FORMATS` in `aaiclick/data/formats.py`, `open_object_from_url` in
`aaiclick/data/object/url.py`, the read-only guard in `Object`, the
deploy-template credentials example, and `docs/user_guide/object.md`.

# Phase 2 — Writable Location

Deferred until Phase 1 is in use. Design notes kept so the first phase does
not paint it in:

- **Entry points.** `create_object(schema, location=url)` for a new empty
  object, and `copy(location=url)` to materialize a result into a bucket.
  `Object.export` stays a local-file writer.
- **Derived locations.** A context `blob_base` plus `<table_name>/` so
  `open_object("orders", scope="global")` finds the files by name on any
  cluster; explicit `location=` overrides.
- **Append layout.** A directory per object: inserts write new files
  through a partition-placeholder path with `s3_create_new_file_on_insert`,
  reads glob the directory. Whether a table reads every file its own
  inserts wrote must be verified on the pinned ClickHouse first; if not,
  located objects are single-file and write-once.
- **Engine limits.** No `ORDER BY`, no mutations or `DELETE`, append only.
- **Cleanup.** `DROP TABLE` only drops the pointer. Files expire through a
  bucket lifecycle rule keyed on prefix (`t_` / `j_` on the job TTL, `p_`
  never), documented as a deployment step.

# Testing

- A MinIO service in the GitHub Actions job gives a real S3 endpoint for
  both backends; chdb ships the same engines as the server.
- Locally the located-object tests skip unless `AAICLICK_BLOB_TEST_URL` is
  set.
- Phase 1 coverage: open and read one file per input format, a glob read,
  a `where` / aggregation over the located table, `copy()` into MergeTree,
  `insert` raising, and reopen by name after `DROP TABLE`.
- GCS and Azure are covered by SQL-rendering tests only; their wire
  behavior is ClickHouse's.
