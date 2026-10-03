Blob Storage Support
---

An `Object` can be backed by files in object storage — S3, GCS, Azure Blob —
instead of a ClickHouse MergeTree table. ClickHouse's object-store table
engines do the work: the Object keeps its table name, operators and views
keep producing SQL, and only the `ENGINE` clause changes. The MergeTree
default stays as it is; blob storage is opt-in per object.

Two pieces, files as the contract between them:

| Piece                    | What it does                                            | Goal served                                  |
|--------------------------|---------------------------------------------------------|----------------------------------------------|
| Read-only source Object  | Queries an existing path in place, nothing copied       | External data without ingestion              |
| Export to a bucket       | Writes an Object or View as a file, server-side         | Results that outlive the server and the job  |

A job exports its result, the URL travels to the next job as a plain string,
and that job opens it read-only on any cluster. That is the whole of
ephemeral compute and cross-job sharing.

Out of scope: replacing MergeTree for temp objects, a second query engine
(DuckDB or similar), and lake formats (Iceberg, Delta) — ClickHouse reads
them through the same engine family, so they can be added later as schemes.

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

# Read-Only Source Object

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

Rules for a located Object:

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

# Export to a Bucket

```python
url = await daily.export("s3://lake/reports/daily_2026_10.parquet")
# next job, any cluster
daily = await open_object_from_url(url)
```

`Object.export` accepts the same URL schemes as the read side. For a bucket
target it runs `INSERT INTO FUNCTION s3(url, format) SELECT ...` on the
server, so the bytes never pass through Python and chdb and remote
ClickHouse behave the same — unlike the local-file path, which streams over
HTTP for a remote server. Format comes from the extension as today, view
constraints are honored, and the return value is the URL written. Writing
to an existing key is an error unless the caller passes `overwrite=True`
(`s3_truncate_on_insert`).

Append is a directory of files: export to a new key under a prefix each
run, read the prefix with a glob. Changes: the bucket branch in
`export_query_to_file` (`aaiclick/data/data_context/ch_client.py`) and the
`export` docstring.

# Why No Writable Located Object

`create_object(location=)` with inserts landing in the bucket was
considered and rejected. The S3 engine's only form of append is a new file
per insert read back through a glob — the export-plus-glob layout above,
with worse semantics: unverified read-after-write across files, no
concurrent-writer safety, orphan files that need bucket lifecycle rules,
and derived locations plus registry changes to make named objects
reopenable. Export keeps files as the contract and needs none of it.
Revisit only if a workload needs in-place append to one bucket-backed table
from several tasks.

# Testing

- A MinIO service in the GitHub Actions job gives a real S3 endpoint for
  both backends; chdb ships the same engines as the server.
- Locally the located-object tests skip unless `AAICLICK_BLOB_TEST_URL` is
  set.
- Coverage: open and read one file per input format, a glob read, a
  `where` / aggregation over the located table, `copy()` into MergeTree,
  `insert` raising, reopen by name after `DROP TABLE`, and an export →
  open round trip per output format.
- GCS and Azure are covered by SQL-rendering tests only; their wire
  behavior is ClickHouse's.
