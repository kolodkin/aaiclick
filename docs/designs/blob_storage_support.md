Blob Storage Support
---

An `Object` can live in object storage — S3, GCS, Azure Blob — instead of a
ClickHouse MergeTree table. ClickHouse's object-store table engines do the
work: the Object keeps its table name, operators and views keep producing
SQL, and only the `ENGINE` clause changes. The MergeTree default stays as it
is; blob storage is opt-in per object.

Goals:

| Goal                  | What it means                                                                 |
|-----------------------|-------------------------------------------------------------------------------|
| Ephemeral compute     | A ClickHouse server can be recreated and persistent objects survive it        |
| Cross-job sharing     | A job's result is another job's input, possibly on another cluster            |
| Zero-copy reads       | External parquet / CSV / ORC datasets are queried in place, never ingested    |

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

# Entry Points

Three ways to get a located Object:

```python
# New, empty, aaiclick-owned — later inserts write files under the location
orders = await create_object(schema, name="orders", scope="global", location="s3://lake/p_orders/")

# Existing external data — schema inferred, read-only when the path is a glob
events = await open_object_from_url("s3://vendor/events/2026/*.parquet")

# Materialize a computed result into a bucket
daily = await events.group_by(...).copy(location="s3://lake/p_daily/")
```

`open_object_from_url` is the located counterpart of
`create_object_from_url`: same URL validation, same `DESCRIBE`-based schema
inference, but it creates the engine table over the path instead of copying
rows into MergeTree. The existing function keeps its copy semantics.
`Object.export` stays a local-file writer; writing to a bucket is
`copy(location=)`.

# Location Resolution

A location is either given explicitly or derived:

| Form     | Source                                                                | Used for                                   |
|----------|-----------------------------------------------------------------------|--------------------------------------------|
| Derived  | context `blob_base` + `<table_name>/`, e.g. `s3://lake/aaiclick/p_orders/` | Named objects aaiclick owns                |
| Explicit | `location=` argument                                                  | External data, or a caller-chosen path     |

Derived paths are what make `open_object("orders", scope="global")` work on
a fresh server or a second cluster: the name alone identifies the files. The
base URL is a `data_context()` / `orch_context()` argument, settable from
the environment like the ClickHouse URL.

# Scheme to Engine

| URL scheme           | Table engine        | Table function (schema inference) |
|----------------------|---------------------|-----------------------------------|
| `s3://`              | `S3`                | `s3()`                            |
| `gs://`              | `S3` (HMAC keys)    | `gcs()`                           |
| `az://`              | `AzureBlobStorage`  | `azureBlobStorage()`              |
| `http(s)://`         | none — copies via `url()` | `url()`                      |

Globs (`*`, `?`, `{a,b}`, `{1..9}`) and Hive-style partition discovery come
through untouched.

# Storage Layout and Append

An aaiclick-owned location is a directory, not a file. Each `insert` writes
a new file under it and reads glob the directory. In engine terms that is a
write path with a partition placeholder and a read path with `*`, plus the
`s3_create_new_file_on_insert` setting.

!!! warning "Verify before building append"
    That a table reads every file its own inserts wrote is the one engine
    behavior to confirm against the pinned ClickHouse first. If it does not
    hold, located objects are single-file and write-once: `create_object`
    plus one `insert`, or `copy`.

What a located Object cannot do, by engine limits rather than design:

| Capability            | MergeTree Object | Located Object                      |
|-----------------------|------------------|-------------------------------------|
| `ORDER BY`            | yes              | no — rows come back in file order   |
| Mutations / `DELETE`  | yes              | no                                  |
| `insert`              | yes              | append only; never on a glob path   |
| `aai_id` column       | optional         | optional; external data has none    |

# Registry and `open_object`

With `location` and `format` in `schema_doc`, `open_object` looks the name
up in `table_registry` and, if the table is missing on this server, runs
`CREATE TABLE IF NOT EXISTS` with the stored engine clause. Dropping an
engine table only drops the pointer, so a recreated server, or any cluster
sharing the registry database, reopens the same files.

# Lifecycle and Cleanup

`DROP TABLE` on an object-store engine never deletes files, and ClickHouse
has no SQL to delete bucket objects. Cleanup is therefore split:

- **Pointer**: the lifecycle worker drops the table as it does today.
- **Files**: a bucket lifecycle rule keyed on prefix — `t_` and `j_` prefixes
  expire on the job TTL, `p_` never. Documented as a deployment step, with
  the Helm and Compose templates carrying an example.

Temp objects (`t_`) are never located by default; a derived location only
applies to named scopes. An explicit `location=` on a temp object is
allowed but the caller owns the files.

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

# Testing

- A MinIO service in the GitHub Actions job gives a real S3 endpoint for
  both backends; chdb ships the same engines as the server.
- Locally the located-object tests skip unless `AAICLICK_BLOB_TEST_URL` is
  set.
- Coverage: one create → insert → read round trip per output format, one
  glob read, one `copy(location=)`, one `open_object` after `DROP TABLE`
  (the ephemeral-compute case), one cross-context reopen by name.
- GCS and Azure are covered by SQL-rendering tests only; their wire
  behavior is ClickHouse's.
