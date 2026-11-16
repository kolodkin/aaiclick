Blob Storage
---

An `Object` can point at a file in S3-compatible object storage or Azure Blob
Storage instead of
owning a ClickHouse table, and any Object or View can be exported to a bucket.
Files are the contract: a job exports its result, the URL travels to the next
job as a plain string, and that job opens it read-only on any cluster. User
guide: `docs/user_guide/object.md` — "Blob Storage". Deferred pieces:
`docs/designs/future.md` — "Blob Storage Support".

# Located Objects

**Implementation**: `aaiclick/data/object/url.py`

A *located* Object's table is an `ENGINE = S3(url, format)` pointer
(`AzureBlobStorage` for `az://`, see [Azure](#azure)). `Schema`
carries `location` and `format`; everything above the engine clause is
unchanged, so operators, views, joins and aggregations run SQL against the
table name as for any Object, and computed results land in temp tables.

- **Schema**: `DESCRIBE s3(url, format)`; the pointer keeps the file's column
  names, so the Object is dict-shaped unless the only column is `value`.
- **Format**: the extension's mapped format, else `auto` so ClickHouse
  detects it. A `format=` name is only checked to be a bare identifier —
  it is inlined into SQL — and ClickHouse rejects one it cannot read.
- **Read-only**: `insert` and `insert_from_url` raise on `Schema.location`.
- **Temp only**: a `t_` pointer dropped at context exit; `name` raises. The
  bucket is never touched.
- **No `aai_id`** and no guaranteed row order.
- **Local files (chdb only)**: a path instead of a URL makes the pointer
  `ENGINE = File(format, path)` and the schema `DESCRIBE file(...)`. The path
  is resolved to absolute on open. A remote server would read its own disk,
  so a path raises there.

# Export to a Bucket

**Implementation**: `aaiclick/data/object_context/ch_client.py`

A URL target runs `INSERT INTO FUNCTION s3(url, format) SELECT ...` on both
backends, so bytes never pass through Python. A local path is chdb-only
(`INSERT INTO FUNCTION file()`): a remote server cannot reach the client's
disk, so it raises. `s3_truncate_on_insert` follows
`overwrite`; an existing key is otherwise ClickHouse's error.

The format comes from the extension, mapped as ClickHouse maps it (`.csv` is
headerless `CSV`), or from `format=`, which takes any output format.

# Credentials

aaiclick never handles object-store secrets: no SQL it renders carries them
(a URL with userinfo or query parameters is rejected), so the operation log
and ClickHouse's query log stay clean. ClickHouse uses
its own identity — `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` in its
process environment, or an IAM role.

| Backend           | Behavior                                                                  |
|-------------------|---------------------------------------------------------------------------|
| chdb              | Reads the aaiclick process environment; no setting needed                 |
| ClickHouse server | Refuses its ambient identity for user queries unless the profile opts in  |

The server check runs on every statement that touches an S3 table (`CREATE`,
`DESCRIBE`, `SELECT`), so the opt-in is a profile setting, not SQL:
`s3_allow_server_credentials_in_user_queries=1` in the user's profile,
shipped as `docker/clickhouse/users.d/s3.xml`. aaiclick has no setting of its
own — lending the server's identity is the operator's decision, and a client
that could switch it on per connection would make the guard moot.

The compose stack and Helm chart ship the file too, mounted but with the profile
commented out: they have no bucket credentials, so there is nothing to
lend until the operator supplies them.

`s3://bucket/key` resolves to AWS; S3-compatible servers (MinIO) take
bucket-style `http(s)://host/bucket/key`.

**Implementation**: `aaiclick/data/blob_sql.py`

`connection="lake"` names a ClickHouse named collection holding the keys
instead: `s3(lake, url = '...', format = '...')`, and `S3(...)` likewise for
the pointer engine. Each bucket can use its own keys, and the opt-in above does
not apply. The collection holds only `access_key_id` / `secret_access_key` —
aaiclick sends `url` and `format`. On chdb `connection` is ignored for S3: no
server config can hold a collection, and the process env already serves. The
name is still stored in `Schema.connection`, so job code runs unchanged on both
backends.

# Azure

`az://container/path` renders as `azureBlobStorage(<collection>, container =
'...', blob_path = '...', format = '...')` for open, the pointer engine and
export. The endpoint and key live in a ClickHouse named collection, so no
secret reaches aaiclick's SQL, the operation log or `schema_doc`;
`Schema.connection` stores only the name.

| Backend           | Collection                                                                       |
|-------------------|----------------------------------------------------------------------------------|
| ClickHouse server | Operator-defined in `config.d`; `connection=` required; needs `NAMED COLLECTION` |
| chdb              | Defined from `AZURE_STORAGE_CONNECTION_STRING` once per client and key, any name |

- **chdb**: `CREATE ... IF NOT EXISTS` then `ALTER ... SET`, so a collection
  persisted by an earlier session follows the current env. These two
  statements alone carry the key and are never oplog-recorded, but chdb saves
  the collection, key included, as plain text under its data directory.
- **Collection contract**: endpoint and credentials only. aaiclick always adds
  `container`, `blob_path` and `format`; overriding a stored key needs
  `SHOW NAMED COLLECTIONS SECRETS`.
- **Export**: `overwrite` maps to `azure_truncate_on_insert`; it and
  `azure_create_new_file_on_insert` are reserved, as for S3.
- **Not supported**: the server's own managed / workload identity —
  `docs/designs/future.md` — "Blob Storage Support".

# Testing

`aaiclick/data/object/test_blob.py` runs against an in-process moto server on
both backends; a remote ClickHouse reaches it through
`AAICLICK_TEST_FILESERVER_HOST`. On a remote server the module probes
`system.settings` and skips unless the profile opts in, so it runs in the
dedicated `Blob storage dist` CI job — which starts ClickHouse with the
`users.d` file mounted and fails on a skip — and skips in the jobs that use
stock service containers. `claudeai/run.sh` uses the same `users.d` file and
starts a real MinIO for manual checks.

Its `store` fixture runs the shared export / open tests against S3 and Azure.
Azure uses Azurite, Microsoft's Azure Storage emulator: started with `npx`
locally (chdb, connection string in the env), or the container the
`Blob storage dist` job runs, whose ClickHouse mounts `docker/clickhouse/test/`
to define and grant the `azurite_test` collection.
