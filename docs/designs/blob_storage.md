Blob Storage
---

Credentials (secrets never in SQL): `docs/architecture/security.md`.

An `Object` can point at a file in S3-compatible object storage or Azure Blob
Storage instead of
owning a ClickHouse table, and any Object or View can be exported to a bucket.
Files are the contract: a job exports its result, the URL travels to the next
job as a plain string, and that job opens it read-only on any cluster. User
guide: `docs/user_guide/object/blob_storage.md` — "Blob Storage". Deferred pieces:
`docs/designs/future.md` — "Blob Storage Support".

# Located Objects

**Implementation**: `aaiclick/data/object/url.py`

<!-- req: located-object-pointer -->
A *located* Object's table is an `ENGINE = S3(url, format)` pointer
(`AzureBlobStorage` for `az://`, see [Azure](#azure)). `Schema`
carries `location` and `format`; everything above the engine clause is
unchanged, so operators, views, joins and aggregations run SQL against the
table name as for any Object, and computed results land in temp tables.

<!-- req: located-object-rules -->
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

<!-- req: export-to-bucket -->
A URL target runs `INSERT INTO FUNCTION s3(url, format) SELECT ...` on both
backends, so bytes never pass through Python. A local path is chdb-only
(`INSERT INTO FUNCTION file()`): a remote server cannot reach the client's
disk, so it raises. `s3_truncate_on_insert` follows
`overwrite`; an existing key is otherwise ClickHouse's error.

<!-- req: export-format -->
The format comes from the extension, mapped as ClickHouse maps it (`.csv` is
headerless `CSV`), or from `format=`, which takes any output format.

# Azure

<!-- req: azure-rendering -->
`az://container/path` renders as `azureBlobStorage(<collection>, container =
'...', blob_path = '...', format = '...')` for open, the pointer engine and
export. The endpoint and key live in a ClickHouse named collection, so no
secret reaches aaiclick's SQL, the operation log or `schema_doc`;
`Schema.connection` stores only the name.

<!-- req: azure-collection-per-backend -->
| Backend           | Collection                                                                       |
|-------------------|----------------------------------------------------------------------------------|
| ClickHouse server | Operator-defined in `config.d`; `connection=` required; needs `NAMED COLLECTION` |
| chdb              | Defined from `AZURE_STORAGE_CONNECTION_STRING` once per client and key, any name |

<!-- req: azure-collection-handling -->
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
