Blob Storage
---

# Blob Storage

<!-- req: open-object-from-url -->
`open_object_from_url` opens a file in S3-compatible storage as an Object and
queries it in place — nothing is ingested. With `export` it lets one job hand
a result to the next as a plain URL:

```python
url = await daily.export("s3://lake/reports/daily.parquet")

# next job, any cluster
daily = await open_object_from_url(url)
await daily.view(where="country = 'US'")["revenue"].sum().data()
```

<!-- req: open-object-from-url-rules -->
- **Schema** is inferred from the file; `columns=` narrows it,
  `column_types=` skips inference.
- **Format** follows the extension as in [Export](export.md#supported-extensions).
  `format=` takes any ClickHouse input format; with an unmapped or missing
  extension ClickHouse detects it from the content.
- **CSV / TSV headers**: ClickHouse detects a header row; a headerless file
  exposes columns as `c1`, `c2`, …. Detection cannot tell a header from data
  when every column is a string — pass `format="CSVWithNames"` there.
- **Read-only**: `insert` and `insert_from_url` raise.
- **Temporary**: the Object is a pointer dropped at context exit; the file is
  never touched.
- **Shape**: a dict Object, or an array when the file's only column is
  `value` (what an exported array has).
- **URLs**: `s3://bucket/key` for AWS, `http(s)://host/bucket/key` for
  S3-compatible servers such as MinIO, `az://container/path` for
  [Azure](#azure). Globs pass through to ClickHouse; userinfo and query
  parameters (signed URLs) are rejected.

## Local files

<!-- req: open-local-file -->
On the embedded chdb backend the same call opens a file on local disk, so an
exported path feeds straight back in. A remote ClickHouse raises `ValueError`,
as local export does.

```python
path = await daily.export("/data/reports/daily.parquet")
daily = await open_object_from_url(path)
```

## Credentials

URLs never carry credentials; ClickHouse resolves them from its own
environment (`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`, or an IAM role).

| Backend           | Where the credentials go         | Extra setup                |
|-------------------|----------------------------------|----------------------------|
| chdb (embedded)   | the aaiclick process environment | none                       |
| ClickHouse server | the server process environment   | a `users.xml` profile line |

A ClickHouse server refuses to use its own S3 identity for user queries unless
the user's profile opts in. Mount this as
`/etc/clickhouse-server/users.d/s3.xml` (the repo ships it at
`docker/clickhouse/users.d/s3.xml`):

```xml
<clickhouse>
  <profiles>
    <default>
      <s3_allow_server_credentials_in_user_queries>1</s3_allow_server_credentials_in_user_queries>
    </default>
  </profiles>
</clickhouse>
```

The compose stack and Helm chart mount the same file with the profile commented
out, next to commented `AWS_*` entries for the ClickHouse container: uncomment
both once you have bucket credentials.

!!! warning "Without the profile line a private bucket fails with `ACCESS_DENIED`"
    Only public buckets work on a server that has not opted in. aaiclick has
    no setting of its own for this: lending the server's identity is the
    ClickHouse operator's decision.

<!-- req: s3-named-collection -->
To use different keys per bucket instead, name a ClickHouse named collection
with `connection=` — no profile line needed. Define it in `config.d` with only
the keys, and grant it as for [Azure](#azure):

```xml
<clickhouse>
  <named_collections>
    <lake>
      <access_key_id>...</access_key_id>
      <secret_access_key>...</secret_access_key>
    </lake>
  </named_collections>
</clickhouse>
```

```python
url = await daily.export("s3://lake/reports/daily.parquet", connection="lake")
```

On chdb `connection` is ignored for S3 — the process env is used — so the same
code runs locally.

## Azure

<!-- req: azure-blob-storage -->
`az://container/path` reads and writes Azure Blob Storage through a ClickHouse
named collection — the account endpoint and key never appear in a URL or in the
SQL aaiclick sends. Pass its name as `connection=`:

```python
url = await daily.export("az://reports/2026/daily.parquet", connection="lake")
daily = await open_object_from_url(url, connection="lake")
```

<!-- req: azure-credentials -->
| Backend           | Where the credentials go                                        |
|-------------------|-----------------------------------------------------------------|
| chdb (embedded)   | `AZURE_STORAGE_CONNECTION_STRING` in the aaiclick environment   |
| ClickHouse server | a named collection the operator defines; `connection=` required |

On chdb every `connection` name reads the one environment variable, so job code
written for a server runs unchanged; `connection` may be omitted there. chdb
saves the collection, key included, in plain text under its data directory. On a
server, define the collection in `config.d` with only the endpoint and
credentials — aaiclick sends `container`, `blob_path` and `format` itself — and
grant it to the aaiclick user:

```xml
<clickhouse>
  <named_collections>
    <lake>
      <connection_string>DefaultEndpointsProtocol=https;AccountName=...;AccountKey=...;EndpointSuffix=core.windows.net</connection_string>
    </lake>
  </named_collections>
</clickhouse>
```

```sql
GRANT NAMED COLLECTION ON lake TO aaiclick
```

!!! warning "Without the grant every `az://` call fails with `ACCESS_DENIED`"
    ClickHouse checks `NAMED COLLECTION ON <name>` on each query. A collection
    that stores `container` or `blob_path` needs `SHOW NAMED COLLECTIONS SECRETS`
    as well, since aaiclick overrides them.
