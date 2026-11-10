Blob Storage
---

An `Object` can point at a file in S3-compatible object storage instead of
owning a ClickHouse table, and any Object or View can be exported to a bucket.
Files are the contract: a job exports its result, the URL travels to the next
job as a plain string, and that job opens it read-only on any cluster. User
guide: `docs/user_guide/object.md` — "Blob Storage". Deferred pieces:
`docs/designs/future.md` — "Blob Storage Support".

# Located Objects

**Implementation**: `aaiclick/data/object/url.py`

A *located* Object's table is an `ENGINE = S3(url, format)` pointer. `Schema`
carries `location` and `format`; everything above the engine clause is
unchanged, so operators, views, joins and aggregations run SQL against the
table name as for any Object, and computed results land in temp tables.

- **Schema**: `DESCRIBE s3(url, format)`; the pointer keeps the file's column
  names, so the Object is dict-shaped unless the only column is `value`.
- **Read-only**: `insert` and `insert_from_url` raise on `Schema.location`.
- **Temp only**: a `t_` pointer dropped at context exit; `name` raises. The
  bucket is never touched.
- **No `aai_id`** and no guaranteed row order.

# Export to a Bucket

**Implementation**: `aaiclick/data/object_context/ch_client.py`

A URL target runs `INSERT INTO FUNCTION s3(url, format) SELECT ...` on both
backends, so bytes never pass through Python — unlike the local-file path,
which streams over HTTP for a remote server. `s3_truncate_on_insert` follows
`overwrite`; an existing key is otherwise ClickHouse's error.

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

The compose and Helm scaffolds ship the file too, mounted but with the profile
commented out: a scaffold has no bucket credentials, so there is nothing to
lend until the operator supplies them.

`s3://bucket/key` resolves to AWS; S3-compatible servers (MinIO) take
bucket-style `http(s)://host/bucket/key`.

# Testing

`aaiclick/data/object/test_blob.py` runs against an in-process moto server on
both backends; a remote ClickHouse reaches it through
`AAICLICK_TEST_FILESERVER_HOST`. On a remote server the module probes
`system.settings` and skips unless the profile opts in, so it runs in the
dedicated `Blob storage dist` CI job — which starts ClickHouse with the
`users.d` file mounted and fails on a skip — and skips in the jobs that use
stock service containers. `claudeai/run.sh` uses the same `users.d` file and
starts a real MinIO for manual checks.
