---
name: minio-s3
description: Run a local MinIO (S3-compatible) server for blob-storage tests and spikes — start/stop, bucket, env vars. Use when a test or spike needs an S3 endpoint, when AAICLICK_BLOB_TEST_URL is mentioned, or when the user says MinIO / S3 / object storage.
---

# MinIO for Blob Storage Tests

`claudeai/minio.sh` builds MinIO from source with Go (MinIO stopped publishing
binaries, Docker images and `mc` in 2025) into `.cache/minio/` and starts it on
port 9000 with a `aaiclick-test` bucket. First build takes ~2 minutes; later
runs reuse the binary.

```bash
claudeai/minio.sh          # start — prints the exports below
claudeai/minio.sh stop
```

On a developer machine prefix with `CLAUDEAI_ALLOW_LOCAL=1`. Override
`MINIO_PORT`, `MINIO_BUCKET`, `MINIO_VERSION` (a `RELEASE.…` tag, default
`latest`) through the environment.

## Run tests

```bash
export AAICLICK_BLOB_TEST_URL=http://localhost:9000/aaiclick-test
export AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin
uv run pytest -m blob
```

URLs in aaiclick code never carry credentials: ClickHouse and chdb read
`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` from the environment by default
(verified with chdb 26.7 — no setting needed). Bucket-style
`http://host:9000/bucket/key` URLs work against MinIO; `s3://bucket/key`
resolves to AWS only. Design: `docs/drafts/blob_storage_support.md`.

## Bucket operations without mc

```bash
curl --aws-sigv4 aws:amz:us-east-1:s3 --user minioadmin:minioadmin \
  http://localhost:9000/aaiclick-test/          # list objects
```
