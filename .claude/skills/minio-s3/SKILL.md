---
name: minio-s3
description: Run a local MinIO (S3-compatible) server for blob-storage tests and spikes — start/stop, bucket setup, env vars. Use when a test or spike needs an s3:// endpoint, when AAICLICK_BLOB_TEST_URL is mentioned, or when the user says MinIO / S3 / object storage.
---

# MinIO for Blob Storage Tests

MinIO is the S3-compatible server used for blob-storage tests (local and the
GitHub Actions `minio` service). Blob tests skip unless `AAICLICK_BLOB_TEST_URL`
is set.

## Start

```bash
docker run -d --name minio -p 9000:9000 -p 9001:9001 \
  -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin \
  minio/minio:RELEASE.2025-09-07T16-13-09Z server /data --console-address :9001
docker run --rm --network host --entrypoint sh minio/mc:RELEASE.2025-08-13T08-35-41Z -c \
  "mc alias set local http://localhost:9000 minioadmin minioadmin && mc mb -p local/aaiclick-test"
```

MinIO stopped publishing community images in late 2025 — keep the tags
pinned; do not use `latest`.

## Run tests

```bash
export AAICLICK_BLOB_TEST_URL=http://localhost:9000/aaiclick-test
export AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin
uv run pytest -m blob
```

URLs in aaiclick code never carry credentials; ClickHouse / chdb pick them up
from the environment (`use_environment_credentials`). Design:
`docs/drafts/blob_storage_support.md`.

## Stop

```bash
docker rm -f minio
```
