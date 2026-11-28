# Connection settings for the local services started by claudeai/run.sh.
# Same credentials as the CI services in .github/workflows/_test-reusable.yaml.
# Source it before running tests or examples against the distributed backend.
# All or nothing: every endpoint assumes run.sh started its service this
# session, so after a container resume rerun run.sh first.
export AAICLICK_CH_URL="clickhouse://default:click123@localhost:8123/default"
export AAICLICK_SQL_URL="postgresql+asyncpg://aaiclick:secret@localhost:5432/aaiclick"
export AAICLICK_JWT_SECRET="local-dev-jwt-secret-key-at-least-32-bytes-long"
# MinIO on :9100 for blob-storage checks (bucket aaiclick-test). The server
# has the same pair in its own environment; chdb reads it from this one.
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin
# Azurite on :10000: test_blob.py shares it instead of starting its own via npx.
export AAICLICK_TEST_AZURITE_ENDPOINT=http://127.0.0.1:10000
