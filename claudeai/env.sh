# Connection settings for the local services started by claudeai/run.sh.
# Same credentials as the CI services in .github/workflows/_test-reusable.yaml.
# Source it before running tests or examples against the distributed backend.
export AAICLICK_CH_URL="clickhouse://default:click123@localhost:8123/default"
export AAICLICK_SQL_URL="postgresql+asyncpg://aaiclick:secret@localhost:5432/aaiclick"
export AAICLICK_JWT_SECRET="local-dev-jwt-secret-key-at-least-32-bytes-long"
# MinIO: ClickHouse and chdb read the AWS_* pair from the environment, so URLs
# carry no credentials.
export AAICLICK_BLOB_TEST_URL="http://localhost:9100/aaiclick-test"
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin
