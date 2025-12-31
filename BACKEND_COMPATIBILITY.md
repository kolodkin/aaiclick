# Backend Compatibility: chdb and clickhouse-connect

aaiclick now supports two ClickHouse backends with a unified interface:

## Backends

### 1. clickhouse-connect (Default)
- Remote ClickHouse server connections
- Full async/await support
- Network-based queries

### 2. chdb (Embedded)
- In-process ClickHouse engine
- Local .chdb file storage (MergeTree only)
- No network overhead
- Ideal for embedded/local workloads

## Configuration

Set the backend using the `AAICLICK_BACKEND` environment variable:

```bash
# Use clickhouse-connect (default)
export AAICLICK_BACKEND=clickhouse-connect
export CLICKHOUSE_HOST=localhost
export CLICKHOUSE_PORT=8123
export CLICKHOUSE_USER=default
export CLICKHOUSE_PASSWORD=""
export CLICKHOUSE_DB=default

# Use chdb (embedded)
export AAICLICK_BACKEND=chdb
export CHDB_SESSION_PATH=mydata.chdb  # Optional: path to .chdb file
```

## Result Format Compatibility

Both backends return results through the unified `QueryResult` interface:

```python
import aaiclick

# Works with both backends
client = await aaiclick.get_client()
result = await client.query("SELECT * FROM table")

# Access results the same way (compatible with clickhouse-connect)
rows = result.result_rows  # List[Tuple[...]]
```

## API Compatibility

The adapter provides a unified interface:

```python
# Query with results
result = await client.query("SELECT * FROM table")
rows = result.result_rows

# Command without results
await client.command("CREATE TABLE test (...)")
await client.command("INSERT INTO test VALUES (...)")
await client.command("DROP TABLE test")
```

## Implementation Details

### chdb Backend
- Uses `chdb.session.Session()` for persistent storage
- Converts results from Arrow format to list of tuples
- MergeTree engine only for persistent tables
- Synchronous execution wrapped in async interface

### clickhouse-connect Backend
- Uses `clickhouse_connect.get_async_client()`
- Native async/await support
- Direct access to `result.result_rows`
- Full ClickHouse server feature support

## Migration Guide

No code changes required! The adapter ensures both backends work identically:

```python
# Before (clickhouse-connect only)
from clickhouse_connect import get_async_client
client = await get_async_client(...)
result = await client.query("SELECT 1")
rows = result.result_rows

# After (works with both backends)
import aaiclick
client = await aaiclick.get_client()
result = await client.query("SELECT 1")
rows = result.result_rows  # Same interface!
```

## Choosing a Backend

**Use clickhouse-connect when:**
- Connecting to remote ClickHouse servers
- Need distributed query execution
- Require all ClickHouse engine types
- Working with large-scale production deployments

**Use chdb when:**
- Embedded/local data processing
- No server infrastructure required
- Single-node analytics
- Development and testing
- MergeTree tables only

## Limitations

### chdb
- MergeTree engine only for persistent tables
- Single-node execution
- Limited to local file storage

### clickhouse-connect
- Requires network connection
- Server infrastructure needed
- Potential network latency
