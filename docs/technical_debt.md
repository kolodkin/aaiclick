Technical Debt
---

# chdb `url()` Table Function

- **`ChdbClient._rewrite_external_urls()`** (`aaiclick/data/data_context/chdb_client.py`)
  - **Issue**: chdb's embedded ClickHouse hangs or misinterprets external HTTP/HTTPS URLs passed to the `url()` table function. The embedded HTTP client either blocks the process with no timeout (≤4.1.2) or treats URLs as named collections (26.1.0).
  - **Workaround**: `ChdbClient.command()` and `.query()` intercept any `url('https://...', 'fmt')` in SQL via regex, download the file to a `NamedTemporaryFile` via `asyncio.to_thread(urllib.request.urlretrieve)`, and rewrite the expression to `file('/tmp/x', 'fmt')` before execution. All URLs (including localhost) are rewritten consistently. `NamedTemporaryFile` is used (not `TemporaryFile`) because chdb needs a filesystem path string.
  - **Debt**: Confirmed broken in chdb 4.1.2 and 26.1.0; no upstream fix. Remove this workaround once chdb's `url()` works reliably. Track at [chdb-io/chdb](https://github.com/chdb-io/chdb).

# clickhouse-connect Async FutureWarning

- **`filterwarnings` in `pyproject.toml`** (`[tool.pytest.ini_options]`)
  - **Issue**: `clickhouse-connect>=0.15` emits a `FutureWarning` about the async client being a thread-pool wrapper, recommending the `[async]` prerelease.
  - **Workaround**: `warnings.catch_warnings()` around `get_async_client()` calls in `clickhouse_client.py` and `pg_cleanup.py`.
  - **Debt**: Remove the filter once `clickhouse-connect` 1.0 ships the native async client as default.

# GitHub Actions

- **`FORCE_JAVASCRIPT_ACTIONS_TO_NODE24=true`** (`.github/workflows/test.yaml`)
  - **Issue**: `dorny/test-reporter@v2` targets Node.js 20, which GitHub Actions deprecates from June 2, 2026.
  - **Debt**: No v3 of the action exists yet. Remove the env var and pin to the new version once `dorny/test-reporter` releases Node.js 24 support.
