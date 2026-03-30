# Technical Debt

## Warning Suppressions

- **clickhouse-connect FutureWarning** (`aaiclick/data/data_context.py`)
  - **Issue**: clickhouse-connect 0.6.x–0.8.x emits FutureWarnings from numpy datetime internals during queries.
  - **Debt**: Blocked on clickhouse-connect fix, expected in 1.0.0; remove global `warnings.filterwarnings` once upgraded.

## chdb `url()` Table Function

- **`ChdbClient._rewrite_external_urls()`** (`aaiclick/data/chdb_client.py`)
  - **Issue**: chdb's embedded ClickHouse hangs indefinitely on external HTTP/HTTPS URLs passed to the `url()` table function. The embedded HTTP client blocks the process with no timeout.
  - **Workaround**: `ChdbClient.command()` and `.query()` intercept any `url('https://...', 'fmt')` in SQL via regex, download the file to a `NamedTemporaryFile` via `asyncio.to_thread(urllib.request.urlretrieve)`, and rewrite the expression to `file('/tmp/x', 'fmt')` before execution. `NamedTemporaryFile` is used (not `TemporaryFile`) because chdb needs a filesystem path string.
  - **Debt**: Confirmed broken in chdb 4.1.2–4.1.6; no upstream fix. Remove this workaround once chdb's `url()` works reliably for external hosts. Track at [chdb-io/chdb](https://github.com/chdb-io/chdb).

## GitHub Actions

- **`FORCE_JAVASCRIPT_ACTIONS_TO_NODE24=true`** (`.github/workflows/test.yaml`)
  - **Issue**: `dorny/test-reporter@v2` targets Node.js 20, which GitHub Actions deprecates from June 2, 2026.
  - **Debt**: No v3 of the action exists yet. Remove the env var and pin to the new version once `dorny/test-reporter` releases Node.js 24 support.
