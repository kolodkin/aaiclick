Technical Debt
---

# chdb `url()` Table Function

- **`ChdbClient._rewrite_external_urls()`** (`aaiclick/data/data_context/chdb_client.py`)
  - **Issue**: chdb's embedded ClickHouse hangs or misinterprets external HTTP/HTTPS URLs passed to the `url()` table function. The embedded HTTP client either blocks the process with no timeout (≤4.1.2) or treats URLs as named collections (26.1.0).
  - **Workaround**: `ChdbClient.command()` and `.query()` intercept any `url('https://...', 'fmt')` in SQL via regex, download the file to a `NamedTemporaryFile` via `asyncio.to_thread(urllib.request.urlretrieve)`, and rewrite the expression to `file('/tmp/x', 'fmt')` before execution. `NamedTemporaryFile` is used (not `TemporaryFile`) because chdb needs a filesystem path string.
  - **Debt**: Confirmed broken in chdb 4.1.2 and 26.1.0; no upstream fix. Remove this workaround once chdb's `url()` works reliably for external hosts. Track at [chdb-io/chdb](https://github.com/chdb-io/chdb).

# Flaky `test_claim_respects_group_dependency` in orch-dist

- **`claim_next_task()`** (`aaiclick/orchestration/execution/claiming.py`)
  - **Issue**: `claim_next_task` claims the next PENDING task across all jobs — correct by design for production workers. But under `pytest -n auto`, multiple tests run sequentially in the same xdist worker (same DB). Leftover PENDING tasks from a prior test can be claimed by the next test, causing assertion failures on task ID comparisons.
  - **Observed**: `test_claim_respects_group_dependency` (`test_worker.py:429`) intermittently fails on orch-dist CI — the worker claims a `failing_task` from a prior test instead of the expected `simple_task`.
  - **Fix**: Improve test isolation — drain or cancel leftover tasks at the start of each test that calls `claim_next_task`, or add a per-test cleanup fixture that cancels all jobs created during the test.

# GitHub Actions

- **`FORCE_JAVASCRIPT_ACTIONS_TO_NODE24=true`** (`.github/workflows/test.yaml`)
  - **Issue**: `dorny/test-reporter@v2` targets Node.js 20, which GitHub Actions deprecates from June 2, 2026.
  - **Debt**: No v3 of the action exists yet. Remove the env var and pin to the new version once `dorny/test-reporter` releases Node.js 24 support.
