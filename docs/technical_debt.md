Technical Debt
---

# `insert()` / `concat()` preserve source `aai_id` values

- **`_insert_source()`** (`aaiclick/data/object/ingest.py`)
  - **Issue**: `insert()` and `concat()` copy the source `aai_id` values verbatim (`SELECT aai_id, ... FROM source`). If the same source is inserted twice into one table, or two copies of the same data are concatenated, the target table ends up with **duplicate `aai_id` values**. Since `data()` reads with `ORDER BY aai_id`, duplicates make row ordering non-deterministic.
  - **Context**: `copy()` already generates fresh `aai_id` values (excludes `aai_id` from INSERT, lets `DEFAULT generateSnowflakeID()` fire). `insert()` and `concat()` should follow the same pattern for consistency.
  - **Fix**: Exclude `aai_id` from the SELECT in `_insert_source()` so fresh Snowflake IDs are generated for every inserted row. This ensures globally unique IDs and deterministic `ORDER BY aai_id` ordering. The original CLAUDE.md guidance ("preserve existing Snowflake IDs from source data") should be revised — ClickHouse does not guarantee row order without `ORDER BY`, so `aai_id` uniqueness is the only ordering contract.

# chdb `LowCardinality(String)` sort is ~x10 slower than plain `String`

- **`INSERT...SELECT...ORDER BY` on `LowCardinality(String)` columns is ~10x slower than `String` in chdb**
  - **Symptom**: Sort benchmark shows x15 aaiclick overhead vs native chdb. Root cause: aaiclick infers `LowCardinality(String)` for dictionary-encoded PyArrow string columns, while the native benchmark uses plain `String`. The performance gap is entirely in chdb's handling of `LowCardinality` during sorted inserts (822ms vs 83ms at 1M rows).
  - **Confirmed**: Identical `INSERT...SELECT...ORDER BY` on the same chdb Session takes 130ms with `String` columns and 1.6 sec with `LowCardinality(String)` — same data, same engine, same query.
  - **This is a chdb bug**: `LowCardinality` should be faster for sorting (dictionary encoding reduces comparison cost), not 10x slower. Regular ClickHouse server does not exhibit this behavior.
  - **Workaround options**: (1) Stop inferring `LowCardinality` in `create_object_from_value()`, (2) Cast to plain `String` before sorted operations, (3) Wait for chdb fix.
  - **Impact**: Sort, filter+copy, and any operation that materializes large `LowCardinality(String)` columns via `INSERT...SELECT`.

# chdb `url()` Table Function

- **`ChdbClient._rewrite_external_urls()`** (`aaiclick/data/data_context/chdb_client.py`)
  - **Issue**: chdb's embedded ClickHouse hangs or misinterprets external HTTP/HTTPS URLs passed to the `url()` table function. The embedded HTTP client either blocks the process with no timeout (≤4.1.2) or treats URLs as named collections (26.1.0).
  - **Workaround**: `ChdbClient.command()` and `.query()` intercept any `url('https://...', 'fmt')` in SQL via regex, download the file to a `NamedTemporaryFile` via `asyncio.to_thread(urllib.request.urlretrieve)`, and rewrite the expression to `file('/tmp/x', 'fmt')` before execution. All URLs (including localhost) are rewritten consistently. `NamedTemporaryFile` is used (not `TemporaryFile`) because chdb needs a filesystem path string.
  - **Debt**: Confirmed broken in chdb 4.1.2 and 26.1.0; no upstream fix. Remove this workaround once chdb's `url()` works reliably. Track at [chdb-io/chdb](https://github.com/chdb-io/chdb).

# GitHub Actions

- **`FORCE_JAVASCRIPT_ACTIONS_TO_NODE24=true`** (`.github/workflows/test.yaml`)
  - **Issue**: `dorny/test-reporter@v2` targets Node.js 20, which GitHub Actions deprecates from June 2, 2026.
  - **Debt**: No v3 of the action exists yet. Remove the env var and pin to the new version once `dorny/test-reporter` releases Node.js 24 support.
