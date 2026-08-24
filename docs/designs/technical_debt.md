Technical Debt
---

# chdb `url()` Table Function

- **`ChdbClient._rewrite_external_urls()`** (`aaiclick/data/data_context/chdb_client.py`)
  - **Issue**: chdb's embedded ClickHouse hangs or misinterprets external HTTP/HTTPS URLs passed to the `url()` table function. The embedded HTTP client either blocks the process with no timeout (≤4.1.2) or treats URLs as named collections (26.1.0).
  - **Workaround**: `ChdbClient.command()` and `.query()` intercept any `url('https://...', 'fmt')` in SQL via regex, download the file to a `NamedTemporaryFile` via `asyncio.to_thread(urllib.request.urlretrieve)`, and rewrite the expression to `file('/tmp/x', 'fmt')` before execution. All URLs (including localhost) are rewritten consistently. `NamedTemporaryFile` is used (not `TemporaryFile`) because chdb needs a filesystem path string.
  - **Debt**: Confirmed broken in chdb 4.1.2 and 26.1.0; no upstream fix. Remove this workaround once chdb's `url()` works reliably. Track at [chdb-io/chdb](https://github.com/chdb-io/chdb).

# chdb Missing `HTML` Output Format

- **`FORMATS`** (`aaiclick/data/formats.py`)
  - **Issue**: `Object.export()` maps an `.html` extension to ClickHouse's `HTML` output format, but the chdb build aaiclick ships against omits the HTML output handler and rejects it with `Unknown format HTML. Maybe you meant: ['XML']. (UNKNOWN_FORMAT)`. The format is supported by upstream server ClickHouse.
  - **Workaround**: No `.html` / `HTML` entry in `FORMATS` — the extension is simply unsupported for export.
  - **Debt**: Re-confirmed broken on chdb 4.1.9 (chdb-core 26.5.0 / ClickHouse 26.5.1). Add an `.html` → `HTML` `FormatSpec` (and its export test) once chdb's build includes the HTML output handler, or once aaiclick gains a fallback to clickhouse-connect for formats chdb doesn't ship. Track at [chdb-io/chdb](https://github.com/chdb-io/chdb).

# clickhouse-connect `'u'` Type Code DeprecationWarning on Python 3.13

- **`filterwarnings` in `pyproject.toml`** (`[tool.pytest.ini_options]`)
  - **Issue**: `clickhouse-connect` 1.0.1's compiled `driverc/buffer.pyx` still builds an `array.array('u', ...)` template (`for c in 'bBuhHiIlLqQfd'`), and `'u'` is deprecated since Python 3.13 and slated for removal in 3.16. Under `filterwarnings = ["error"]` every distributed test fails at the `import clickhouse_connect` step.
  - **Workaround**: `"ignore:The 'u' type code is deprecated:DeprecationWarning"` in `pyproject.toml`.
  - **Debt**: Upstream can't trivially swap to `'w'` — the new typecode was added in 3.13 and raises `ValueError: bad typecode` on 3.10–3.12. Drop the filter once `clickhouse-connect` ships a wheel that either drops sub-3.13 support or version-gates the typecode. Track at [ClickHouse/clickhouse-connect](https://github.com/ClickHouse/clickhouse-connect).

# GitHub Actions

- **`FORCE_JAVASCRIPT_ACTIONS_TO_NODE24=true`** (`.github/workflows/test.yaml`)
  - **Issue**: `dorny/test-reporter@v2` targets Node.js 20, which GitHub Actions deprecates from June 2, 2026.
  - **Debt**: No v3 of the action exists yet. Remove the env var and pin to the new version once `dorny/test-reporter` releases Node.js 24 support.

# LiteLLM Unawaited Coroutine on Ollama Path

- **`pytest_collection_modifyitems()`** (`aaiclick/ai/conftest.py`)
  - **Issue**: LiteLLM 1.82.4 leaves `async_success_handler` coroutines unawaited on the Ollama code path despite the upstream fix in v1.76.1 (PR #14050). This produces `RuntimeWarning` and `PytestUnraisableExceptionWarning` that fail tests under `filterwarnings = ["error"]`.
  - **Workaround**: `pytest_collection_modifyitems()` attaches per-test `filterwarnings` marks to `live_llm`-marked tests, suppressing only those two warnings. Scoped to live LLM tests so it doesn't mask warnings elsewhere.
  - **Debt**: Remove the suppressions once LiteLLM fixes the Ollama-specific code path. Track at [BerriAI/litellm](https://github.com/BerriAI/litellm).

# Python 3.10: Generic `NamedTuple` Unsupported

- **`PageRows`** (`aaiclick/internal_api/pagination.py`)
  - **Issue**: Python 3.10 rejects `class PageRows(NamedTuple, Generic[T])` with `TypeError: Multiple inheritance with NamedTuple is not supported`. Subscripting a frozen-dataclass generic at runtime (`PageRows[int](...)`) also fails on 3.10 because `__orig_class__` assignment hits the frozen guard. Python 3.11 fixes both.
  - **Workaround**: `PageRows` is declared as `@dataclass(frozen=True, slots=True)` + `Generic[T]`, and constructed without a runtime subscript (`PageRows(total=..., rows=...)`). The return annotation on `paginate()` keeps the generic info for type checking.
  - **Debt**: Switch back to `class PageRows(NamedTuple, Generic[T])` — lighter runtime, iterable/unpackable — once `requires-python` bumps to `>=3.11`. Python 3.10 reaches EOL October 2026.

# Unused Compound Entries in `ColumnType`

- **`ColumnType`** (`aaiclick/data/models.py`)
  - **Issue**: The literal lists `Tuple`, `Map`, and `Nested`, but nothing produces them — inference emits only leaf types (nested data flattens to dot-notation columns), `parse_ch_type()` does not decompose them, and `ch_type_to_pa()` falls back to `pa.string()` for any unrecognized type, so a compound column arriving via explicit `Schema` would be silently mistyped at ingest.
  - **Debt**: Either drop the three entries from `ColumnType` (flattening is the supported representation) or implement real parse/arrow support. If they stay, replace the silent `pa.string()` fallback in `ch_type_to_pa()` with a raise so mistyping surfaces at the boundary.
