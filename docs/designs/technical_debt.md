Technical Debt
---

# ClickHouse Mishandles Path-Relative Redirects

- **`_resolve_redirect_url()`** (`aaiclick/example_projects/cyber_threat_feeds/cyber_threat_feeds/epss.py`)
  - **Issue**: The engine resolves a path-relative `Location` (`sample.parquet`) against the full request path rather than its parent directory, so `/dir/entry.parquet` redirects to `/dir/entry.parquet/sample.parquet` and the fetch fails. Absolute and root-relative `Location` headers resolve correctly. Both backends share the engine's HTTP client, so neither escapes it.
  - **Workaround**: The EPSS loader issues a `HEAD` in Python and passes the resolved final URL to `url()`. Its feed 301s cross-host and then 302s path-relative.
  - **Debt**: Drop the pre-resolution once the engine resolves relative redirects per RFC 3986. `test_url_path_relative_redirect_unsupported` pins the current behavior and will fail when that lands. Track at [ClickHouse/ClickHouse](https://github.com/ClickHouse/ClickHouse).

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

# Unquoted Identifier Interpolation in Remaining SQL Builders

- **`copy_db()`, `join.py` projections, `Object.insert_from_url()`, `count_if` dict form, `build_order_by_clause()`, `Computed` transform helpers** (`aaiclick/data/object/ingest.py`, `join.py`, `object.py`, `operators.py`, `transforms.py`, `aaiclick/data/models.py`)
  - **Issue**: dotted column names (`m.x`, `b.*.c`) are first-class since nested-dict ingest, but only the insert/concat/group_by builders quote identifiers. The listed builders still interpolate raw column names into SQL (`", ".join(columns)`, `f"toYear({column})"`, `ORDER BY ({', '.join(columns)})`), so each fails or misparses on dotted names and will be discovered as its own one-line bug.
  - **Debt**: route SQL emitters through a shared expression-builder (`select_list`, `cast_as`, `qualified`, `alias`) so quoting is an invariant of construction rather than per-site discipline; `_cast_select_exprs()` in `ingest.py` is the seed.
