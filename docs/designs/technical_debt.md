Technical Debt
---

# chdb Mishandles Path-Relative Redirects

- **Site**: the EPSS loader, `example_projects/cyber_threat_feeds/cyber_threat_feeds/epss.py`
  - **Issue**: The engine resolves a path-relative `Location` (`sample.parquet`) against the full request path rather than its parent directory, so `/dir/entry.parquet` redirects to `/dir/entry.parquet/sample.parquet` and the fetch fails. Absolute and root-relative `Location` headers resolve correctly. ClickHouse server fixed this in 26.10; chdb 26.9 has not.
  - **Workaround**: The loader issues a `HEAD` in Python and passes the resolved final URL to `url()`. Its feed 301s cross-host, then 302s path-relative.
  - **Debt**: Drop the pre-resolution once chdb ships a 26.10+ engine. `test_url_path_relative_redirect_unsupported` (chdb-only) pins the bug and will fail when that lands. Track at [ClickHouse/ClickHouse](https://github.com/ClickHouse/ClickHouse).

# chdb Missing `HTML` Output Format

- **Site**: `FORMATS` in `aaiclick/data/formats.py`
  - **Issue**: `Object.export()` would map `.html` to ClickHouse's `HTML` output format, but the chdb build aaiclick ships against omits the HTML output handler and rejects it with `Unknown format HTML. Maybe you meant: ['XML']. (UNKNOWN_FORMAT)`. Upstream server ClickHouse supports it.
  - **Workaround**: No `.html` / `HTML` entry in `FORMATS` — the extension is unsupported for export.
  - **Debt**: Re-confirmed broken on chdb 4.3.0 (chdb-core 26.7.0 / ClickHouse 26.7.2.1). Add an `.html` → `HTML` entry (and its export test) once chdb's build includes the HTML output handler, or once aaiclick can fall back to clickhouse-connect for formats chdb doesn't ship. Track at [chdb-io/chdb](https://github.com/chdb-io/chdb).

# clickhouse-connect `'u'` Type Code DeprecationWarning on Python 3.13

- **Site**: `filterwarnings` in `pyproject.toml` (`[tool.pytest.ini_options]`)
  - **Issue**: `clickhouse-connect` 1.0.1's compiled `driverc/buffer.pyx` still builds an `array.array('u', ...)` template (`for c in 'bBuhHiIlLqQfd'`); `'u'` is deprecated since Python 3.13 and slated for removal in 3.16. Under `filterwarnings = ["error"]` every distributed test fails at `import clickhouse_connect`.
  - **Workaround**: `"ignore:The 'u' type code is deprecated:DeprecationWarning"`.
  - **Debt**: Upstream can't simply swap to `'w'` — that typecode was added in 3.13 and raises `ValueError: bad typecode` on 3.10–3.12. Drop the filter once `clickhouse-connect` ships a wheel that drops sub-3.13 support or version-gates the typecode. Track at [ClickHouse/clickhouse-connect](https://github.com/ClickHouse/clickhouse-connect).

# GitHub Actions

- **Site**: `FORCE_JAVASCRIPT_ACTIONS_TO_NODE24=true` in `.github/workflows/_test-reusable.yaml` (also set in `project-example.yaml`)
  - **Issue**: `dorny/test-reporter@v2` targets Node.js 20, which GitHub Actions deprecates from June 2, 2026.
  - **Debt**: No v3 of the action exists yet. Remove the env var and pin the new version once `dorny/test-reporter` releases Node.js 24 support.

# Unused Compound Entries in `ColumnType`

- **Site**: `ColumnType` in `aaiclick/data/models.py`
  - **Issue**: The literal lists `Tuple`, `Map` and `Nested`, but nothing produces them — inference emits only leaf types (nested data flattens to dot-notation columns), ClickHouse type parsing does not decompose them, and the CH → Arrow mapping (`aaiclick/data/object_context/arrow_types.py`) falls back to `pa.string()` for any unrecognized type, so a compound column arriving via explicit `Schema` is silently mistyped at ingest.
  - **Debt**: Either drop the three entries (flattening is the supported representation) or implement real parse/Arrow support. If they stay, replace the silent `pa.string()` fallback with a raise so mistyping surfaces at the boundary.

# Unquoted Identifier Interpolation in Remaining SQL Builders

- **Sites**: the copy path, `join` projections, `insert_from_url`, the `count_if` dict form, the ORDER BY builder and the `Computed` transform helpers (`aaiclick/data/object/` — `ingest.py`, `join.py`, `object.py`, `operators.py`, `transforms.py` — and `aaiclick/data/models.py`)
  - **Issue**: Dotted column names (`m.x`, `b.*.c`) are first-class since nested-dict ingest, but only the insert/concat/group_by builders quote identifiers. The listed builders still interpolate raw names into SQL (`", ".join(columns)`, `f"toYear({column})"`, `ORDER BY ({', '.join(columns)})`), so each fails or misparses on dotted names and will surface as its own one-line bug.
  - **Debt**: Route SQL emitters through a shared expression builder (`select_list`, `cast_as`, `qualified`, `alias`) so quoting is an invariant of construction rather than per-site discipline; the cast-select builder in `ingest.py` is the seed. Plain reserved / spaced names, the same gap: `future.md` — "Quote Column Names in Generated SQL".
