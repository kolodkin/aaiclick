# Technical Debt

## Warning Suppressions

### clickhouse-connect FutureWarning

**Location**: `aaiclick/data/data_context.py` — see `warnings.filterwarnings(...)` call after imports

**What**: clickhouse-connect (0.6.x–0.8.x) triggers `FutureWarning` from numpy datetime
internals during query result processing (e.g., `datetime64` handling in response parsing).

**Why global filter**: A local `warnings.catch_warnings()` context manager around client
creation (`_create_ch_client()`) is insufficient — the warnings fire during queries, inserts,
and other operations throughout the client's lifetime. A module-level filter ensures all call
sites are covered.

**Filter definition**:

```python
# aaiclick/data/data_context.py
warnings.filterwarnings("ignore", category=FutureWarning, module=r"clickhouse_connect\.")
```

**Affected code paths** (all use `get_ch_client()` from `data_context.py`):

- `_create_ch_client()` — client initialization
- `Object` operations — queries via `ch_client.query()`, `ch_client.command()`
- `create_object()` / `create_object_from_value()` — table creation and inserts

**When to remove**: Once clickhouse-connect ships a release that no longer emits these
FutureWarnings. Check by temporarily removing the filter and running the test suite with
`-W error` (the default pytest config in `pyproject.toml`). If no FutureWarnings surface,
the filter can be safely deleted.
