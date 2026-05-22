Discard Terminal & Query Stats
---

`Object.execute()` runs the query an Object/View describes, discards every
row server-side, and returns the run's `QueryStats`. The same `QueryStats`
value also surfaces on `Object.stats` for objects that were *born from a
server-side query* — `.copy()` results and materialized `LazyOperator`
results.

**Status:** ⚠️ NOT YET IMPLEMENTED — design spec. Once all phases land, fold
the user-facing API (`execute()`, `.stats`, `QueryStats`) into
`docs/object.md` and **delete this file** — it exists only to drive the
implementation.

# Motivation

Two gaps, one shared foundation:

1. **No way to force full compute without paying for transport.** `.data()`
   caps at 1000 rows and pulls results back to Python; there is no terminal
   that says "run the whole thing, measure it, keep nothing." Useful for
   benchmarking a View, warming caches, or asserting a query *runs* without
   asserting its output.
2. **Materialization stats are discarded.** `.copy()` and operator
   materialization run `INSERT … SELECT` inside ClickHouse, but the row/byte
   counts ClickHouse reports are dropped on the floor. There is no way to ask
   a freshly-materialized Object "how much did you just write?"

Both follow from capturing ClickHouse's query summary — received on every
statement, currently discarded — into a small immutable `QueryStats`.

# Public API

## `QueryStats`

A frozen, public value type (exported from `aaiclick`). All fields are
best-effort: a backend fills what it can and leaves the rest `None`.

```python
from dataclasses import dataclass

@dataclass(frozen=True)
class QueryStats:
    read_rows: int | None        # rows ClickHouse scanned
    read_bytes: int | None       # uncompressed bytes scanned
    elapsed_s: float | None      # server-side wall time, seconds
    result_rows: int | None      # rows the SELECT produced (0 for FORMAT Null)
    written_rows: int | None     # rows written by an INSERT (None for read-only)
    written_bytes: int | None    # bytes written by an INSERT
```

## `Object.execute()`

```python
async def execute(
    self,
    *,
    order_by: Any = _UNSET,
    limit: Any = _UNSET,
    offset: Any = _UNSET,
) -> QueryStats:
    ...
```

Rebuilds the *exact* SELECT that `.data()` would issue via the shared
`_build_select()` (so `where`, `order_by`, `limit`, `offset`, computed
columns, renames, and field selection are all honored), appends
`FORMAT Null`, and runs it through the discard path. ClickHouse runs the full
pipeline and emits zero rows — nothing is materialized or transported back.
Returns the run's `QueryStats`.

Lives on `Object`, so `View` inherits it — and because `View` overrides
`_build_select()`, a View's `execute()` measures the View's projection
automatically. The per-call `order_by` / `limit` / `offset` overrides mirror
`.data()` for parity.

```python
view = obj.filter(...).rename(...)
stats = await view.execute()
print(stats.read_rows)   # rows scanned by the full pipeline
print(stats.elapsed_s)   # server-side wall time
# view's table is unchanged; no result rows came back
```

## `Object.stats`

```python
@property
def stats(self) -> QueryStats | None:
    return self._stats
```

Read-only. Populated only on objects **born from a server-side query**:

| Object origin                                      | `.stats`                       |
|----------------------------------------------------|--------------------------------|
| `await obj.copy()`                                 | stats of the `INSERT … SELECT` |
| materialized `LazyOperator` (e.g. `await (a + b)`) | stats of the materialization   |
| plain table-backed `Object`                        | `None`                         |
| unexecuted `View`                                  | `None` (it never ran a query)  |

```python
result = await big_view.copy()
print(result.stats.written_rows)   # how many rows the copy wrote
print(result.stats.read_rows)      # how many it scanned to produce them
```

# Internal design

## The discard / stats seam

ClickHouse reports a query summary on every statement (the
`X-ClickHouse-Summary` header over HTTP; the result object's accessors under
chdb). Today both backends discard it. We add **one module-level dispatcher**
in `ch_client.py`, mirroring the existing `export_query_to_file()` backend-
dispatch pattern rather than widening the `ChClient` protocol — the HTTP
client is an unwrapped third-party `clickhouse-connect` `AsyncClient` (see
`create_clickhouse_client()`), so a free function avoids re-wrapping it:

```python
async def execute_for_stats(
    query: str,
    settings: dict | None = None,
    parameters: dict | None = None,
) -> QueryStats:
    client = get_ch_client()
    if is_chdb():
        # chdb's result object exposes .rows_read()/.bytes_read()/.elapsed()
        return _chdb_stats(...)
    summary = await client.command(query, settings, parameters)
    return QueryStats.from_clickhouse_summary(summary)
```

- **clickhouse-connect (HTTP):** `command()` already returns a `QuerySummary`
  carrying the summary dict (`read_rows`, `read_bytes`, `written_rows`,
  `written_bytes`, `result_rows`, `elapsed_ns`). Map it in
  `QueryStats.from_clickhouse_summary()`; `elapsed_s = elapsed_ns / 1e9`.
- **chdb:** the command-path result object (the `TabSeparated` query in
  `ChdbClient.command()`) exposes `.rows_read()`, `.bytes_read()`,
  `.elapsed()`. Fill `read_rows` / `read_bytes` / `elapsed_s`; leave
  `written_rows` / `written_bytes` / `result_rows` as `None` (chdb does not
  surface them). A small helper in `chdb_client.py` runs the statement and
  extracts these.

`execute()` builds `… FORMAT Null` and calls `execute_for_stats()`.
`copy_db()` / operator materialization call it with their `INSERT … SELECT`.

!!! warning "chdb may double-specify `FORMAT`"
    Appending `FORMAT Null` to the SQL while chdb's session call also passes
    an output-format argument can double-specify `FORMAT`. The chdb branch
    must run the discard statement without a conflicting output format.
    Validate with the `chdb-eval` skill.

## Backend availability matrix

| field           | clickhouse-connect (HTTP) | chdb                |
|-----------------|---------------------------|---------------------|
| `read_rows`     | ✅ summary                | ✅ `.rows_read()`   |
| `read_bytes`    | ✅ summary                | ✅ `.bytes_read()`  |
| `elapsed_s`     | ✅ `elapsed_ns / 1e9`     | ✅ `.elapsed()`     |
| `result_rows`   | ✅ summary                | `None`              |
| `written_rows`  | ✅ summary                | `None`              |
| `written_bytes` | ✅ summary                | `None`              |

## `.stats` population

`Object.__init__()` gains `self._stats: QueryStats | None = None` and a
read-only `stats` property. The value is stamped by the *factory* that creates
the object, before the object is returned — so no observer ever sees `_stats`
change, and the Object's immutability guarantee holds.

- **`.copy()`** → `ingest.copy_db()` / `ingest.copy_db_selected_fields()`:
  switch the `command(insert_query)` call to `execute_for_stats`, then set
  `result._stats` before returning. Two sites.
- **`LazyOperator` materialization**: the stats live inside the ~10 operator
  functions in `operators.py` (each does `create_object()` + `command(INSERT)`),
  which funnel up through `LazyOperator._materialize()`. Each operator function
  captures the INSERT's stats and stamps `result._stats` before returning.
  Larger surface — see phasing below.

A `View` is a lazy description, never a materialization, so `View.__init__()`
leaves `_stats` `None`.

# Testing

Default backend (chdb + SQLite); CI also exercises distributed HTTP. Follow
the `python-testing-style` skill.

- `execute()` returns `QueryStats` with `read_rows > 0` on a non-trivial
  query; creates no table and returns no rows; honors a `where` / `limit`.
- `.copy()` result `.stats.written_rows` equals the source row count (HTTP);
  on chdb assert `read_rows` / `elapsed_s` are populated and tolerate `None`
  writes.
- `.stats is None` on a plain Object and on an unexecuted View.
- Per-backend best-effort: assert the fields each backend supports, tolerate
  `None` elsewhere (see the availability matrix).

# Implementation phasing

1. **Foundation + `execute()`**: `QueryStats`, `execute_for_stats()`
   dispatcher, both backend mappings, `Object.execute()`, public export,
   tests. Delivers feature 1 end-to-end.
2. **`.stats` on `.copy()`**: `_stats` / `stats` on `Object`, wire
   `copy_db` / `copy_db_selected_fields`, tests.
3. **`.stats` on operators**: thread stats through the operator functions in
   `operators.py`, tests.

# Out of scope

- Stats on the `.data()` (read) path — chdb's Arrow result carries no
  summary, so it would be HTTP-only and asymmetric. Track in `docs/future.md`
  if wanted.
- Per-stage / per-node profiling beyond ClickHouse's summary.
