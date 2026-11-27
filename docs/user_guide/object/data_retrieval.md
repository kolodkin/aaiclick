Data Retrieval
---

# Data Retrieval

## data()

<!-- req: data-shapes -->
Returns: scalar → value, array → list, dict → dict or list of dicts.

**Orient** (dict Objects):

<!-- req: data-orient -->
| Constant         | Value       | Description                                  |
|------------------|-------------|----------------------------------------------|
| `ORIENT_DICT`    | `'dict'`    | Dict with arrays as values (default)         |
| `ORIENT_RECORDS` | `'records'` | List of dicts (one per row)                  |

## markdown()

<!-- req: markdown -->
Returns data as a plain-text markdown table with auto-sized columns. Optional `truncate: dict[str, int]` caps column widths. Floats → 2dp, None → `N/A`.

## execute()

<!-- req: execute -->
Runs the same SELECT `.data()` would issue (honoring `where`, `order_by`, `limit`, `offset`, computed columns, renames, field selection), discards every row server-side (`FORMAT Null`), and returns a `QueryStats`. Use it to benchmark a View, warm caches, or assert a query *runs* without paying for transport. Per-call `order_by` / `limit` / `offset` mirror `.data()`. No safety row cap applies — the whole pipeline runs unless you pass `limit`.

```python
view = obj.view(where="score > 10").rename({"score": "s"})
stats = await view.execute()
print(stats.read_rows)   # rows scanned by the full pipeline
print(stats.elapsed_s)   # server-side wall time
# view's table is unchanged; no result rows came back
```

`QueryStats` is a frozen value type (exported from `aaiclick`). Every field is best-effort — a backend fills what it can surface and leaves the rest `None`:

<!-- req: query-stats-fields -->
| Field           | Meaning                                  | clickhouse-connect (HTTP) | chdb                |
|-----------------|------------------------------------------|---------------------------|---------------------|
| `read_rows`     | rows ClickHouse scanned                  | summary                   | `storage_rows_read` |
| `read_bytes`    | uncompressed bytes scanned               | summary                   | `storage_bytes_read`|
| `elapsed_s`     | server-side wall time, seconds           | `elapsed_ns / 1e9`        | `elapsed`           |
| `result_rows`   | rows the SELECT produced                 | summary                   | `None`              |
| `written_rows`  | rows written by an INSERT                | summary                   | `None`              |
| `written_bytes` | bytes written by an INSERT               | summary                   | `None`              |

## stats()

<!-- req: stats -->
Returns `QueryStats | None`: populated on objects **born from a server-side query**, `None` otherwise. It is async (not a property) because on an un-awaited `LazyOperator` it first materializes it, like `.data()`.

| Object origin                                                  | `await .stats()`               |
|----------------------------------------------------------------|--------------------------------|
| `.copy()` / `.concat()` / `.join()`                            | stats of the `INSERT … SELECT` |
| operator / aggregation / `group_by` result (incl. `await a + b`) | stats of the materialization   |
| plain table-backed `Object`                                    | `None`                         |
| unexecuted `View`                                              | `None` (it never ran a query)  |

```python
result = await big_view.copy()
stats = await result.stats()
print(stats.written_rows)   # how many rows the copy wrote (HTTP)
print(stats.read_rows)      # how many it scanned to produce them
```
