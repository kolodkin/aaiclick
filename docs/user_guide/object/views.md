Views and copy()
---

# Views

<!-- req: views -->
Read-only filtered projection of an Object — same table, no data copy. Created via `obj.view(where=..., limit=..., offset=..., order_by=...)`. Supports all read operations; cannot `insert()`. See `examples/view_examples.py`.

## Chained WHERE Clauses

<!-- req: chained-where -->
- `obj.where(cond)` — creates View with initial WHERE
- `view.where(cond)` — AND-chains: `.where('x > 10').where('y < 20')` → `WHERE (x > 10) AND (y < 20)`
- `view.or_where(cond)` — OR-chains: `.where('x > 100').or_where('y < 5')` → `WHERE (x > 100) OR (y < 5)`

!!! warning "`or_where()` requires a prior `where()`"
    <!-- req: or-where-requires-where -->
    Calling `or_where()` without a preceding `where()` raises `ValueError`.
    Same applies to `or_having()` on `GroupByQuery`.

## Column Selection

Select columns from a dict Object → View (same table, restricted SELECT).

<!-- req: column-selection -->
- `obj["col"]` — single column → array-like View (single `value` field)
- `obj[["col_a", "col_b"]]` — multiple columns → dict-like View

```python
obj = await create_object_from_value({"x": [1, 2, 3], "y": [10, 20, 30]})

# Single column — returns array-like View
await obj["x"].data()              # [1, 2, 3]
arr = await obj["x"].copy()        # new array Object

# Multiple columns — returns dict-like View
await obj[["x", "y"]].data()       # {'x': [1, 2, 3], 'y': [10, 20, 30]}
```

<!-- req: column-selection-constraints -->
Preserves WHERE and computed column constraints when chained on a filtered View. The selection is the View's output: computed columns it omits are dropped from `data()` and `copy()`, while ones added after a multi-field selection join it. A single-field selection can't take computed columns — add them before selecting.

```python
s = {"s": Computed("Int64", "x + y")}
await obj.with_columns(s)[["x", "y"]].data()  # {'x': [...], 'y': [...]}
await obj[["x", "y"]].with_columns(s).data()  # {'x': [...], 'y': [...], 's': [...]}
```

## Computed Column Expansion: `with_columns()`

`with_columns({name: Computed(type, expression)})` adds SQL-expression columns as a View — no new table, no data copy. `Computed` comes from `aaiclick.data.models`.

<!-- req: with-columns -->
- Synchronous (no `await`). Works on Objects and Views; preserves existing constraints.
- Always returns a dict-like View: array sources promote to dict; scalar sources raise `ValueError`.
- Adds, never replaces — a column name collision raises `ValueError`.
- Expressions pass verbatim to ClickHouse. Basic validation rejects semicolons and `SELECT`; type mismatches surface from ClickHouse at query time.
- Chains with every View operation: `group_by()`, `where()`, column selection, further (additive) `with_columns()`, and operators on selected columns.

## `literal()` Helper

<!-- req: literal-helper -->
Convenience wrapper for constant columns — handles SQL quoting:

```python
from aaiclick import literal

# Before: manual quoting
obj.with_columns({"source": Computed("String", "'dataset_a'")})

# After: literal() handles it
obj.with_columns({"source": literal("dataset_a", "String")})
obj.with_columns({"flag": literal(True, "UInt8")})
obj.with_columns({"weight": literal(1.0, "Float64")})
```

Supported types: `str` (quoted), `int`/`float` (bare), `bool` (`true`/`false`).

## Domain Helpers

Named shortcuts that delegate to `with_columns()`. Each auto-names the result column; all accept `alias=` override and return a `View`.

<!-- req: domain-helpers -->
| Helper                                       | Default Alias          | Type            | Expression                       |
|----------------------------------------------|------------------------|-----------------|----------------------------------|
| `with_year(col)`                             | `{col}_year`           | `UInt16`        | `toYear(col)`                    |
| `with_month(col)`                            | `{col}_month`          | `UInt8`         | `toMonth(col)`                   |
| `with_day_of_week(col)`                      | `{col}_dow`            | `UInt8`         | `toDayOfWeek(col)`               |
| `with_date_diff(unit, col_a, col_b)`         | `{col_a}_{col_b}_diff` | `Int64`         | `dateDiff('unit', col_a, col_b)` |
| `with_lower(col)`                            | `{col}_lower`          | `String`        | `lower(col)`                     |
| `with_upper(col)`                            | `{col}_upper`          | `String`        | `upper(col)`                     |
| `with_length(col)`                           | `{col}_length`         | `UInt64`        | `length(col)`                    |
| `with_trim(col)`                             | `{col}_trimmed`        | `String`        | `trim(col)`                      |
| `with_abs(col)`                              | `{col}_abs`            | `Float64`       | `abs(col)`                       |
| `with_log2(col)`                             | `{col}_log2`           | `Float64`       | `log2(col)`                      |
| `with_sqrt(col)`                             | `{col}_sqrt`           | `Float64`       | `sqrt(col)`                      |
| `with_bucket(col, size)`                     | `{col}_bucket`         | `Int64`         | `intDiv(col, size)`              |
| `with_hash_bucket(col, n)`                   | `{col}_hash`           | `UInt64`        | `cityHash64(col) % n`            |
| `with_if(cond, then, else, *, alias)`        | required `alias`       | `String`        | `if(cond, then, else)`           |
| `with_multi_if(branches, *, default, alias)` | required `alias`       | `String`        | `multiIf(c1, r1, …, default)`    |
| `with_cast(col, ch_type)`                    | `{col}_{type_lower}`   | `ch_type`       | `to{Type}(col)`                  |
| `with_split_by_char(col, sep)`               | `{col}_parts`          | `Array(String)` | `splitByChar(sep, col)`          |
| `with_isin(col, other)`                      | `{col}_isin`           | `UInt8`         | `col IN (SELECT value FROM …)`   |

<!-- req: with-multi-if -->
`with_multi_if()` takes `(condition, result)` pairs — plain 2-tuples or `Branch` from `aaiclick.data.models` — and the earliest
match wins. `default` is required: ClickHouse's `multiIf` has no implicit else. Both halves are raw
SQL, so `AND` / `OR` / `NOT` / `IN` compose in a condition, and a result may be a literal, a column,
or an expression.

!!! warning "Conditions must be `UInt8`"
    <!-- req: multi-if-uint8-condition -->
    ClickHouse rejects a bare numeric column with `Illegal type Int64 of argument (condition)`.
    Write `is_member = 1`, not `is_member`.

## Column Renaming: `rename()`

<!-- req: rename -->
Returns a **View** aliasing column names (`old AS new`). Synchronous, no `await`.

```python
from aaiclick import literal

# Rename camelCase columns to snake_case for a consolidated table
kev_view = kev.rename({
    "cveID": "cve_id",
    "vendorProject": "vendor",
    "vulnerabilityName": "vulnerability_name",
}).with_columns({
    "source": literal("kev", "String"),
})
await consolidated.insert(kev_view)
```

<!-- req: rename-chaining -->
Chainable with `with_columns()`, `where()`, and other View operations. New names must not collide with non-renamed columns.

## Explode

<!-- req: explode -->
`.explode(*columns, left=False)` flattens Array column(s) into rows, duplicating scalar columns; exploded columns change from `Array(T)` to `T`. Returns a **View**, so downstream operators fuse into a single query. See `aaiclick/data/examples/explode.py`.

# The copy() Method

<!-- req: copy -->
Full data copy → new Object. Works on both Objects and Views — filters, computed columns, and ORDER BY are preserved.

```python
# Copy an array Object
obj_copy = await obj.copy()

# Materialize a View into a new Object
arr = await obj["x"].copy()         # array Object from dict column
subset = await obj.where("x > 5").copy()  # filtered copy

# Sorted copy — ORDER BY is preserved
sorted_copy = await obj.view(order_by="amount DESC").copy()
await sorted_copy.data()  # returns rows sorted by amount DESC
```

The result carries the `INSERT … SELECT` [stats()](data_retrieval.md#stats).

<!-- req: create-object-rejects-view-schema -->
`Schema` = "what a table is" and `ViewSchema` = "how to read it"; the registry never sees the latter. `create_object()` rejects a View's `.schema`; materialize a View with `copy()`. See [Schema Storage](https://github.com/kolodkin/aaiclick/blob/main/docs/architecture/data_model.md#schema-storage).

!!! warning "`copy()` is not serialized across workers"
    Unlike `insert()` and `concat()`, `copy()` does not take a per-table
    advisory lock. Two concurrent `copy()` calls into the same named
    destination will interleave their rows non-deterministically. Structure
    your pipeline so a named destination has a single writer.
