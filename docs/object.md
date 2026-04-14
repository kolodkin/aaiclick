Object
---

# Overview

The `Object` class (`aaiclick/data/object.py`) wraps a ClickHouse table. Each instance corresponds to one table; operator overloading creates new tables with results.

**Key Features:**
- Operator overloading for arithmetic, comparison, and bitwise operations
- Immutable structure — no in-place schema changes; `insert()` appends data but never mutates structure
- Support for scalars, arrays, and dictionaries
- Element-wise operations on arrays

See [DataContext](data_context.md) for lifecycle, schemas, and deployment modes.

# API Quick Reference

| API                                              | Category         | Description                                   | Section                                                              |
|--------------------------------------------------|------------------|-----------------------------------------------|----------------------------------------------------------------------|
| `+`, `-`, `*`, `/`, `//`, `%`, `**`              | Arithmetic       | Element-wise arithmetic                       | [Arithmetic Operators](#arithmetic-operators)                        |
| `==`, `!=`, `<`, `<=`, `>`, `>=`                 | Comparison       | Element-wise comparison                       | [Comparison Operators](#comparison-operators)                        |
| `&`, `\|`, `^`                                   | Bitwise          | Bitwise AND / OR / XOR                        | [Bitwise Operators](#bitwise-operators)                              |
| `.min()`, `.max()`, `.sum()`, `.mean()`          | Aggregation      | Reduce array to scalar                        | [Aggregation Operators](#aggregation-operators)                      |
| `.std()`, `.var()`, `.count()`, `.quantile(q)`   | Aggregation      | Statistical reduction                         | [Aggregation Operators](#aggregation-operators)                      |
| `.count_if(condition)`                           | Aggregation      | Count rows matching condition(s)              | [Aggregation Operators](#aggregation-operators)                      |
| `.unique()`                                      | Unique           | Deduplicate values via GROUP BY               | [Unique](#unique)                                                    |
| `.nunique()`                                     | Unique           | Count distinct values (fused)                 | [Unique](#unique)                                                    |
| `.isin(other)`                                   | Membership       | IN subquery test (returns UInt8 mask)         | [Membership Operator](#membership-operator-isin)                    |
| `.match(p)`, `.like(p)`, `.ilike(p)`             | String / Regex   | Pattern matching (returns UInt8 mask)         | [String/Regex Operators](#stringregex-operators)                     |
| `.extract(p)`, `.replace(p, r)`                  | String / Regex   | Capture group extraction, regex replace       | [String/Regex Operators](#stringregex-operators)                     |
| `.year()`, `.month()`, `.day_of_week()`          | Unary Transforms | Date/time extraction → new Object             | [Unary Transform Operators](#unary-transform-operators)              |
| `.lower()`, `.upper()`, `.length()`, `.trim()`   | Unary Transforms | String transforms → new Object                | [Unary Transform Operators](#unary-transform-operators)              |
| `.abs()`, `.log2()`, `.sqrt()`                   | Unary Transforms | Math functions → new Object                   | [Unary Transform Operators](#unary-transform-operators)              |
| `.group_by(keys).sum(col)` etc.                  | Group By         | Aggregation with GROUP BY + optional HAVING   | [Group By Operations](#group-by-operations)                          |
| `.group_by(keys).agg({col: spec})`               | Group By         | Multiple aggregations (multi-agg per column)  | [Group By Operations](#group-by-operations)                          |
| `.group_by(keys).any(col)`                       | Group By         | Arbitrary non-NULL value per group            | [Group By Operations](#group-by-operations)                          |
| `.group_by(keys).group_array_distinct(col)`      | Group By         | Distinct values → Array per group             | [Group By Operations](#group-by-operations)                          |
| `.view(where, limit, offset, order_by)`          | Views            | Read-only View with optional filters          | [Views](#views)                                                      |
| `.where(cond)` / `.or_where(cond)`               | Views            | Fluent WHERE chaining (AND / OR)              | [Chained WHERE Clauses](#chained-where-clauses)                      |
| `obj[key]` / `obj[[keys]]`                       | Views            | Select column(s) from dict Object → View      | [Column Selection](#column-selection)                                |
| `.with_columns({name: Computed(type, expr)})`    | Views            | Add SQL expression columns → View             | [Computed Column Expansion](#computed-column-expansion-with_columns) |
| `literal(value, ch_type)`                        | Helpers          | Constant column (`Computed` with SQL literal)  | [Computed Column Expansion](#computed-column-expansion-with_columns) |
| `.with_year(col)`, `.with_month(col)` …          | Views            | Named shortcuts for common computed columns   | [Domain Helpers](#domain-helpers)                                    |
| `.with_split_by_char(col, sep)`                  | Views            | Split String column by separator → Array      | [Domain Helpers](#domain-helpers)                                    |
| `.with_isin(col, other)`                         | Views            | IN subquery computed column → UInt8           | [Domain Helpers](#domain-helpers)                                    |
| `.rename({old: new})`                            | Views            | Alias column names in a View                  | [Column Renaming](#column-renaming-rename)                           |
| `.explode(*columns, left=False)`                 | Views            | Flatten Array column(s) into rows → View      | [Explode](#explode)                                                  |
| `.copy()`                                        | Copy             | Materialize Object / View to new Object       | [copy()](#the-copy-method)                                           |
| `.insert(*sources)`                              | Ingest           | Insert data from Objects / scalars / lists    | [insert()](#insert)                                                  |
| `.insert_from_url(url)`                          | Ingest           | Insert rows from a remote URL                 | [insert_from_url()](#insert_from_url)                                |
| `.concat(*sources)` / `concat(a, b, …)`          | Ingest           | Concatenate sources into a new Object         | [concat()](#concat)                                                  |
| `.data(orient=…)`                                | Data Retrieval   | Fetch results to Python (scalar / list / dict)| [data()](#data)                                                      |
| `.markdown(truncate=…)`                          | Data Retrieval   | Render data as markdown table                 | [markdown()](#markdown)                                              |
| `.export(path)`                                  | Export           | Export to file (format inferred from extension)| [export()](#export)                                                 |
| `.export_csv(path)`                              | Export           | Export to CSV file                            | [export()](#export)                                                  |
| `.export_parquet(path)`                          | Export           | Export to Parquet file                        | [export()](#export)                                                  |

# Operator Support

All operators work element-wise on scalar and array data, creating new Object tables. See `examples/basic_operators.py`.

!!! tip "Scalar broadcast"
    Python scalars work on either side: `obj * 2` and `2 * obj` both work.

??? note "Arithmetic Operators"

    | Python Operator | Description    | ClickHouse Equivalent | Forward Method  | Reverse Method    |
    |-----------------|----------------|-----------------------|-----------------|-------------------|
    | `+`             | Addition       | `+`                   | `__add__`       | `__radd__`        |
    | `-`             | Subtraction    | `-`                   | `__sub__`       | `__rsub__`        |
    | `*`             | Multiplication | `*`                   | `__mul__`       | `__rmul__`        |
    | `/`             | Division       | `/`                   | `__truediv__`   | `__rtruediv__`    |
    | `//`            | Floor Division | `intDiv()`            | `__floordiv__`  | `__rfloordiv__`   |
    | `%`             | Modulo         | `%`                   | `__mod__`       | `__rmod__`        |
    | `**`            | Power          | `power()`             | `__pow__`       | `__rpow__`        |

## Arithmetic Type Promotion

Arithmetic result types match ClickHouse's native promotion rules. See `_promote_arithmetic_type()` in `aaiclick/data/object/operators.py`, validated by `aaiclick/data/object/test_type_promotion.py` against `SELECT toTypeName()`.

??? note "Comparison Operators"

    | Python Operator | Description           | ClickHouse Equivalent | Python Method |
    |-----------------|-----------------------|-----------------------|---------------|
    | `==`            | Equal                 | `=`                   | `__eq__`      |
    | `!=`            | Not Equal             | `!=`                  | `__ne__`      |
    | `<`             | Less Than             | `<`                   | `__lt__`      |
    | `<=`            | Less Than or Equal    | `<=`                  | `__le__`      |
    | `>`             | Greater Than          | `>`                   | `__gt__`      |
    | `>=`            | Greater Than or Equal | `>=`                  | `__ge__`      |

    Comparison operators don't need explicit reverse methods — Python swaps `<`/`>` and `<=`/`>=` automatically (e.g., `5 < obj` becomes `obj > 5`).

??? note "Bitwise Operators"

    | Python Operator | Description | ClickHouse Equivalent | Forward Method | Reverse Method |
    |-----------------|-------------|-----------------------|----------------|----------------|
    | `&`             | Bitwise AND | `bitAnd()`            | `__and__`      | `__rand__`     |
    | `\|`            | Bitwise OR  | `bitOr()`             | `__or__`       | `__ror__`      |
    | `^`             | Bitwise XOR | `bitXor()`            | `__xor__`      | `__rxor__`     |

## Aggregation Operators

Reduce an array to a scalar Object.

| Method         | ClickHouse Function | Notes                |
|----------------|---------------------|----------------------|
| `.min()`       | `min()`             |                      |
| `.max()`       | `max()`             |                      |
| `.sum()`       | `sum()`             |                      |
| `.mean()`      | `avg()`             |                      |
| `.std()`       | `stddevPop()`       |                      |
| `.var()`       | `varPop()`          |                      |
| `.count()`     | `count()`           |                      |
| `.quantile(q)` | `quantile(q)()`     | Approximate          |
| `.count_if(cond)` | `countIf()`      | Scalar (str) or dict Object (dict of conditions) |

## Unique

| Method       | ClickHouse Implementation                         | Notes                          |
|--------------|---------------------------------------------------|--------------------------------|
| `.unique()`  | `GROUP BY`                                        | Order not guaranteed           |
| `.nunique()` | `SELECT count() FROM (... GROUP BY value)`        | Fused count distinct           |

## String/Regex Operators

Pattern matching on String columns. Each method takes a `str` pattern, returns a new Object, and is chainable (e.g., `match()` → `sum()` to count matches).

| Method              | ClickHouse Function          | Result Type | Description                        |
|---------------------|------------------------------|-------------|------------------------------------|
| `.match(p)`         | `match(val, p)`              | UInt8       | RE2 regex match (0 or 1)          |
| `.like(p)`          | `val LIKE p`                 | UInt8       | SQL LIKE (`%`, `_` wildcards)     |
| `.ilike(p)`         | `val ILIKE p`                | UInt8       | Case-insensitive LIKE             |
| `.extract(p)`       | `extract(val, p)`            | String      | Extract first capture group        |
| `.replace(p, r)`    | `replaceRegexpAll(val, p, r)`| String      | Replace all regex matches          |

**Note**: ClickHouse uses RE2 regex syntax (no lookaheads/lookbehinds).

## Membership Operator: `isin()`

UInt8 membership mask via ClickHouse `IN` subquery — all data stays in the database.

| Method       | ClickHouse Equivalent                          | Result Type |
|--------------|-------------------------------------------------|-------------|
| `.isin(other)` | `value IN (SELECT value FROM other_table)`    | UInt8       |

Accepts an `Object` or a Python `list` (auto-converted to Object).

```python
obj = await create_object_from_value(["a", "b", "c", "d"])
allowed = await create_object_from_value(["a", "c"])
mask = await obj.isin(allowed)
await mask.data()           # [1, 0, 1, 0]

# Also works with a plain Python list
mask = await obj.isin(["a", "c"])

# Chain with sum() to count matches
total = await mask.sum()
await total.data()          # 2

# Works on dict column selection
obj = await create_object_from_value({"category": ["a", "b", "c"], "val": [1, 2, 3]})
mask = await obj["category"].isin(allowed)
```

**Tests**: `aaiclick/data/object/test_isin.py`. For runnable examples, see `examples/isin.py`.

## Unary Transform Operators

**Implementation**: `aaiclick/data/object.py` (methods) delegates to `aaiclick/data/operators.py` — see `unary_transform()`

Apply a ClickHouse function element-wise to the value column, returning a new Object. Object-level equivalents of [Domain Helpers](#domain-helpers) which operate on Views.

| Method          | ClickHouse Function | Result Type | Category  |
|-----------------|---------------------|-------------|-----------|
| `.year()`       | `toYear()`          | UInt16      | Date/time |
| `.month()`      | `toMonth()`         | UInt8       | Date/time |
| `.day_of_week()`| `toDayOfWeek()`     | UInt8       | Date/time |
| `.lower()`      | `lower()`           | String      | String    |
| `.upper()`      | `upper()`           | String      | String    |
| `.length()`     | `length()`          | UInt64      | String    |
| `.trim()`       | `trimBoth()`        | String      | String    |
| `.abs()`        | `abs()`             | Float64     | Math      |
| `.log2()`       | `log2()`            | Float64     | Math      |
| `.sqrt()`       | `sqrt()`            | Float64     | Math      |

Results are full Objects — chainable with any operator (e.g., `await (await obj.year()).unique()`).

**Tests**: `aaiclick/data/test_unary_transforms.py`. For runnable examples, see `examples/transforms.py`.

## Group By Operations

Pandas-style two-step: `obj.group_by('key').sum('col')`. See `GroupByQuery` class in `aaiclick/data/object.py`.

| Method             | Description                  | Result Column Type                     |
|--------------------|------------------------------|----------------------------------------|
| `.sum(col)`        | Sum per group                | Preserves int types, Float64 for float |
| `.mean(col)`       | Average per group            | Float64                                |
| `.min(col)`        | Minimum per group            | Preserves source type                  |
| `.max(col)`        | Maximum per group            | Preserves source type                  |
| `.count()`         | Count rows per group         | UInt64 (column named `_count`)         |
| `.std(col)`        | Standard deviation per group | Float64                                |
| `.var(col)`        | Variance per group           | Float64                                |
| `.any(col)`        | Arbitrary non-NULL per group | Preserves source type                  |
| `.group_array_distinct(col)` | Distinct values → Array per group | `Array(T)` where T is source type |
| `.agg({col: op})`  | Multiple aggregations        | Per-function type rules                |

**`agg()` spec formats** — the dict value (per source column) accepts three forms:

| Form                       | Example                                                              | Behavior                              |
|----------------------------|----------------------------------------------------------------------|---------------------------------------|
| `str`                      | `{'amount': GB_SUM}`                                                 | Alias = column name (backward compat) |
| `Agg(op, alias)`           | `{'amount': Agg(GB_SUM, 'total')}`                                   | Single op with explicit alias         |
| `[Agg(op, alias), ...]`    | `{'amount': [Agg(GB_SUM, 'amt_sum'), Agg(GB_MEAN, 'amt_avg')]}`      | Multiple ops on the same column       |

All three forms can be mixed in a single `agg()` call.

Multiple group keys, View support, and chained `.having()`/`.or_having()` for post-aggregation filtering (same AND/OR pattern as WHERE). `.or_having()` requires a prior `.having()`. Result is a normal dict Object. See `examples/group_by.py`.

**Known gap**: No `Array(T)` column support — `groupArray()`, `groupUniqArray()`, per-group concat not available.

## Memory/Disk Settings

For large datasets, ClickHouse can spill to disk via `max_bytes_before_external_sort`, `max_bytes_in_join`, `join_algorithm`.

# Ingest

## concat()

Concatenates multiple sources into a new Object. Also available as standalone `concat(a, b, ...)`. Self must be array; args can be Objects, scalars, or lists.

- **Variadic**: `obj.concat(a, b, c)` — any number of sources in one call
- **Nullable promotion**: if any source has nullable columns, the result column is promoted to `Nullable`
- **Compatible types**: all sources must have matching column names and compatible ClickHouse types

## insert()

Inserts data from one or more sources into an existing Object. Target must be array; sources can be Objects, scalars, or lists. Missing columns get ClickHouse defaults.

- **Variadic**: `obj.insert(a, b, c)` — any number of sources in one call
- **Subset columns**: sources don't need all target columns
- **View support**: sources can be Views with `where()`, `with_columns()`, field selection, etc.
- **Type casting**: source column types are `CAST` to target column types

## insert_from_url()

Insert data from a URL into an existing Object. For `create_object_from_url()`, see [DataContext](data_context.md).

??? note "Shared insert mechanics"

    Both `insert()` and `concat()` delegate to `_insert_source()` (`aaiclick/data/ingest.py`) — one `INSERT INTO ... SELECT CAST(...) FROM source` per source. Fresh Snowflake IDs are generated; source `aai_id` values are not preserved. Order follows argument order.

## Order Preservation

Order is preserved via **Snowflake IDs** — each row gets a globally unique, timestamp-encoded `aai_id`. All operations (`copy()`, `insert()`, `concat()`) generate fresh IDs. Order follows argument order: self first, then args left-to-right. `data()` retrieves rows via `ORDER BY aai_id`.

```python
obj_a = await create_object_from_value([1, 2, 3])
obj_b = await create_object_from_value([4, 5, 6])

result = await obj_a.concat(obj_b)  # Result: [1, 2, 3, 4, 5, 6]
result = await obj_b.concat(obj_a)  # Result: [4, 5, 6, 1, 2, 3]
```

# Data Retrieval

## data()

Returns: scalar → value, array → list, dict → dict or list of dicts.

**Orient** (dict Objects):

| Constant         | Value       | Description                                  |
|------------------|-------------|----------------------------------------------|
| `ORIENT_DICT`    | `'dict'`    | Dict with arrays as values (default)         |
| `ORIENT_RECORDS` | `'records'` | List of dicts (one per row)                  |

## markdown()

Returns data as a plain-text markdown table (`aai_id` omitted, auto-sized columns). Optional `truncate: dict[str, int]` caps column widths. Floats → 2dp, None → `N/A`.

## export()

Export data to a local file. Format is inferred from the file extension, or use the explicit methods directly.

```python
await obj.export("/tmp/data.csv")       # CSV
await obj.export("/tmp/data.parquet")   # Parquet

# Explicit methods
await obj.export_csv("/tmp/data.csv")
await obj.export_parquet("/tmp/data.parquet")
```

# Views

Read-only filtered projection of an Object — same table, no data copy. Created via `obj.view(where=..., limit=..., offset=..., order_by=...)`. Supports all read operations; cannot `insert()`. See `examples/view_examples.py`.

## Chained WHERE Clauses

- `obj.where(cond)` — creates View with initial WHERE
- `view.where(cond)` — AND-chains: `.where('x > 10').where('y < 20')` → `WHERE (x > 10) AND (y < 20)`
- `view.or_where(cond)` — OR-chains: `.where('x > 100').or_where('y < 5')` → `WHERE (x > 100) OR (y < 5)`

!!! warning "`or_where()` requires a prior `where()`"
    Calling `or_where()` without a preceding `where()` raises `ValueError`.
    Same applies to `or_having()` on `GroupByQuery`.

## Column Selection

Select columns from a dict Object → View (same table, restricted SELECT).

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

Preserves WHERE and computed column constraints when chained on a filtered View.

**Tests**: `aaiclick/data/object/test_column_selection.py`

## Computed Column Expansion: `with_columns()`

`with_columns()` adds derived columns (SQL expressions) as a lightweight View — no new table, no data copy.

### API

Synchronous (no `await`). Works on both Object and View; preserves existing constraints. Uses `Computed(type, expression)` from `aaiclick.data.models`.

### `literal()` Helper

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

## Explode

Flattens Array column(s) into individual rows (scalar columns duplicated). Returns a **View** — downstream operators fuse into a single query. Exploded columns change from `Array(T)` to `T`. See `aaiclick/data/examples/explode.py`.

**Tests**: `aaiclick/data/object/test_explode.py`

## Chaining

`with_columns()` returns a View, so all View operations work: `group_by()`, `where()`, column selection, further `with_columns()` calls (additive), and operators on selected columns.

??? note "with_columns() internals"

    Result is always a View with dict-like schema. Array sources promote to dict; scalar sources raise `ValueError`. Column name collisions raise `ValueError` — `with_columns()` adds, never replaces.

    SQL expressions are passed verbatim to ClickHouse. Basic validation rejects semicolons and `SELECT`. Type mismatches are caught by ClickHouse at query time.

## Domain Helpers

Named shortcuts that delegate to `with_columns()`. Each auto-names the result column; all accept `alias=` override and return a `View`.

| Helper                                    | Default Alias         | Type      | Expression                            |
|-------------------------------------------|-----------------------|-----------|---------------------------------------|
| `with_year(col)`                          | `{col}_year`          | `UInt16`  | `toYear(col)`                         |
| `with_month(col)`                         | `{col}_month`         | `UInt8`   | `toMonth(col)`                        |
| `with_day_of_week(col)`                   | `{col}_dow`           | `UInt8`   | `toDayOfWeek(col)`                    |
| `with_date_diff(unit, col_a, col_b)`      | `{col_a}_{col_b}_diff`| `Int64`   | `dateDiff('unit', col_a, col_b)`      |
| `with_lower(col)`                         | `{col}_lower`         | `String`  | `lower(col)`                          |
| `with_upper(col)`                         | `{col}_upper`         | `String`  | `upper(col)`                          |
| `with_length(col)`                        | `{col}_length`        | `UInt64`  | `length(col)`                         |
| `with_trim(col)`                          | `{col}_trimmed`       | `String`  | `trim(col)`                           |
| `with_abs(col)`                           | `{col}_abs`           | `Float64` | `abs(col)`                            |
| `with_log2(col)`                          | `{col}_log2`          | `Float64` | `log2(col)`                           |
| `with_sqrt(col)`                          | `{col}_sqrt`          | `Float64` | `sqrt(col)`                           |
| `with_bucket(col, size)`                  | `{col}_bucket`        | `Int64`   | `intDiv(col, size)`                   |
| `with_hash_bucket(col, n)`               | `{col}_hash`          | `UInt64`  | `cityHash64(col) % n`                |
| `with_if(cond, then, else, *, alias)`     | required `alias`      | `String`  | `if(cond, then, else)`                |
| `with_cast(col, ch_type)`                 | `{col}_{type_lower}`  | `ch_type` | `to{Type}(col)`                       |
| `with_split_by_char(col, sep)`            | `{col}_parts`         | `Array(String)` | `splitByChar(sep, col)`         |
| `with_isin(col, other)`                   | `{col}_isin`          | `UInt8`   | `col IN (SELECT value FROM …)`        |

`with_columns()` remains the public power-user interface for arbitrary expressions via `Computed(type, expression)`.

**Tests**: `aaiclick/data/object/test_with_columns.py`

## Column Renaming: `rename()`

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

Chainable with `with_columns()`, `where()`, and other View operations. `aai_id` cannot be renamed. New names must not collide with non-renamed columns.

**Tests**: `aaiclick/data/object/test_rename.py`

# The copy() Method

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

**Tests**: `aaiclick/data/test_copy_parametrized.py`

# Operation Provenance (Oplog)

All Object operations are instrumented to record provenance via `OplogCollector`. See `docs/oplog.md`.
