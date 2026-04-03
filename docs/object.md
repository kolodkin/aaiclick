Object Class Documentation
---

# Overview

**Implementation**: `aaiclick/data/object.py` — see `Object` and `View` classes

The `Object` class represents data stored in ClickHouse tables. Each Object instance corresponds to a ClickHouse table and supports operations through operator overloading that create new tables with results.

**Key Features:**
- Operator overloading for arithmetic, comparison, and bitwise operations
- Immutable structure — no in-place schema changes; `insert()` appends data but never mutates structure
- Support for scalars, arrays, and dictionaries
- Element-wise operations on arrays

For context management, deployment modes, table schemas, data types, lifecycle tracking, and URL loading, see [DataContext documentation](data_context.md).

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

# Operator Support

**Implementation**: `aaiclick/data/object.py` (dunder methods) delegates to `aaiclick/data/operators.py` (async functions).

All operators work element-wise on both scalar and array data. Each operator creates a new Object table with the result via `_apply_operator()` using SQL templates (`apply_op_scalar.sql`, `apply_op_array.sql`).

For runnable examples, see `examples/basic_operators.py`.

## Scalar Broadcast

**Implementation**: `aaiclick/data/object.py` — see `_ensure_object()`

!!! tip "Scalar broadcast"
    Python scalars work on either side of an operator:
    `obj * 2` and `2 * obj` both work. The scalar is auto-converted
    to a single-value Object via `_ensure_object()`.

For runnable examples, see `examples/basic_operators.py`.

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

Reduce an array to a scalar Object. All computation in ClickHouse, streaming O(1) memory.

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

**Implementation**: `aaiclick/data/object.py` (methods) delegates to `aaiclick/data/operators.py` — see `_apply_string_op_db()`

Pattern matching on String columns. All methods take a Python `str` pattern and return a new Object. Results can be chained with further operations (e.g., `match()` → `sum()` to count matches).

| Method              | ClickHouse Function          | Result Type | Description                        |
|---------------------|------------------------------|-------------|------------------------------------|
| `.match(p)`         | `match(val, p)`              | UInt8       | RE2 regex match (0 or 1)          |
| `.like(p)`          | `val LIKE p`                 | UInt8       | SQL LIKE (`%`, `_` wildcards)     |
| `.ilike(p)`         | `val ILIKE p`                | UInt8       | Case-insensitive LIKE             |
| `.extract(p)`       | `extract(val, p)`            | String      | Extract first capture group        |
| `.replace(p, r)`    | `replaceRegexpAll(val, p, r)`| String      | Replace all regex matches          |

**Note**: ClickHouse uses RE2 regex syntax (no lookaheads/lookbehinds).

## Membership Operator: `isin()`

**Implementation**: `aaiclick/data/object/object.py` — see `Object.isin()`, `aaiclick/data/object/operators.py` — see `isin_op()`

Test if each value is a member of another Object's value set. Returns a UInt8 mask (1 = in set, 0 = not in set). Generates a ClickHouse `IN` subquery — all data stays in the database.

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

Apply a ClickHouse function element-wise to the value column, returning a new Object. These are the Object-level equivalents of [Domain Helpers](#domain-helpers) (`with_year`, `with_lower`, etc.) which operate on Views.

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

**Implementation**: `aaiclick/data/object.py` — see `GroupByQuery` class, `aaiclick/data/operators.py` — see `group_by_agg()`

Pandas-style two-step: `obj.group_by('key').sum('col')`. `GroupByQuery` is a standalone intermediate class (not an Object subclass).

**Aggregation methods on GroupByQuery:**

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

**Features**: Multiple group keys, chained `.having()`/`.or_having()` for post-aggregation filtering, View support (WHERE + selected_fields). Result is a normal dict Object supporting all existing operations.

**HAVING clause chaining** — same pattern as WHERE chaining on Views. `.having(cond)` chains with AND, `.or_having(cond)` chains with OR. `.or_having()` requires a prior `.having()` — raises `ValueError` otherwise. For examples, see `examples/group_by.py`.

**Known gap**: No `Array(T)` column support — `groupArray()`, `groupUniqArray()`, per-group concat not available.

## Memory/Disk Settings

For large datasets, ClickHouse can spill to disk via `max_bytes_before_external_sort`, `max_bytes_in_join`, `join_algorithm`.

# Ingest

## concat()

**Implementation**: `aaiclick/data/object.py` — see `Object.concat()`, `aaiclick/data/ingest.py` — see `concat_objects_db()`

Concatenates multiple sources into a new Object. Self must be array; args can be Objects (array or scalar), Python scalars, or lists. Also available as standalone function `concat(obj_a, obj_b, ...)`.

- **Variadic**: `obj.concat(a, b, c)` — any number of sources in one call
- **Nullable promotion**: if any source has nullable columns, the result column is promoted to `Nullable`
- **Compatible types**: all sources must have matching column names and compatible ClickHouse types

## insert()

**Implementation**: `aaiclick/data/object.py` — see `Object.insert()`, `aaiclick/data/ingest.py` — see `insert_objects_db()`

Inserts data from one or more sources into an existing Object. Target must be array; sources can be Objects (array or scalar), Python scalars, or lists. Sources may have a subset of target columns — missing columns get their ClickHouse default values.

- **Variadic**: `obj.insert(a, b, c)` — any number of sources in one call
- **Subset columns**: sources don't need all target columns
- **View support**: sources can be Views with `where()`, `with_columns()`, field selection, etc.
- **Type casting**: source column types are `CAST` to target column types

## insert_from_url()

**Implementation**: `aaiclick/data/object.py` — see `Object.insert_from_url()`

Insert data from a URL into an existing Object. Schema created once, multiple workers can insert.

For `create_object_from_url()` (creates a new Object from a URL), see [DataContext documentation](data_context.md).

??? note "Shared insert mechanics"

    Both `insert()` and `concat()` delegate to `_insert_source()` (`aaiclick/data/ingest.py`) which executes one `INSERT INTO target (cols) SELECT aai_id, CAST(...) FROM source` per source. Computed columns from Views are already resolved to `ColumnInfo` by the time they reach `_insert_source()`.

# Data Retrieval

## data()

**Implementation**: `aaiclick/data/object.py` — see `Object.data()`

Returns values based on data type: scalar → value, array → list, dict → dict or list of dicts.

**Orient parameter** (for dict Objects with multiple rows):

| Constant         | Value       | Description                                  |
|------------------|-------------|----------------------------------------------|
| `ORIENT_DICT`    | `'dict'`    | Dict with arrays as values (default)         |
| `ORIENT_RECORDS` | `'records'` | List of dicts (one per row)                  |

## markdown()

**Implementation**: `aaiclick/data/object.py` — see `Object.markdown()`

Returns the Object's data as a plain-text markdown table. The `aai_id` column is omitted. Column widths are auto-sized.

- **truncate**: optional `dict[str, int]` mapping column names to max character width — values exceeding the limit are truncated with `…`
- **Scalars/arrays**: wrapped into a single `value` column
- **Floats**: formatted to 2 decimal places
- **None**: rendered as `N/A`

# Views

**Implementation**: `aaiclick/data/object.py` — see `View` class

Read-only filtered view of an Object — references the same table, no data copy.

Created via `obj.view(where=..., limit=..., offset=..., order_by=...)`. Supports all read operations (`.data()`, operators, aggregations). Cannot `insert()`.

For runnable examples, see `examples/view_examples.py`.

## Chained WHERE Clauses

**Implementation**: `aaiclick/data/object.py` — see `Object.where()`, `View.where()`, `View.or_where()`

Fluent API for building WHERE conditions. `Object.where()` creates a View; `View.where()` and `View.or_where()` chain additional conditions.

- `obj.where(cond)` — creates View with initial WHERE condition
- `view.where(cond)` — AND-chains: `.where('x > 10').where('y < 20')` → `WHERE (x > 10) AND (y < 20)`
- `view.or_where(cond)` — OR-chains: `.where('x > 100').or_where('y < 5')` → `WHERE (x > 100) OR (y < 5)`

!!! warning "`or_where()` requires a prior `where()`"
    Calling `or_where()` without a preceding `where()` raises `ValueError`.
    Same applies to `or_having()` on `GroupByQuery`.

## Column Selection

**Implementation**: `aaiclick/data/object.py` — see `Object.__getitem__()`

Select one or more columns from a dict Object, returning a View. No data copy — the View references the same table with a restricted SELECT list.

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

### Motivation

`group_by()` only accepts column names, not SQL expressions. When you need to group by a derived value (e.g., `toYear(dateAdded)`, `lower(name)`), the workaround is manual: create an intermediate Object, populate it with `INSERT...SELECT`, then group. `with_columns()` automates this pattern.

See `aaiclick/example_projects/cyber_threat_feeds/__init__.py` — `analyze_kev_by_year()` for the manual workaround in production code.

### Design

`with_columns()` returns a **View** with `expr AS name` aliases in the SELECT list — no new table, no data copy, O(1) creation.

### API

**Implementation**: `aaiclick/data/object.py` — see `Object.with_columns()` and `View.with_columns()` methods

**Implementation**: `aaiclick/data/models.py` — see `Computed` class (NamedTuple with `type` and `expression` fields). Import as `from aaiclick.data.models import Computed`.

`with_columns()` is synchronous — it creates a View, no database call needed. No `await`. Works on both Object and View. On Views, preserves existing constraints (WHERE, LIMIT, OFFSET, ORDER BY) and adds computed columns to the SELECT list.

## Explode

**Implementation**: `aaiclick/data/object.py` — see `Object.explode()` method

Flattens Array column(s) into individual rows. Each array element becomes its own row; scalar columns are duplicated. Returns a **View** (lazy, no materialization) — downstream operators fuse into a single SQL query.

**Example**: `aaiclick/examples/explode.py`

**Schema change**: exploded columns change from `Array(T)` to `T` in the View's effective schema.

**Tests**: `aaiclick/data/object/test_explode.py`

**Parameters**: `columns: dict[str, Computed]` — mapping of column name to `Computed(type, expression)`. Expression is passed verbatim to ClickHouse.

**Returns**: View with original columns + computed expression aliases.

**Raises**: `ValueError` if column name collides with existing, if called on scalar Object, or if columns dict is empty. `RuntimeError` if Object is stale.

**Examples**: See `aaiclick/data/object/test_with_columns.py` for usage patterns including basic computed columns, chaining, group_by integration, and error cases.

## Chaining

`with_columns()` returns a View, so all View operations work: `group_by()`, `where()`, column selection, further `with_columns()` calls (additive), and operators on selected columns.

??? note "with_columns() internals"

    **Result Schema Rules** — the result is always a View with dict-like schema (`fieldtype='d'`):

    | Source Type           | Behavior                                                          |
    |-----------------------|-------------------------------------------------------------------|
    | Array (`value` col)   | View selects `value` + computed columns → promotes to dict        |
    | Dict (named columns)  | View selects all existing columns + computed columns              |
    | Scalar                | **Rejected** — raises `ValueError`                                |
    | View (single field)   | View selects source field + computed columns                      |
    | View (multi field)    | View selects selected fields + computed columns                   |
    | View (with WHERE)     | WHERE preserved, computed columns added to SELECT                 |

    **Column Name Collision** — computed column names must not collide with existing column names. `with_columns()` adds new columns, it doesn't replace.

    **Implementation** — `ViewSchema` gains a `computed_columns` field (`aaiclick/data/models.py`). `View._build_select()` expands `*` to explicit columns plus `expr AS name` for each computed column.

    **Security** — SQL expressions are passed verbatim to ClickHouse. Basic validation rejects semicolons and subqueries (`SELECT` keyword). Type mismatches are caught by ClickHouse at query time. See `_validate_expression()`.

## Domain Helpers

**Implementation**: `aaiclick/data/object.py` — methods on `Object` class, delegating to `with_columns()`

Each helper auto-names the result column and auto-selects the ClickHouse type. All accept `alias=` to override the default name. All return a `View`.

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

**Implementation**: `aaiclick/data/object.py` — see `Object.rename()` method

`rename()` returns a **View** whose SELECT list aliases old column names to new ones (`old AS new`). This enables inserting data from sources with different column naming conventions into a shared target table.

```python
# Rename camelCase columns to snake_case for a consolidated table
kev_view = kev.rename({
    "cveID": "cve_id",
    "vendorProject": "vendor",
    "vulnerabilityName": "vulnerability_name",
}).with_columns({
    "source": Computed("String", "'kev'"),
})
await consolidated.insert(kev_view)
```

**Key behaviors**:

- Synchronous — creates a View, no database call needed. No `await`.
- Chainable with `with_columns()`, `where()`, `select()`, and other View operations.
- `aai_id` cannot be renamed.
- New names must not collide with non-renamed column names.
- `insert()` skips extra source columns not present in the target — no need to `select()` away unwanted columns.

**Tests**: `aaiclick/data/object/test_rename.py`

# The copy() Method

**Implementation**: `aaiclick/data/object.py` — see `Object.copy()`

Creates a new Object with a full copy of the data. Works on both Objects and Views — column selection, WHERE filters, computed columns, and ORDER BY are preserved in the copy.

When copying a sorted View, the result is a View of the new table with the same `order_by` constraint. This ensures `data()` returns rows in the expected sort order. The underlying table retains original `aai_id` values (creation-time ordering), while the View applies the sort on read.

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

All Object operations (`create_from_value`, arithmetic, `concat`, `insert`, `copy`, etc.) are instrumented to record provenance via `OplogCollector`. Activation via `data_context()` is planned for Phase 3. See `docs/oplog.md` for the full specification.

**Implementation**: `aaiclick/oplog/collector.py` — see `OplogCollector.record()`, `record_table()`
