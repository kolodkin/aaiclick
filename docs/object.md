# Object Class Documentation

## Overview

**Implementation**: `aaiclick/data/object.py` — see `Object` and `View` classes

The `Object` class represents data stored in ClickHouse tables. Each Object instance corresponds to a ClickHouse table and supports operations through operator overloading that create new tables with results.

**Key Features:**
- Operator overloading for arithmetic, comparison, and bitwise operations
- Immutable operations (all operations return new Objects)
- Automatic table naming with Snowflake IDs
- Support for scalars, arrays, and dictionaries
- Element-wise operations on arrays
- Automatic lifecycle management and staleness detection

## Object Lifecycle and Staleness

Objects are managed by a `data_context()` and become **stale** when the context exits. All async methods call `self.checkstale()` — using a stale Object raises `RuntimeError`.

**Implementation**: `aaiclick/data/object.py` — see `checkstale()`, `stale` property, `_register()`

**Rules**: Create and use Objects within the same `data_context()`. Don't store Objects for use after context exit. Don't pass Objects between contexts.

## Table Schema and Structure

Each Object gets a dedicated ClickHouse table named `t{snowflake_id}`. All tables include an `aai_id` column (Snowflake ID) for ordering — ClickHouse doesn't guarantee insertion order in SELECT.

**Implementation**: `aaiclick/data/data_context.py` — see `create_object()` and `create_object_from_value()`

### Schema Patterns

| Data Type       | Columns                              | Rows     |
|-----------------|--------------------------------------|----------|
| Scalar          | `aai_id UInt64`, `value {type}`      | 1        |
| Array/List      | `aai_id UInt64`, `value {type}`      | N        |
| Dict of Scalars | `aai_id UInt64`, `col1`, `col2`, ... | 1        |
| Dict of Arrays  | `aai_id UInt64`, `col1`, `col2`, ... | N        |

Column names are backtick-quoted via `quote_identifier()` in `aaiclick/data/sql_utils.py`.

### Snowflake ID Structure (64 bits)

| Bits  | Field      | Range                         |
|-------|------------|-------------------------------|
| 63    | Sign       | Always 0                      |
| 62-22 | Timestamp  | 41 bits, ~69 years            |
| 21-12 | Machine ID | 10 bits, up to 1024 machines  |
| 11-0  | Sequence   | 12 bits, up to 4096 IDs/ms    |

**Implementation**: `aaiclick/snowflake_id.py`

## Operator Support

**Implementation**: `aaiclick/data/object.py` (dunder methods) delegates to `aaiclick/data/operators.py` (async functions).

All operators work element-wise on both scalar and array data. Each operator creates a new Object table with the result via `_apply_operator()` using SQL templates (`apply_op_scalar.sql`, `apply_op_array.sql`).

See [operators.md](operators.md) for complete function reference. For runnable examples, see `examples/basic_operators.py`.

### Scalar Broadcast

**Implementation**: `aaiclick/data/object.py` — see `_to_query_info()` and `_scalar_query_info()`

All binary operators accept Python scalars (`int`, `float`, `bool`, `str`) on either side. The scalar is inlined as a SQL literal — no temporary table is created. This works for both `obj + 5` (forward) and `5 + obj` (reverse).

**How it works**: `_to_query_info()` dispatches based on operand type:
- **Object**: calls `_get_query_info()` → references the Object's ClickHouse table
- **Scalar**: calls `_scalar_query_info()` → inlines the value as a SQL literal (e.g., `5`, `'hello'`, `true`)

Reverse operators (`__radd__`, `__rsub__`, etc.) call `_apply_reverse_operator()` which swaps the operand order so the scalar appears on the left in SQL.

```python
# Forward: obj is left operand
result = await (obj * 2)      # SQL: ... obj.value * 2
result = await (obj + 100)    # SQL: ... obj.value + 100

# Reverse: obj is right operand
result = await (10 - obj)     # SQL: ... 10 - obj.value
result = await (2 ** obj)     # SQL: ... power(2, obj.value)
```

### Arithmetic Operators

| Python Operator | Description    | ClickHouse Equivalent | Forward Method  | Reverse Method    |
|-----------------|----------------|-----------------------|-----------------|-------------------|
| `+`             | Addition       | `+`                   | `__add__`       | `__radd__`        |
| `-`             | Subtraction    | `-`                   | `__sub__`       | `__rsub__`        |
| `*`             | Multiplication | `*`                   | `__mul__`       | `__rmul__`        |
| `/`             | Division       | `/`                   | `__truediv__`   | `__rtruediv__`    |
| `//`            | Floor Division | `intDiv()`            | `__floordiv__`  | `__rfloordiv__`   |
| `%`             | Modulo         | `%`                   | `__mod__`       | `__rmod__`        |
| `**`            | Power          | `power()`             | `__pow__`       | `__rpow__`        |

### Comparison Operators

| Python Operator | Description           | ClickHouse Equivalent | Python Method |
|-----------------|-----------------------|-----------------------|---------------|
| `==`            | Equal                 | `=`                   | `__eq__`      |
| `!=`            | Not Equal             | `!=`                  | `__ne__`      |
| `<`             | Less Than             | `<`                   | `__lt__`      |
| `<=`            | Less Than or Equal    | `<=`                  | `__le__`      |
| `>`             | Greater Than          | `>`                   | `__gt__`      |
| `>=`            | Greater Than or Equal | `>=`                  | `__ge__`      |

Comparison operators don't need explicit reverse methods — Python swaps `<`/`>` and `<=`/`>=` automatically (e.g., `5 < obj` becomes `obj > 5`).

### Bitwise Operators

| Python Operator | Description | ClickHouse Equivalent | Forward Method | Reverse Method |
|-----------------|-------------|-----------------------|----------------|----------------|
| `&`             | Bitwise AND | `bitAnd()`            | `__and__`      | `__rand__`     |
| `\|`            | Bitwise OR  | `bitOr()`             | `__or__`       | `__ror__`      |
| `^`             | Bitwise XOR | `bitXor()`            | `__xor__`      | `__rxor__`     |

### Aggregation Operators

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

### Set Operators

| Method      | ClickHouse Implementation | Notes                          |
|-------------|---------------------------|--------------------------------|
| `.unique()` | `GROUP BY`                | Order not guaranteed           |

### String/Regex Operators

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

### Group By Operations

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
| `.agg({col: op})`  | Multiple aggregations        | Per-function type rules                |

**Features**: Multiple group keys, `.having()` for post-aggregation filtering, View support (WHERE + selected_fields). Result is a normal dict Object supporting all existing operations.

**Known gap**: No `Array(T)` column support — `groupArray()`, `groupUniqArray()`, per-group concat not available.

### Memory/Disk Settings

For large datasets, ClickHouse can spill to disk via `max_bytes_before_external_sort`, `max_bytes_in_join`, `join_algorithm`.

## Loading Data from URLs

**Implementation**: `aaiclick/data/url.py` — see `create_object_from_url()`

Loads data from HTTP URLs directly into ClickHouse using the `url()` table function — **zero Python memory footprint**.

**Parameters**: `url` (required), `columns` (required), `format` (default `"Parquet"`), `where`, `limit`.

**Supported formats**: Parquet, CSV, CSVWithNames, CSVWithNamesAndTypes, TSV, TSVWithNames, TSVWithNamesAndTypes, JSON, JSONEachRow, JSONCompactEachRow, ORC, Avro.

**Schema behavior**: Single column renamed to `value`; multiple columns preserve names; types inferred by ClickHouse.

**Snowflake IDs**: Generated as `base_id + row_number() OVER ()` since data never enters Python.

**Validation**: HTTP(S) only, `aai_id` reserved, no `;` in WHERE, format must be in supported list.

### insert_from_url() ✅ IMPLEMENTED

**Implementation**: `aaiclick/data/object.py` — see `Object.insert_from_url()`

Insert data from a URL into an existing Object. Schema created once, multiple workers can insert.

## The concat() Method

**Implementation**: `aaiclick/data/operators.py` — see `concat()`

Concatenates Objects — first must be array, second can be array or scalar. Both must have compatible ClickHouse types. Also available as standalone function `concat(obj_a, obj_b)`.

## The data() Method

**Implementation**: `aaiclick/data/object.py` — see `Object.data()`

Returns values based on data type: scalar → value, array → list, dict → dict or list of dicts.

**Orient parameter** (for dict Objects with multiple rows):

| Constant         | Value       | Description                                  |
|------------------|-------------|----------------------------------------------|
| `ORIENT_DICT`    | `'dict'`    | Dict with arrays as values (default)         |
| `ORIENT_RECORDS` | `'records'` | List of dicts (one per row)                  |

## Column Metadata

**Implementation**: `aaiclick/data/object.py` — see `FIELDTYPE_SCALAR`, `FIELDTYPE_ARRAY`, `FIELDTYPE_DICT`

Each column gets a YAML comment with fieldtype: `'s'` (scalar), `'a'` (array), `'d'` (dict).

## Views

**Implementation**: `aaiclick/data/object.py` — see `View` class

Read-only filtered view of an Object — references the same table, no data copy.

Created via `obj.view(where=..., limit=..., offset=..., order_by=...)`. Supports all read operations (`.data()`, operators, aggregations). Cannot `insert()`.

For runnable examples, see `examples/view_examples.py`.

## Table Lifecycle Tracking

Tables are tracked via reference counting and dropped when no Objects reference them.

```
┌─────────────────────────────────────────────────────────────┐
│                    data_context()                             │
│                                                              │
│  enter  ──► Start LifecycleHandler                           │
│  exit   ──► Stop LifecycleHandler, cleanup                   │
│                                                              │
│  ┌──────────┐    incref()   ┌─────────────────────────────┐  │
│  │  Object  │──────────────►│                             │  │
│  │  __init__│               │    LifecycleHandler (ABC)   │  │
│  └──────────┘               │                             │  │
│                              │  Implementations:           │  │
│  ┌──────────┐    decref()   │  - LocalLifecycleHandler    │  │
│  │  Object  │──────────────►│    (TableWorker thread)     │  │
│  │  __del__ │               │  - PgLifecycleHandler       │  │
│  └──────────┘               │    (PostgreSQL refcounts)   │  │
│                              └─────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

### LifecycleHandler ABC

**Implementation**: `aaiclick/data/lifecycle.py` — see `LifecycleHandler` class

Abstract interface: `start()`, `stop()`, `incref()`, `decref()`, `pin()` (no-op default), `claim()` (raises NotImplementedError default).

### Local Mode

**Implementation**: `aaiclick/data/lifecycle.py` — see `LocalLifecycleHandler` class

Default handler. Wraps `TableWorker` (background thread in `aaiclick/data/table_worker.py`). Drops tables immediately on refcount 0. `data_context()` creates this when no handler is injected.

### Distributed Mode

**Implementation**: `aaiclick/orchestration/pg_lifecycle.py` — see `PgLifecycleHandler` class

Writes refcounts to PostgreSQL. Implements pin/claim for ownership transfer. Does NOT drop tables — cleanup by `PgCleanupWorker`.

See [Orchestration documentation](orchestration.md) — "Distributed Object Lifecycle" for the full design.

### Object Registration Flow

1. `create_object()` generates table name, calls `obj._register(ctx)` → `incref` (write-ahead, before CREATE TABLE)
2. `CREATE TABLE` in ClickHouse
3. On garbage collection, `Object.__del__` → `decref`
4. Views incref/decref the source Object's table (no new tables)

### __del__ Guard Clauses

| Guard                               | Scenario                                      |
|-------------------------------------|-----------------------------------------------|
| `sys.is_finalizing()`               | Interpreter shutdown — skip for thread safety  |
| `_data_ctx_ref is None`             | Object was never registered                    |
| `_data_ctx_ref()` returns None      | Context already garbage collected              |

## Test Files

| Operator Group                  | Test File                        |
|---------------------------------|----------------------------------|
| Arithmetic, Comparison, Bitwise | `test_operators_parametrized.py` |
| Scalar Broadcast                | `test_scalar_broadcast.py`       |
| Aggregation                     | `test_aggregation.py`            |
| Set Operators                   | `test_unique_parametrized.py`    |
| URL Loading                     | `test_url.py`                    |
| String/Regex Operators          | `test_regex_operators.py`        |
