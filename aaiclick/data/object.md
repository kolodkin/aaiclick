# Object Class Documentation

## Overview

The `Object` class represents data stored in ClickHouse tables. Each Object instance corresponds to a ClickHouse table and supports operations through operator overloading that create new tables with results.

**Key Features:**
- Operator overloading for arithmetic, comparison, and bitwise operations
- Immutable operations (all operations return new Objects)
- Automatic table naming with Snowflake IDs
- Support for scalars, arrays, and dictionaries
- Element-wise operations on arrays
- Automatic lifecycle management and staleness detection

## Object Lifecycle and Staleness

### Context Management

Objects are managed by a `DataContext` and automatically cleaned up when the context exits. All objects created within a context become **stale** when the context is closed.

```python
async with DataContext():
    obj = await create_object_from_value([1, 2, 3])
    data = await obj.data()  # ✓ Works fine

# DataContext exits, obj becomes stale

data = await obj.data()  # ✗ RuntimeError: Cannot use stale Object
```

### Staleness Detection

Objects have built-in staleness detection that prevents operations on stale objects:

**Properties:**
- `obj.stale` - Returns `True` if object is stale, `False` otherwise
- `obj.ctx` - Raises `RuntimeError` if object is stale
- `obj.ch_client` - Raises `RuntimeError` if object is stale

**Methods:**
- All async database methods call `self.checkstale()` at the start
- Clear error message: `"Cannot use stale Object. Table 't123...' has been deleted."`

### Explicit Staleness Check

```python
# Manual check
obj.checkstale()  # Raises RuntimeError if stale

# Check property
if obj.stale:
    print("Object is stale")
```

### Best Practices

**✓ DO:**
- Keep all object operations within the context manager
- Create and use objects in the same context
- Allow automatic cleanup on context exit

```python
async with DataContext():
    a = await create_object_from_value([1, 2, 3])
    b = await create_object_from_value([4, 5, 6])
    result = await (a + b)
    data = await result.data()  # All operations in context
```

**✗ DON'T:**
- Store objects for use outside the context
- Try to use objects after context exits
- Pass objects between different contexts

```python
# Bad: Storing object for later use
async with DataContext():
    obj = await create_object_from_value([1, 2, 3])

data = await obj.data()  # Error! Object is stale
```

### Implementation Details

Staleness is implemented through explicit checks:
- Each async method calls `self.checkstale()` at execution time
- Properties `ctx` and `ch_client` also call `self.checkstale()`
- When context exits, all registered objects have their `_stale` flag set to `True`
- Any attempt to use a stale object raises a clear `RuntimeError`

This provides robust protection against accessing deleted tables.

## Table Schema and Structure

### Table Naming Convention

Each Object gets a dedicated ClickHouse table with a unique name generated using Snowflake IDs (prefixed with 't' for ClickHouse compatibility).

### Schema Patterns

All tables include an `aai_id` column with Snowflake IDs for consistency.

**Scalars** - Single row with aai_id:
```sql
CREATE TABLE (
    aai_id UInt64,  -- Snowflake ID
    value {type}
)
```

**Arrays/Lists** - Multiple rows with guaranteed insertion order:
```sql
CREATE TABLE (
    aai_id UInt64,  -- Snowflake ID for ordering
    value {type}
)
```

**Dict of Scalars** - Single row with aai_id:
```sql
CREATE TABLE (
    aai_id UInt64,  -- Snowflake ID
    col1 {type},
    col2 {type},
    ...
)
```

**Dict of Arrays** - Multiple rows with guaranteed insertion order:
```sql
CREATE TABLE (
    aai_id UInt64,  -- Snowflake ID for ordering
    col1 {type},
    col2 {type},
    ...
)
```

### Why aai_id?

ClickHouse doesn't guarantee insertion order in SELECT queries. The `aai_id` column uses **[Snowflake IDs](https://en.wikipedia.org/wiki/Snowflake_ID)** to:
- Guarantee globally unique row identifiers
- Preserve insertion order (time-ordered IDs)
- Enable correct element-wise operations (a + b matches by position)
- Support distributed/concurrent scenarios

**Snowflake ID structure (64 bits):**
- Bit 63: Sign bit (always 0)
- Bits 62-22: Timestamp (41 bits, ~69 years)
- Bits 21-12: Machine ID (10 bits, up to 1024 machines)
- Bits 11-0: Sequence (12 bits, up to 4096 IDs/ms per machine)

## Operator Support

All operators work element-wise on both scalar and array data types.

### Arithmetic Operators

| Python Operator | Description | ClickHouse Equivalent | Python Method | ClickHouse Reference |
|----------------|-------------|----------------------|---------------|---------------------|
| `+` | Addition | `+` | `__add__` | [operators#plus](https://clickhouse.com/docs/sql-reference/operators#plus) |
| `-` | Subtraction | `-` | `__sub__` | [operators#minus](https://clickhouse.com/docs/sql-reference/operators#minus) |
| `*` | Multiplication | `*` | `__mul__` | [operators#multiply](https://clickhouse.com/docs/sql-reference/operators#multiply) |
| `/` | Division | `/` | `__truediv__` | [operators#divide](https://clickhouse.com/docs/sql-reference/operators#divide) |
| `//` | Floor Division | `intDiv()` | `__floordiv__` | [intDiv](https://clickhouse.com/docs/sql-reference/functions/arithmetic-functions#intdiva-b) |
| `%` | Modulo | `%` | `__mod__` | [operators#modulo](https://clickhouse.com/docs/sql-reference/operators#modulo) |
| `**` | Power | `power()` | `__pow__` | [pow](https://clickhouse.com/docs/sql-reference/functions/math-functions#pow) |

### Comparison Operators

| Python Operator | Description | ClickHouse Equivalent | Python Method | ClickHouse Reference |
|----------------|-------------|----------------------|---------------|---------------------|
| `==` | Equal | `=` | `__eq__` | [operators#equals](https://clickhouse.com/docs/sql-reference/operators#equals) |
| `!=` | Not Equal | `!=` | `__ne__` | [operators#not-equals](https://clickhouse.com/docs/sql-reference/operators#not-equals) |
| `<` | Less Than | `<` | `__lt__` | [operators#less](https://clickhouse.com/docs/sql-reference/operators#less) |
| `<=` | Less Than or Equal | `<=` | `__le__` | [operators#less-or-equals](https://clickhouse.com/docs/sql-reference/operators#less-or-equals) |
| `>` | Greater Than | `>` | `__gt__` | [operators#greater](https://clickhouse.com/docs/sql-reference/operators#greater) |
| `>=` | Greater Than or Equal | `>=` | `__ge__` | [operators#greater-or-equals](https://clickhouse.com/docs/sql-reference/operators#greater-or-equals) |

### Bitwise Operators

| Python Operator | Description | ClickHouse Equivalent | Python Method | ClickHouse Reference |
|----------------|-------------|----------------------|---------------|---------------------|
| `&` | Bitwise AND | `bitAnd()` | `__and__` | [bitAnd](https://clickhouse.com/docs/sql-reference/functions/bit-functions#bitanda-b) |
| `\|` | Bitwise OR | `bitOr()` | `__or__` | [bitOr](https://clickhouse.com/docs/sql-reference/functions/bit-functions#bitora-b) |
| `^` | Bitwise XOR | `bitXor()` | `__xor__` | [bitXor](https://clickhouse.com/docs/sql-reference/functions/bit-functions#bitxora-b) |

### Aggregate Functions

| Python Method | Description | ClickHouse Function | Memory Behavior | ClickHouse Reference |
|--------------|-------------|--------------------|-----------------|--------------------|
| `.min()` | Minimum value | `min()` | Streaming (O(1)) | [min](https://clickhouse.com/docs/sql-reference/aggregate-functions/reference/min) |
| `.max()` | Maximum value | `max()` | Streaming (O(1)) | [max](https://clickhouse.com/docs/sql-reference/aggregate-functions/reference/max) |
| `.sum()` | Sum of values | `sum()` | Streaming (O(1)) | [sum](https://clickhouse.com/docs/sql-reference/aggregate-functions/reference/sum) |
| `.mean()` | Average value | `avg()` | Streaming (O(1)) | [avg](https://clickhouse.com/docs/sql-reference/aggregate-functions/reference/avg) |
| `.std()` | Standard deviation | `stddevPop()` | Streaming (O(1)) | [stddevPop](https://clickhouse.com/docs/sql-reference/aggregate-functions/reference/stddevpop) |

**Note:** All aggregate functions use ClickHouse's streaming aggregation, which processes data in chunks without holding the full dataset in memory. This makes them memory-efficient for large datasets.

### Set Operations

| Python Method | Description | ClickHouse Implementation | Memory Behavior | ClickHouse Reference |
|--------------|-------------|--------------------------|-----------------|---------------------|
| `.unique()` | Unique values | `GROUP BY` | Hash table | [GROUP BY](https://clickhouse.com/docs/sql-reference/statements/select/group-by) |

**Note:** The `unique()` method uses `GROUP BY` instead of `DISTINCT` for better performance on large datasets. `GROUP BY` is optimized for aggregation and enables better distributed processing in ClickHouse. The order of returned unique values is not guaranteed.

### Window Functions (Internal)

Element-wise array operations use window functions internally:

| Function | Purpose | ClickHouse Reference |
|----------|---------|---------------------|
| `row_number()` | Position-based element pairing | [row_number](https://clickhouse.com/docs/sql-reference/window-functions#row_number) |

### Memory/Disk Settings (for large datasets)

For very large datasets, ClickHouse can spill intermediate results to disk:

| Setting | Purpose | ClickHouse Reference |
|---------|---------|---------------------|
| `max_bytes_before_external_sort` | Spill sorts to disk | [max_bytes_before_external_sort](https://clickhouse.com/docs/operations/settings/query-complexity#max_bytes_before_external_sort) |
| `max_bytes_in_join` | Limit join memory | [max_bytes_in_join](https://clickhouse.com/docs/operations/settings/query-complexity#max_bytes_in_join) |
| `join_algorithm` | Join implementation strategy | [join_algorithm](https://clickhouse.com/docs/operations/settings/settings#join_algorithm) |

## Usage Examples

All operators work element-wise on both scalar and array data:

```python
import aaiclick

# Arithmetic
a = await aaiclick.create_object_from_value([10, 20, 30])
b = await aaiclick.create_object_from_value([2, 4, 5])
result = await (a + b)      # [12, 24, 35]
result = await (a ** b)     # [100, 160000, 24300000]

# Comparison
x = await aaiclick.create_object_from_value([1, 5, 10])
y = await aaiclick.create_object_from_value([5, 5, 8])
result = await (x == y)     # [False, True, False]
result = await (x < y)      # [True, False, False]

# Bitwise
m = await aaiclick.create_object_from_value([12, 10, 8])
n = await aaiclick.create_object_from_value([10, 12, 4])
result = await (m & n)      # [8, 8, 0]

# Set operations - unique values
data = await aaiclick.create_object_from_value([1, 2, 2, 3, 3, 3, 4])
unique = await data.unique()
values = await unique.data()  # [1, 2, 3, 4] (order not guaranteed)
```

For complete runnable examples of all operators, see:
- `examples/basic_operators.py` - Comprehensive examples of all 14 operators

## The concat() Method

The `concat()` method concatenates objects together, creating a new object with rows from the first object followed by rows from the second object.

**Requirements:**
- The first object must be an array (have `FIELDTYPE_ARRAY`)
- The second object can be an array or scalar
- Both objects must have compatible types (same ClickHouse type)

**Examples:**

```python
# Concatenate two arrays
a = await aaiclick.create_object_from_value([1, 2, 3])
b = await aaiclick.create_object_from_value([4, 5, 6])
result = await a.concat(b)
await result.data()  # [1, 2, 3, 4, 5, 6]

# Append scalar to array
a = await aaiclick.create_object_from_value([1, 2, 3])
b = await aaiclick.create_object_from_value(42)
result = await a.concat(b)
await result.data()  # [1, 2, 3, 42]

# Chain multiple concatenations
a = await aaiclick.create_object_from_value([1, 2])
b = await aaiclick.create_object_from_value([3, 4])
c = await aaiclick.create_object_from_value([5, 6])
result = await (await a.concat(b)).concat(c)
await result.data()  # [1, 2, 3, 4, 5, 6]
```

**Note:** The standalone `concat(obj_a, obj_b)` function is also available as an alternative to the method form.

## The data() Method

The `data()` method returns values directly based on the data type:

- **Scalar**: returns the value directly
- **Array**: returns a list of values
- **Dict (single row)**: returns dict directly
- **Dict (multiple rows)**: use `orient` parameter to control output format

### Orient Parameter for Dicts

| Constant | Value | Description |
|----------|-------|-------------|
| `ORIENT_DICT` | `'dict'` | Returns dict with arrays as values (default) |
| `ORIENT_RECORDS` | `'records'` | Returns list of dicts (one per row) |

### Examples

**Scalar:**
```python
obj = await aaiclick.create_object_from_value(42.0)
value = await obj.data()  # 42.0
```

**Array:**
```python
obj = await aaiclick.create_object_from_value([1, 2, 3, 4, 5])
values = await obj.data()  # [1, 2, 3, 4, 5]
```

**Dict of Scalars:**
```python
obj = await aaiclick.create_object_from_value({"id": 1, "name": "Alice", "age": 30})
data = await obj.data()  # {"id": 1, "name": "Alice", "age": 30}
```

**Dict of Arrays:**
```python
data = {
    "id": [1, 2, 3],
    "name": ["Alice", "Bob", "Charlie"],
    "age": [30, 25, 35]
}
obj = await aaiclick.create_object_from_value(data)

# Default: returns dict with arrays as values
data = await obj.data()  # {"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"], ...}

# With orient='records': returns list of dicts
rows = await obj.data(orient=aaiclick.ORIENT_RECORDS)
# [{"id": 1, "name": "Alice", "age": 30}, {"id": 2, "name": "Bob", "age": 25}, ...]
```

## Column Metadata

When tables are created via factory functions, each column gets a YAML comment containing the fieldtype.

### Fieldtype Constants

| Constant | Value | Meaning |
|----------|-------|---------|
| `FIELDTYPE_SCALAR` | `'s'` | Scalar - single value |
| `FIELDTYPE_ARRAY` | `'a'` | Array - list of values |
| `FIELDTYPE_DICT` | `'d'` | Dict - structured record |

### Example Column Comment

```yaml
{fieldtype: a}
```

This indicates an array column.

## Views

The `View` class provides a read-only filtered view of an Object with SQL query constraints applied. Views reference the same underlying ClickHouse table without creating data copies.

### Creating Views

Views are created using the `obj.view()` method with optional constraint parameters:

```python
obj = await create_object_from_value([1, 2, 3, 4, 5])
view = obj.view(where="value > 2", limit=2, order_by="value ASC")
await view.data()  # Returns [3, 4]
```

### View Constraints

| Attribute | Type | Description |
|-----------|------|-------------|
| `where` | `Optional[str]` | WHERE clause condition for filtering rows (e.g., `"value > 2"`) |
| `limit` | `Optional[int]` | Maximum number of rows to return from the query |
| `offset` | `Optional[int]` | Number of rows to skip before returning results |
| `order_by` | `Optional[str]` | ORDER BY clause for sorting results (e.g., `"value DESC"`) |

### Key Characteristics

**Read-Only:** Views cannot be modified with write operations like `insert()`. Attempting to insert will raise a `RuntimeError`.

**Same Table Reference:** Views don't create new tables - they query the source Object's table with constraints applied.

**Supports Read Operations:** All read operations work on Views:
- `.data()` - retrieve filtered data
- Arithmetic and comparison operators - create new Objects from filtered data
- Aggregation methods (`.min()`, `.max()`, `.sum()`, `.mean()`, `.std()`)

**Automatic Staleness Detection:** Views inherit staleness checks from the source Object.

### Examples

For complete runnable examples of filtering, pagination, sorting, combining constraints, and performing operations on views, see:
- `examples/view_examples.py` - Comprehensive examples of all View capabilities

## Implementation Details

### Operator Architecture

Operator implementations are separated into the `operators` module for modularity and testability. Each operator in the Object class delegates to a corresponding function in the operators module.

**Object class** (`object.py`):
- Provides operator overloading via dunder methods (`__add__`, `__mul__`, etc.)
- Delegates to operator functions in the `operators` module

**Operators module** (`operators.py`):
- Contains static async functions for each operator (`add()`, `mul()`, etc.)
- Each function calls `_apply_operator()` with the appropriate operator string

See [operators.md](operators.md) for complete operator function reference and usage examples.

### Operator Flow

All operators use the common `_apply_operator` method that:
1. Creates a new Object to hold the result
2. Determines if the operation is on scalars or arrays
3. Selects the appropriate SQL template (scalar or array)
4. Executes the SQL query with the operator
5. Returns the new Object

**SQL Templates:**
- `apply_op_scalar.sql` - For scalar-to-scalar operations
- `apply_op_array.sql` - For array-to-array operations (preserves aai_id)

**Type Preservation:**
The result preserves the fieldtype metadata from the source objects, ensuring proper data type handling throughout operation chains.
