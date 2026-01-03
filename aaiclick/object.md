# Object Class Documentation

## Overview

The `Object` class represents data stored in ClickHouse tables. Each Object instance corresponds to a ClickHouse table and supports operations through operator overloading that create new tables with results.

**Key Features:**
- Operator overloading for arithmetic, comparison, and bitwise operations
- Immutable operations (all operations return new Objects)
- Automatic table naming with Snowflake IDs
- Support for scalars, arrays, and dictionaries
- Element-wise operations on arrays

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

## Lifecycle

### Automatic Table Cleanup with TTL

All Object tables are created with a TTL (time-to-live) of **1 day by default**. This ensures that data is automatically removed from ClickHouse after the specified period, providing automatic resource cleanup without requiring explicit `delete_table()` calls.

For immediate cleanup (e.g., to free resources during long-running sessions), you can explicitly call `await obj.delete_table()`.

**Configuration:**

TTL is controlled via the `Config` singleton in `config.py`, which reads the value from the environment variable:

- **Environment Variable**: `OBJECT_TABLE_TTL`
- **Default Value**: `1` (day)
- **Unit**: Days

**Setting a custom TTL:**

```bash
# Set TTL to 7 days
export OBJECT_TABLE_TTL=7
```

**Accessing TTL configuration programmatically:**

```python
from aaiclick.config import get_config

config = get_config()
print(f"Current TTL: {config.table_ttl_days} days")

# Modify TTL for new tables
config.table_ttl_days = 3
```

**How TTL works:**

- TTL is based on the `aai_id` column (Snowflake ID containing creation timestamp)
- Timestamp is extracted from aai_id using bit operations
- Tables are created with `TTL toDateTime((bitShiftRight(aai_id, 22) + epoch) / 1000) + INTERVAL {days} DAY`
- ClickHouse automatically removes expired data based on row creation time
- TTL applies to all tables created by factory functions and operators
- Provides robust, automatic resource management without manual intervention

## Operator Support

All operators work element-wise on both scalar and array data types.

### Arithmetic Operators

| Python Operator | Description | ClickHouse Equivalent | Python Method |
|----------------|-------------|----------------------|---------------|
| `+` | Addition | `+` | `__add__` |
| `-` | Subtraction | `-` | `__sub__` |
| `*` | Multiplication | `*` | `__mul__` |
| `/` | Division | `/` | `__truediv__` |
| `//` | Floor Division | `DIV` | `__floordiv__` |
| `%` | Modulo | `%` | `__mod__` |
| `**` | Power | `power()` | `__pow__` |

### Comparison Operators

| Python Operator | Description | ClickHouse Equivalent | Python Method |
|----------------|-------------|----------------------|---------------|
| `==` | Equal | `=` | `__eq__` |
| `!=` | Not Equal | `!=` | `__ne__` |
| `<` | Less Than | `<` | `__lt__` |
| `<=` | Less Than or Equal | `<=` | `__le__` |
| `>` | Greater Than | `>` | `__gt__` |
| `>=` | Greater Than or Equal | `>=` | `__ge__` |

### Bitwise Operators

| Python Operator | Description | ClickHouse Equivalent | Python Method |
|----------------|-------------|----------------------|---------------|
| `&` | Bitwise AND | `bitAnd()` | `__and__` |
| `\|` | Bitwise OR | `bitOr()` | `__or__` |
| `^` | Bitwise XOR | `bitXor()` | `__xor__` |

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
