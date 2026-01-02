# Object Class

The `Object` class represents data stored in ClickHouse tables. Each parameter (both positional args and keyword args) is wrapped in an Object class that manages ClickHouse table operations.

## Table Naming Convention

- Each parameter gets a dedicated ClickHouse table named: `attr_name_[oid]`
- The table name is stored in the Object class instance

## Table Schema and aai_id Column

Tables created by `create_object_from_value()` follow specific schema patterns based on data type:

### Tables WITHOUT aai_id (Single Row)

**Scalars** - Single value tables don't need ordering:
```sql
CREATE TABLE (
    value {type}
)
```

**Dict of Scalars** - Single row tables don't need ordering:
```sql
CREATE TABLE (
    col1 {type},
    col2 {type},
    ...
)
```

### Tables WITH aai_id (Multiple Rows)

**Arrays/Lists** - Multiple rows need guaranteed insertion order:
```sql
CREATE TABLE (
    aai_id UInt64,  -- Snowflake ID for ordering
    value {type}
)
```

**Dict of Arrays** - Multiple rows need guaranteed insertion order:
```sql
CREATE TABLE (
    aai_id UInt64,  -- Snowflake ID for ordering
    col1 {type},
    col2 {type},
    ...
)
```

### Why aai_id?

ClickHouse doesn't guarantee insertion order in SELECT queries. The `aai_id` column uses **[Snowflake IDs](https://en.wikipedia.org/wiki/Snowflake_ID)** (based on Twitter's algorithm) to:
- Guarantee globally unique row identifiers
- Preserve insertion order (time-ordered IDs)
- Enable correct element-wise operations (a + b matches by position)
- Support distributed/concurrent scenarios

**Snowflake ID structure (64 bits):**
- Bit 63: Sign bit (always 0)
- Bits 62-22: Timestamp (41 bits, ~69 years)
- Bits 21-12: Machine ID (10 bits, up to 1024 machines)
- Bits 11-0: Sequence (12 bits, up to 4096 IDs/ms per machine)

Single-row tables (scalars and dict of scalars) don't need `aai_id` because there's nothing to order.

## Object Class Features

**Operator Overloading:**
- All basic Python operations are overloaded (arithmetic, comparison, logical, etc.)
- Each operation creates a new table with the operation result asynchronously
- No in-place operations - all operations are immutable (return new Objects)
- All operations return awaitables that execute async ClickHouse queries via clickhouse-connect

**Extended Operations:**
- Set of framework-specific operations (to be defined in future releases)

**Example:**
```python
# Each operation creates a new table (all operations are async)
obj1 = await Object(data1)  # Creates table: data1_[oid1]
obj2 = await Object(data2)  # Creates table: data2_[oid2]
result = await (obj1 + obj2)  # Creates new table: result_[oid3]
```

## Immutability Principle

All operations are immutable - no in-place modifications. Each operation returns a new Object with a new underlying ClickHouse table. This ensures:

- Operation history preservation
- Reproducibility
- Safe concurrent execution
- Clear data lineage

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

## The data() Method

The `data()` method returns values directly based on the data type:

- **Scalar**: returns the value directly
- **Array**: returns a list of values
- **Dict (single row)**: returns dict directly
- **Dict (multiple rows)**: use `orient` parameter to control output format

### Scalar Example

```python
from aaiclick import create_object_from_value

# Create scalar object
obj = await create_object_from_value(42.0)

# Get scalar value directly
value = await obj.data()
print(value)  # 42.0

# Scalar addition
a = await create_object_from_value(100.0)
b = await create_object_from_value(50.0)
result = await (a + b)
print(await result.data())  # 150.0
```

### Array Example

```python
# Create array object
obj = await create_object_from_value([1, 2, 3, 4, 5])

# Get list of values directly
values = await obj.data()
print(values)  # [1, 2, 3, 4, 5]
```

### Dict Example (Single Row)

```python
from aaiclick import create_object_from_value

# Create dict object (single row)
obj = await create_object_from_value({"id": 1, "name": "Alice", "age": 30})

# Get dict directly
data = await obj.data()
print(data)  # {"id": 1, "name": "Alice", "age": 30}
```

### Orient Parameter (for Dict with Multiple Rows)

The `orient` parameter controls the output format when dict has multiple rows:

| Constant | Value | Description |
|----------|-------|-------------|
| `ORIENT_DICT` | `'dict'` | Returns first row as dict (default) |
| `ORIENT_RECORDS` | `'records'` | Returns list of dicts (one per row) |

```python
from aaiclick import ORIENT_DICT, ORIENT_RECORDS

# Default orient='dict' - returns first row as dict
data = await obj.data()

# orient='records' - returns list of dicts
data = await obj.data(orient=ORIENT_RECORDS)
print(data)  # [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
```
