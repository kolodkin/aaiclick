# Object Class

The `Object` class represents data stored in ClickHouse tables. Each parameter (both positional args and keyword args) is wrapped in an Object class that manages ClickHouse table operations.

## Table Naming Convention

- Each parameter gets a dedicated ClickHouse table named: `attr_name_[oid]`
- The table name is stored in the Object class instance

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

### Fieldtype Values

| Value | Meaning |
|-------|---------|
| `s` | Scalar - single value per row |
| `a` | Array - column represents array elements across rows |

### Example Column Comment

```yaml
{fieldtype: a}
```

This indicates an array column.

## The data() Method

The `data()` method returns values directly based on the data type:

- **Scalar**: returns the value directly
- **Array**: returns a list of values
- **Dict**: returns a dict with column names as keys

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

### Dict Example

```python
# Create dict object
obj = await create_object_from_value({"id": 1, "name": "Alice", "age": 30})

# Get dict directly
data = await obj.data()
print(data)  # {"id": 1, "name": "Alice", "age": 30}
```
