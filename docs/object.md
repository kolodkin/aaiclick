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

## Usage Example

```python
from aaiclick import create_object_from_value

# Create object from array
obj = await create_object_from_value([1, 2, 3, 4, 5])

# Get data with metadata
data = await obj.data()

# Access rows
print(data.rows)  # [(0, 1), (1, 2), (2, 3), (3, 4), (4, 5)]

# Access column metadata
print(data.columns["value"].fieldtype)  # 'a'
print(data.columns["row_id"].fieldtype) # 's'
```

## DataResult Structure

The `data()` method returns a `DataResult` object:

```python
@dataclass
class DataResult:
    rows: List[Tuple[Any, ...]]      # Table data
    columns: Dict[str, ColumnMeta]    # Column name -> metadata
```

## ColumnMeta Structure

```python
@dataclass
class ColumnMeta:
    fieldtype: Optional[str]  # 's' or 'a'
```
