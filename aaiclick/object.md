# Object Class Documentation

## Overview

The `Object` class represents a data object stored in a ClickHouse table. Each Object instance corresponds to a ClickHouse table and supports operations through operator overloading that create new tables with results.

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

## Implementation Details

All operators use a common `_binary_operation` method that:
1. Creates a new Object to hold the result
2. Determines if the operation is on scalars or arrays
3. Selects the appropriate SQL template (scalar or array)
4. Executes the SQL query with the operator
5. Returns the new Object

**SQL Templates:**
- `binary_op_scalar.sql` - For scalar-to-scalar operations
- `binary_op_array.sql` - For array-to-array operations (preserves aai_id)

**Type Preservation:**
The result preserves the fieldtype metadata from the source objects, ensuring proper data type handling throughout operation chains.
