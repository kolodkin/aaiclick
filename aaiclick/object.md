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

### Arithmetic Operations

```python
import aaiclick as ai

# Create objects with scalar values
a = await ai.scalar(10)
b = await ai.scalar(5)

# Arithmetic operations
result = await (a + b)  # Addition: 15
result = await (a - b)  # Subtraction: 5
result = await (a * b)  # Multiplication: 50
result = await (a / b)  # Division: 2.0
result = await (a // b) # Floor Division: 2
result = await (a % b)  # Modulo: 0
result = await (a ** b) # Power: 100000

# Get the result
value = await result.data()
```

### Array Operations

```python
# Create objects with array values
a = await ai.array([1, 2, 3, 4, 5])
b = await ai.array([10, 20, 30, 40, 50])

# Element-wise operations
result = await (a + b)  # [11, 22, 33, 44, 55]
result = await (a * b)  # [10, 40, 90, 160, 250]

# Get the result
values = await result.data()
```

### Comparison Operations

```python
a = await ai.array([1, 2, 3, 4, 5])
b = await ai.array([3, 3, 3, 3, 3])

# Element-wise comparisons (return boolean arrays)
result = await (a == b)  # [False, False, True, False, False]
result = await (a < b)   # [True, True, False, False, False]
result = await (a >= b)  # [False, False, True, True, True]
```

### Bitwise Operations

```python
a = await ai.array([12, 10, 8])  # Binary: 1100, 1010, 1000
b = await ai.array([10, 12, 4])  # Binary: 1010, 1100, 0100

# Element-wise bitwise operations
result = await (a & b)  # Bitwise AND: [8, 8, 0]
result = await (a | b)  # Bitwise OR: [14, 14, 12]
result = await (a ^ b)  # Bitwise XOR: [6, 6, 12]
```

### Chained Operations

```python
a = await ai.array([1, 2, 3])
b = await ai.array([10, 20, 30])
c = await ai.array([100, 200, 300])

# Chain multiple operations
result = await ((a + b) * c)  # [1100, 4400, 9900]
```

## Implementation Details

### Binary Operation Pattern

All operators use a common `_binary_operation` method that:
1. Creates a new Object to hold the result
2. Determines if the operation is on scalars or arrays
3. Selects the appropriate SQL template
4. Executes the SQL query with the operator
5. Returns the new Object

### SQL Templates

Two SQL templates handle operations:
- `binary_op_scalar.sql` - For scalar-to-scalar operations
- `binary_op_array.sql` - For array-to-array operations (preserves aai_id)

### Type Preservation

The result preserves the fieldtype metadata from the source objects, ensuring proper data type handling throughout operation chains.
