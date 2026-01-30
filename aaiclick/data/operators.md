# Operators Module Documentation

## Overview

The `operators` module contains static async functions that implement all binary operations for Object instances. Each function is a standalone implementation that can be called directly or through operator overloading.

## Design

All operator functions follow a consistent pattern:
- Take two Object parameters (`obj_a` and `obj_b`)
- Return a new Object with the result
- Are async functions (must be awaited)
- Delegate to `_apply_operator` method on the Object class

## Function Reference

### Arithmetic Operators

| Function | Operator | Description | ClickHouse |
|----------|----------|-------------|------------|
| `add(obj_a, obj_b)` | `+` | Addition | `+` |
| `sub(obj_a, obj_b)` | `-` | Subtraction | `-` |
| `mul(obj_a, obj_b)` | `*` | Multiplication | `*` |
| `truediv(obj_a, obj_b)` | `/` | Division | `/` |
| `floordiv(obj_a, obj_b)` | `//` | Floor Division | `DIV` |
| `mod(obj_a, obj_b)` | `%` | Modulo | `%` |
| `pow(obj_a, obj_b)` | `**` | Power | `power()` |

### Comparison Operators

| Function | Operator | Description | ClickHouse |
|----------|----------|-------------|------------|
| `eq(obj_a, obj_b)` | `==` | Equal | `=` |
| `ne(obj_a, obj_b)` | `!=` | Not Equal | `!=` |
| `lt(obj_a, obj_b)` | `<` | Less Than | `<` |
| `le(obj_a, obj_b)` | `<=` | Less or Equal | `<=` |
| `gt(obj_a, obj_b)` | `>` | Greater Than | `>` |
| `ge(obj_a, obj_b)` | `>=` | Greater or Equal | `>=` |

### Bitwise Operators

| Function | Operator | Description | ClickHouse |
|----------|----------|-------------|------------|
| `and_(obj_a, obj_b)` | `&` | Bitwise AND | `bitAnd()` |
| `or_(obj_a, obj_b)` | `\|` | Bitwise OR | `bitOr()` |
| `xor(obj_a, obj_b)` | `^` | Bitwise XOR | `bitXor()` |

## Usage

### Via Operator Overloading (Recommended)

```python
import aaiclick

a = await aaiclick.create_object_from_value([10, 20, 30])
b = await aaiclick.create_object_from_value([2, 4, 5])

# Use Python operators
result = await (a + b)   # Uses operators.add(a, b)
result = await (a * b)   # Uses operators.mul(a, b)
result = await (a ** b)  # Uses operators.pow(a, b)
```

### Direct Function Calls

```python
from aaiclick import operators
import aaiclick

a = await aaiclick.create_object_from_value([10, 20, 30])
b = await aaiclick.create_object_from_value([2, 4, 5])

# Call operator functions directly
result = await operators.add(a, b)
result = await operators.mul(a, b)
result = await operators.pow(a, b)
```

## Implementation Details

All operator functions delegate to the `_apply_operator` method on the Object class:

```python
async def add(obj_a: "Object", obj_b: "Object") -> "Object":
    """Add two objects together."""
    return await obj_a._apply_operator(obj_b, "+")
```

The `_apply_operator` method:
1. Creates a new Object to hold the result
2. Determines if operating on scalars or arrays
3. Selects appropriate SQL template
4. Executes SQL with the operator string
5. Returns the new Object

### Set Operations

| Function | Description | ClickHouse | Returns |
|----------|-------------|------------|---------|
| `unique_agg(info, ch_client)` | Unique values | `GROUP BY` | Array Object |

**Note:** Uses `GROUP BY` instead of `DISTINCT` for better performance on large datasets.

## Benefits of This Architecture

**Modularity**: Operator implementations are separate from the Object class
**Testability**: Each operator can be tested independently
**Reusability**: Functions can be called directly or through operators
**Maintainability**: Easy to add new operators or modify existing ones
**Clarity**: Clear separation between interface (Object) and implementation (operators)
