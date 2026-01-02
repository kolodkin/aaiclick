# Operators Module Documentation

## Overview

The `operators` module contains static async functions that implement all binary operations for Object instances. Each function is a standalone implementation that can be called directly or through operator overloading.

## Design

All operator functions follow a consistent pattern:
- Take two Object parameters (`a` and `b`)
- Return a new Object with the result
- Are async functions (must be awaited)
- Delegate to `_binary_operator` method on the Object class

## Function Reference

### Arithmetic Operators

| Function | Operator | Description | ClickHouse |
|----------|----------|-------------|------------|
| `add(a, b)` | `+` | Addition | `+` |
| `sub(a, b)` | `-` | Subtraction | `-` |
| `mul(a, b)` | `*` | Multiplication | `*` |
| `truediv(a, b)` | `/` | Division | `/` |
| `floordiv(a, b)` | `//` | Floor Division | `DIV` |
| `mod(a, b)` | `%` | Modulo | `%` |
| `pow(a, b)` | `**` | Power | `power()` |

### Comparison Operators

| Function | Operator | Description | ClickHouse |
|----------|----------|-------------|------------|
| `eq(a, b)` | `==` | Equal | `=` |
| `ne(a, b)` | `!=` | Not Equal | `!=` |
| `lt(a, b)` | `<` | Less Than | `<` |
| `le(a, b)` | `<=` | Less or Equal | `<=` |
| `gt(a, b)` | `>` | Greater Than | `>` |
| `ge(a, b)` | `>=` | Greater or Equal | `>=` |

### Bitwise Operators

| Function | Operator | Description | ClickHouse |
|----------|----------|-------------|------------|
| `and_(a, b)` | `&` | Bitwise AND | `bitAnd()` |
| `or_(a, b)` | `\|` | Bitwise OR | `bitOr()` |
| `xor(a, b)` | `^` | Bitwise XOR | `bitXor()` |

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

All operator functions delegate to the `_binary_operator` method on the Object class:

```python
async def add(a: "Object", b: "Object") -> "Object":
    """Add two objects together."""
    return await a._binary_operator(b, "+")
```

The `_binary_operator` method:
1. Creates a new Object to hold the result
2. Determines if operating on scalars or arrays
3. Selects appropriate SQL template
4. Executes SQL with the operator string
5. Returns the new Object

## Benefits of This Architecture

**Modularity**: Operator implementations are separate from the Object class
**Testability**: Each operator can be tested independently
**Reusability**: Functions can be called directly or through operators
**Maintainability**: Easy to add new operators or modify existing ones
**Clarity**: Clear separation between interface (Object) and implementation (operators)
