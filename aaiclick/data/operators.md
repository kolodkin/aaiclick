# Operators Module Documentation

## Overview

The `operators` module contains the database-level implementation for all binary operations on Object instances. All operators are dispatched through `_apply_operator_db()` which handles fieldtype combinations (array×array, array×scalar, scalar×scalar) via a unified SQL generation path.

## Design

All operators follow a consistent pattern:
- Object class methods (`__add__`, `__sub__`, etc.) call `_apply_operator(other, op_symbol)`
- `_apply_operator` builds `QueryInfo` for both operands and delegates to `_apply_operator_db()`
- Reverse operators (`__radd__`, `__rsub__`, etc.) swap operand order for `scalar op object` syntax
- Python scalars are inlined as `(SELECT literal AS value)` — no ClickHouse table created

## Operator Reference

### Arithmetic Operators

| Python | ClickHouse           | Expression                |
|--------|----------------------|---------------------------|
| `+`    | `+`                  | `a.value + b.value`       |
| `-`    | `-`                  | `a.value - b.value`       |
| `*`    | `*`                  | `a.value * b.value`       |
| `/`    | `/`                  | `a.value / b.value`       |
| `//`   | `intDiv(a, b)`       | `intDiv(a.value, b.value)`|
| `%`    | `%`                  | `a.value % b.value`       |
| `**`   | `power(a, b)`        | `power(a.value, b.value)` |

### Comparison Operators

| Python | ClickHouse | Expression            |
|--------|------------|-----------------------|
| `==`   | `=`        | `a.value = b.value`   |
| `!=`   | `!=`       | `a.value != b.value`  |
| `<`    | `<`        | `a.value < b.value`   |
| `<=`   | `<=`       | `a.value <= b.value`  |
| `>`    | `>`        | `a.value > b.value`   |
| `>=`   | `>=`       | `a.value >= b.value`  |

### Bitwise Operators

| Python | ClickHouse   | Expression                  |
|--------|--------------|-----------------------------|
| `&`    | `bitAnd()`   | `bitAnd(a.value, b.value)`  |
| `\|`   | `bitOr()`    | `bitOr(a.value, b.value)`   |
| `^`    | `bitXor()`   | `bitXor(a.value, b.value)`  |

## Usage

```python
import aaiclick

a = await aaiclick.create_object_from_value([10, 20, 30])
b = await aaiclick.create_object_from_value([2, 4, 5])

# Use Python operators — all dispatch through _apply_operator_db
result = await (a + b)   # [12, 24, 35]
result = await (a * b)   # [20, 80, 150]
result = await (a ** b)  # [100, 160000, 24300000]

# Scalar broadcast
result = await (a * 2)   # [20, 40, 60]
result = await (10 - a)  # [0, -10, -20] (reverse operator)
```

## Scalar Broadcasting

**Implementation**: `operators.py` — see `_apply_operator_db()` and `Object._scalar_query_info()`

When a Python scalar is used with an Object, it is inlined as a SQL literal `(SELECT 5 AS value)` with `FIELDTYPE_SCALAR`. The cross-join in `_apply_operator_db` handles all non-array×array cases uniformly — only the `aai_id` source differs.

### Aggregation Operators

Aggregation operators reduce an array to a scalar value. All computation happens within ClickHouse.

| Function | Description          | ClickHouse     | Returns       |
|----------|----------------------|----------------|---------------|
| `min()`  | Minimum value        | `min()`        | Scalar Object |
| `max()`  | Maximum value        | `max()`        | Scalar Object |
| `sum()`  | Sum of values        | `sum()`        | Scalar Object |
| `mean()` | Average value        | `avg()`        | Scalar Object |
| `std()`  | Standard deviation   | `stddevPop()`  | Scalar Object |

**Note:** Aggregation functions use streaming aggregation with O(1) memory.

### Set Operators

| Function                       | Description   | ClickHouse  | Returns      |
|--------------------------------|---------------|-------------|--------------|
| `unique_group(info, ch_client)`| Unique values | `GROUP BY`  | Array Object |

## Benefits of This Architecture

**Modularity**: Operator implementations are separate from the Object class
**Testability**: Each operator can be tested independently
**Maintainability**: Easy to add new operators or modify existing ones
**Clarity**: Clear separation between interface (Object) and implementation (operators)
