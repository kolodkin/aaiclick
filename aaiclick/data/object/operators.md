# Operators Module Documentation

## Overview

The `operators` module contains the database-level implementation for all binary operations on Object instances. All operators are dispatched through `_apply_operator_db()` which handles fieldtype combinations (array×array, array×scalar, scalar×scalar) via a unified SQL generation path.

## Design

All operators follow a consistent two-stage pattern:

- **Plan (sync, in `object.py`):** Each binary dunder (`__add__`, `__sub__`, etc.) calls `Object._plan_operator(other, op_symbol)` and returns a `LazyOperator`. No DB call. Reverse operators (`__radd__`, `__rsub__`, etc.) use `_plan_operator_reverse` which swaps operand order for `scalar op object` syntax.
- **Materialize (async, in this module):** Awaiting the `LazyOperator` triggers `_apply_operator_db()`, which builds `QueryInfo` for both operands and emits the `CREATE TABLE` + `INSERT INTO ... SELECT ...`. Python scalars are inlined as `(SELECT literal AS value)` — no extra ClickHouse table.

Shared schema-computation helpers (`_compute_operator_schema`, `_preview_operator_schema`, `_promote_arithmetic_type`, `_scalar_to_schema`) live in the neutral `schema_compute.py` module so both `_plan_operator` (preview) and `_apply_operator_db` (materialize) hit the same code — no drift between preview and result schemas.

See `docs/object.md` ("Lazy Operator Results") for the LazyOperator design, `.as_(name, scope=...)` naming API, and the materialize-on-await contract.

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

# `a + b` returns a LazyOperator; `await` materializes the result table.
result = await (a + b)   # [12, 24, 35]
result = await (a * b)   # [20, 80, 150]
result = await (a ** b)  # [100, 160000, 24300000]

# Scalar broadcast
result = await (a * 2)   # [20, 40, 60]
result = await (10 - a)  # [0, -10, -20] (reverse operator)

# Name the result table — see docs/object.md (Lazy Operator Results)
revenue = await (prices * quantities).as_("revenue")
```

## Scalar Broadcasting

**Implementation**: `operators.py` — see `_apply_operator_db()` and `Object._ensure_object()`

When a Python scalar is used with an Object, it is first converted to a scalar Object via `create_object_from_value`. The cross-join in `_apply_operator_db` handles all non-array×array cases uniformly.

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
