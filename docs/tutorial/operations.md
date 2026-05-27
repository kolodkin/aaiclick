Operations
---

Operators on Objects build new Objects. They are lazy: an expression like
`a + b` records the operation but issues no query. ClickHouse runs the
computation only when you call `.data()` (or another terminal method). That
lets you chain several steps and have them execute as a single query.

Each operation below uses two source Objects created with `aai_id=True`, which
gives each its own table so they can be combined.

# Arithmetic

The standard arithmetic operators work element-wise:

```python
--8<-- "aaiclick/data/examples/basic_operators.py:arithmetic"
```

# Comparison

Comparisons return a `UInt8` column of `0`/`1` per row:

```python
--8<-- "aaiclick/data/examples/basic_operators.py:comparison"
```

# Bitwise

Bitwise operators apply per element on integer columns:

```python
--8<-- "aaiclick/data/examples/basic_operators.py:bitwise"
```

# Next

[Aggregations →](aggregations.md) — reduce an Object to a single value.

# See Also

- [Object API](../object.md) — operators, aggregations, views
- [Examples: Basic Operators](../examples/basic_operators.md) — the complete runnable script
