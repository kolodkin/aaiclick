Building & Combining Data
---

Duplicate a dataset with `copy()`, or grow it with `concat()` (new Object) or
`insert()` (in place).

# Copy

`copy()` materializes an independent duplicate — a new `Object` backed by a new
table:

```python
--8<-- "aaiclick/data/examples/data_manipulation.py:copy"
```

# Concatenate

`concat()` is non-mutating: the originals are unchanged and the result is a new
Object:

```python
--8<-- "aaiclick/data/examples/data_manipulation.py:concat"
```

# Insert

`insert()` mutates in place — same table, more rows. Use it to accumulate data
without leaving ClickHouse:

```python
--8<-- "aaiclick/data/examples/data_manipulation.py:insert"
```

# Next

[Orchestration →](orchestration.md) — turn these operations into a pipeline.

# See Also

- [Object API](../user_guide/object.md) — `copy`, `concat`, and `insert`
- [Examples: Data Manipulation](../examples/data_manipulation.md) — the complete runnable script
