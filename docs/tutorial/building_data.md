Building & Combining Data
---

Two ways to grow a dataset: `concat()` builds a new Object and leaves the
originals untouched, while `insert()` appends rows to the existing table in
place. `copy()` materializes an independent duplicate.

# Copy

`copy()` creates a new table with the same data — a different `Object` backed
by a different table:

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
