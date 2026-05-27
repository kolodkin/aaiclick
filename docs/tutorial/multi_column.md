Multi-Column Data
---

A dict of arrays is a table with named columns. `group_by()` partitions the
rows by one or more key columns, and an aggregation then collapses each group
to a single row — the same GROUP BY you know from SQL, expressed in Python.

# Group and sum

Group by one column, sum another:

```python
--8<-- "aaiclick/data/examples/group_by.py:groupby_basic"
```

# Multiple keys

Pass several columns to group by their combination:

```python
--8<-- "aaiclick/data/examples/group_by.py:groupby_multikey"
```

# Several aggregations at once

`agg()` takes a mapping of column to aggregation, so one pass can compute
different reductions for different columns:

```python
--8<-- "aaiclick/data/examples/group_by.py:groupby_agg"
```

# Next

[Views & Filters →](views_filters.md) — slice a table with WHERE, ORDER BY, and
LIMIT.

# See Also

- [Object API](../object.md) — `group_by`, `agg`, and `having`
- [Examples: Group By](../examples/group_by.md) — every grouping pattern, including HAVING
- [Examples: Aggregation Table](../examples/aggregation_table.md) — wide aggregation output
