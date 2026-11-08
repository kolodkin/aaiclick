Multi-Column Data
---

`group_by()` partitions rows by one or more key columns, and an aggregation
collapses each group to one row — SQL's GROUP BY, in Python.

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

`agg()` maps columns to aggregations, so one pass computes different reductions
per column:

```python
--8<-- "aaiclick/data/examples/group_by.py:groupby_agg"
```

# Next

[Views & Filters →](views_filters.md) — slice a table with WHERE, ORDER BY, and
LIMIT.

# See Also

- [Object API](../user_guide/object.md) — `group_by`, `agg`, and `having`
- [Examples: Group By](../examples/group_by.md) — every grouping pattern, including HAVING
- [Examples: Aggregation Table](../examples/aggregation_table.md) — wide aggregation output
