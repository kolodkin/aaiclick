Aggregations
---

Aggregations reduce a column to a single value — `min`, `max`, `sum`, `mean`,
`std`. Like operators, they return an Object; call `.data()` to get the number.
The reduction runs in ClickHouse, so it works the same on ten rows or ten
billion.

# Basic statistics

```python
--8<-- "aaiclick/data/examples/statistics_ops.py:basic_stats"
```

# A worked example

The same methods describe any numeric column — here, a day of temperature
readings:

```python
--8<-- "aaiclick/data/examples/statistics_ops.py:temperature"
```

# Next

[Multi-Column Data →](multi_column.md) — group rows and aggregate per group.

# See Also

- [Object API](../user_guide/object.md) — the full method surface
- [Examples: Statistics](../examples/statistics.md) — the complete runnable script
