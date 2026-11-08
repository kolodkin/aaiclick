Aggregations
---

Aggregations — `min`, `max`, `sum`, `mean`, `std` — reduce a column to one
value. Like operators they return an Object; `.data()` gets the number. The
reduction runs in ClickHouse, so ten rows or ten billion work the same.

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
