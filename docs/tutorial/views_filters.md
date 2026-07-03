Views & Filters
---

A `View` is a read-only window over a table's data, defined by query
constraints — `where`, `order_by`, `limit`, `offset`. A view copies nothing;
it just records the constraints and applies them when you read it.

# Filter with `where`

The `where` string is a ClickHouse expression over the table's columns:

```python
--8<-- "aaiclick/data/examples/views.py:view_where"
```

# Sort with `order_by`

```python
--8<-- "aaiclick/data/examples/views.py:view_orderby"
```

# Page with `limit` and `offset`

```python
--8<-- "aaiclick/data/examples/views.py:view_limit"
```

# Select a column

Indexing a multi-column Object with a column name returns a view of that
column:

```python
--8<-- "aaiclick/data/examples/dict_selectors.py:select_basic"
```

Selected columns behave like any other Object — you can operate on them:

```python
--8<-- "aaiclick/data/examples/dict_selectors.py:select_operate"
```

!!! warning "Views are read-only"
    A view references existing data; it has no table of its own. Calling
    `insert()` on a view raises `RuntimeError`.

# Next

[Building & Combining Data →](building_data.md) — copy, concatenate, and append
rows.

# See Also

- [Object API](../user_guide/object.md) — `view`, column selection, and operators
- [Examples: Views](../examples/views.md) — every constraint combination
- [Examples: Selectors](../examples/selectors.md) — column selection and metadata
