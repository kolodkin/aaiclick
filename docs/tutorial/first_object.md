Your First Object
---

An `Object` is a handle to a table in ClickHouse. You build one from ordinary
Python values; the data is stored column-wise and the `Object` references it —
nothing is held in Python memory.

This tutorial runs on the default local backend (embedded chdb + SQLite) — no
servers to start. Every snippet below runs inside a `data_context()` block,
which owns the lifecycle of the tables you create:

```python
import asyncio

from aaiclick import create_object_from_value
from aaiclick.data.data_context import data_context


async def main():
    async with data_context():
        ...  # create and use Objects here

asyncio.run(main())
```

# From a scalar

`create_object_from_value()` infers the type from the value you pass:

```python
--8<-- "aaiclick/data/examples/basic_operators.py:create_scalar"
```

`await obj.data()` pulls the value back into Python.

!!! warning "Always `await` operation results"
    Object methods are coroutines. Forgetting `await` surfaces as a confusing
    error downstream, not at the line you forgot.

# From a list

A list becomes a one-column table, one row per element:

```python
--8<-- "aaiclick/data/examples/basic_operators.py:create_list"
```

# From a dict

A dict of arrays becomes a multi-column table — one key per column, one row per
position. `data(orient="records")` returns rows as a list of dicts:

```python
--8<-- "aaiclick/data/examples/basic_operators.py:create_dict"
```

# Next

[Operations →](operations.md) — combine Objects with arithmetic, comparison,
and bitwise operators.

# See Also

- [Object API](../object.md) — the full surface of `Object`
- [DataContext](../data_context.md) — lifecycle, scopes, and staleness
- [Examples: Basic Operators](../examples/basic_operators.md) — the complete runnable script
