Getting Started
---

# Installation

The base install includes embedded [chdb](https://clickhouse.com/docs/chdb) and SQLite — no external
servers needed:

```bash
pip install aaiclick
```

For a distributed deployment against a remote ClickHouse server and PostgreSQL, add the
`distributed` extra:

```bash
pip install "aaiclick[distributed]"
```

For AI features (lineage tracing, debug agents):

```bash
pip install "aaiclick[ai]"
# or everything at once:
pip install "aaiclick[all]"
```

To build the documentation locally:

```bash
pip install -r docs/requirements-docs.txt
mkdocs serve
```

# First Run

Initialize local storage (creates `~/.aaiclick/` with chdb data and SQLite database):

```bash
python -m aaiclick setup
```

# Quick Example

```python
import asyncio
from aaiclick import create_object_from_value
from aaiclick.data.data_context import data_context

async def main():
    async with data_context():
        # Create objects from Python values — schema is inferred automatically
        prices = await create_object_from_value([10.0, 20.0, 30.0])
        tax_rate = await create_object_from_value(0.1)

        # All computation runs inside ClickHouse
        tax = await (prices * tax_rate)
        total = await (prices + tax)

        print(await total.data())  # [11.0, 22.0, 33.0]

asyncio.run(main())
```

All objects created inside `data_context()` are automatically cleaned up on exit — no manual
table management required.

# Environment Variables

| Variable                | Default                                    | Description                                                              |
|-------------------------|--------------------------------------------|--------------------------------------------------------------------------|
| `AAICLICK_CH_URL`       | `chdb:///~/.aaiclick/chdb_data`            | ClickHouse connection — `chdb://` for embedded, `clickhouse://` for remote |
| `AAICLICK_SQL_URL`      | `sqlite+aiosqlite:///~/.aaiclick/local.db` | Orchestration DB — SQLite (local) or PostgreSQL (distributed)            |
| `AAICLICK_LOG_DIR`      | `~/.aaiclick/logs` / `/var/log/aaiclick`   | Log directory override (macOS default / Linux default)                   |

# Next Steps

- [Object API](object.md) — operators, aggregations, views, group by
- [DataContext](data_context.md) — lifecycle management, persistent objects
- [Orchestration](orchestration.md) — `@task` and `@job` decorators, workers
- [Examples](examples/basic_operators.md) — runnable scripts for every feature
