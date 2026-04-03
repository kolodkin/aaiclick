Getting Started
---

# Installation

=== "Local (chdb + SQLite)"

    No external servers needed — embedded ClickHouse and SQLite included:

    ```bash
    pip install aaiclick
    python -m aaiclick setup
    ```

=== "Distributed (ClickHouse + PostgreSQL)"

    For a remote ClickHouse server and PostgreSQL:

    ```bash
    pip install "aaiclick[distributed]"
    export AAICLICK_CH_URL="clickhouse://user:pass@host:8123/db"
    export AAICLICK_SQL_URL="postgresql+asyncpg://user:pass@host:5432/db"
    ```

=== "AI Features"

    Add lineage tracing and debug agents:

    ```bash
    pip install "aaiclick[ai]"
    # or everything at once:
    pip install "aaiclick[all]"
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

!!! warning "Always `await` operation results"
    `prices * tax_rate` returns a coroutine, not an Object.
    Forgetting `await` gives a confusing error downstream —
    not at the line where you forgot it.

??? info "Automatic cleanup"
    All objects created inside `data_context()` are cleaned up on exit —
    no manual table management required. Don't store Objects for use after
    the context exits.

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
