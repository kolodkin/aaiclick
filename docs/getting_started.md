Getting Started
---

# Installation

=== "Local (chdb + SQLite)"

    No external servers needed — embedded ClickHouse and SQLite included:

    ```bash
    pip install aaiclick
    python -m aaiclick setup
    ```

    !!! warning "Upgrading over an existing `local.db`"
        SQLite databases are not migrated in place. When `local.db` predates the
        installed version, `setup` stops and names the missing columns — re-run
        with `--force` to recreate it. Local job/task history is lost; data
        objects in chdb are untouched. That check compares columns, not
        values: a `local.db` created before the default tenant id moved to
        `1 << 62` still seeds tenant `1` and needs `setup --force` too.

=== "Distributed (ClickHouse + PostgreSQL)"

    For a remote ClickHouse server and PostgreSQL:

    ```bash
    pip install "aaiclick[distributed]"
    python -m aaiclick setup
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

@data_context()
async def main():
    prices = await create_object_from_value([10.0, 20.0, 30.0])

    total = prices * 1.1                        # LazyOperator — no DB call yet
    print(await total.data())                   # [11.0, 22.0, 33.0]
    print(await total.mean().data())            # 22.0

asyncio.run(main())
```

!!! warning "Always `await` operation results"
    Forgetting `await` causes confusing errors downstream, not at the forgotten line.

# Environment Variables

| Variable                | Default                                    | Description                                                              |
|-------------------------|--------------------------------------------|--------------------------------------------------------------------------|
| `AAICLICK_CH_URL`       | `chdb:///~/.aaiclick/chdb_data`            | ClickHouse connection — `chdb://` for embedded, `clickhouse://` for remote |
| `AAICLICK_SQL_URL`      | `sqlite+aiosqlite:///~/.aaiclick/local.db` | Orchestration DB — SQLite (local) or PostgreSQL (distributed)            |

# Next Steps

- [Object API](user_guide/object.md) — operators, aggregations, views, group by
- [DataContext](user_guide/data_context.md) — lifecycle management, persistent objects
- [Orchestration](user_guide/orchestration.md) — `@task` and `@job` decorators, workers
- [Container images](user_guide/container_images.md) — published `aaiclick`, `aaiclick-docker`, `aaiclick-kubectl` images and their Kubernetes/Docker runtime requirements
- [Examples](examples/basic_operators.md) — runnable scripts for every feature
