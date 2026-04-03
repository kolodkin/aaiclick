aaiclick
---

A Python framework that translates Python code into ClickHouse operations for big data computing.

Data lives in ClickHouse as columnar tables; Python code orchestrates operations —
arithmetic, filtering, aggregation, joins — that execute as ClickHouse queries.
Runs locally with embedded chdb + SQLite, or scales out with remote ClickHouse + PostgreSQL.

```python
import asyncio
from aaiclick import create_object_from_value
from aaiclick.data.data_context import data_context

async def main():
    async with data_context():
        prices = await create_object_from_value([10.0, 20.0, 30.0])
        tax_rate = await create_object_from_value(0.1)

        total = await (prices + prices * tax_rate)
        print(await total.data())  # → [11.0, 22.0, 33.0]

        print(await (await total.mean()).data())  # → 22.0

asyncio.run(main())
```

# Key Features

- **ClickHouse-Powered** — all computation runs inside ClickHouse. Python orchestrates, ClickHouse computes.
- **Familiar API** — Pandas-like operators (`+`, `-`, `*`, `.mean()`, `.group_by()`) on distributed data.
- **Local-First, Scale-Out** — start with embedded chdb + SQLite. Scale to remote ClickHouse + PostgreSQL with zero code changes.
- **Orchestration Built-In** — Airflow-inspired `@task` and `@job` decorators with automatic dependency resolution.
- **AI-Ready** — optional lineage tracing and debug agents powered by LLMs.

# Quick Start

```bash
pip install aaiclick
python -m aaiclick setup
```

- [Getting Started](getting_started.md) — installation, setup, environment variables
- [Object API](object.md) — operators, aggregations, views, group by
- [Orchestration](orchestration.md) — `@task` and `@job` decorators, workers
- [Examples](examples/basic_operators.md) — runnable scripts for every feature
- [API Reference](api/data.md) — auto-generated from docstrings
