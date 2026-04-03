aaiclick
---

A Python framework that translates Python code into ClickHouse operations for big data computing.

Data lives in ClickHouse as columnar tables; Python code orchestrates operations —
arithmetic, filtering, aggregation, joins — that execute as ClickHouse queries.
Runs locally with embedded chdb + SQLite, or scales out with remote ClickHouse + PostgreSQL.

# Key Features

- **ClickHouse-Powered** — all computation runs inside ClickHouse. Python orchestrates, ClickHouse computes.
- **Familiar API** — Pandas-like operators (`+`, `-`, `*`, `.mean()`, `.group_by()`) on distributed data.
- **Local-First, Scale-Out** — start with embedded chdb + SQLite. Scale to remote ClickHouse + PostgreSQL with zero code changes.
- **Orchestration Built-In** — Airflow-inspired `@task` and `@job` decorators with automatic dependency resolution.
- **AI-Ready** — optional lineage tracing and debug agents powered by LLMs.

# Orchestration

Define tasks and jobs with decorators — all data operations execute as ClickHouse queries:

```python
from aaiclick import create_object_from_value
from aaiclick.orchestration import job, task

@task
async def load_sales():
    return await create_object_from_value({
        "region": ["US", "EU", "US", "EU", "US"],
        "amount": [500, 300, 150, 200, 80],
    })

@task
async def analyze(sales):
    # GROUP BY + SUM — runs as a single ClickHouse query
    by_region = await sales.group_by("region").sum("amount")
    print(await by_region.data())  # → {'region': ['EU', 'US'], 'amount': [500, 730]}

    # append rows without leaving ClickHouse
    await sales.insert({"region": ["JP"], "amount": [400]})

@job("sales_pipeline")
def sales_pipeline():
    sales = load_sales()
    # dependencies resolved from arguments
    result = analyze(sales=sales)
    return result

if __name__ == "__main__":
    from aaiclick.orchestration import job_test
    job_test(sales_pipeline)  # runs all tasks locally for debugging
```

# Data Operation Only Mode

Use `data_context()` directly for interactive work without orchestration:

```python
import asyncio
from aaiclick import create_object_from_value
from aaiclick.data.data_context import data_context

async def main():
    async with data_context():
        prices = await create_object_from_value([10.0, 20.0, 30.0])

        total = await (prices + prices * 0.1)  # scalars broadcast automatically
        print(await total.data())  # → [11.0, 22.0, 33.0]
        print(await (await total.mean()).data())  # → 22.0

asyncio.run(main())
```

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
