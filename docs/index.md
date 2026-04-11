aaiclick
---

A data orchestration framework built to make distributed computing easy, with three principles in mind:

1. **Simplicity** — Python-native syntax and dynamic task execution.
2. **Performance** — Utilizes ClickHouse's powerful distributed engine. Data lives in ClickHouse as columnar tables; Python code orchestrates operations — arithmetic, filtering, aggregation, joins — that execute as ClickHouse queries.
3. **AI Lineage Superpower** — Query your data flow. How did this value get here? Why don't we see that value there? Trace lineage across operations and debug pipelines with AI-powered agents.

Local (in-process, zero setup) and distributed (Docker Compose provided) deployments.
Runs locally with embedded chdb + SQLite, or scales out with remote ClickHouse + PostgreSQL.

**Early stage — looking for early adopters to join the ride and provide feedback.**

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
