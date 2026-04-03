# aaiclick

A Python framework that translates Python code into ClickHouse operations for big data computing.

## Overview

aaiclick provides a Python API for big data computing backed by ClickHouse. Data lives in ClickHouse as columnar tables; Python code orchestrates operations — arithmetic, filtering, aggregation, joins — that execute as ClickHouse queries. An orchestration layer (Airflow-inspired) manages task dependencies and scheduling, while the Object API (Pandas/SQLAlchemy-inspired) keeps the interface familiar. Runs locally with embedded chdb + SQLite, or scales out with remote ClickHouse + PostgreSQL.

## Installation

The base install includes embedded [chdb](https://clickhouse.com/docs/chdb) and SQLite — no external servers needed:

```bash
pip install aaiclick
python -m aaiclick setup
```

For a distributed deployment (remote ClickHouse server + PostgreSQL):

```bash
pip install "aaiclick[distributed]"
```

For AI features (lineage tracing, debug agents):

```bash
pip install "aaiclick[ai]"
# or everything:
pip install "aaiclick[all]"
```

## Orchestration

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

```bash
python sales_pipeline.py
```

## Data Operation Only Mode

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

## Documentation

- [Getting Started](https://aaiclick.readthedocs.io/en/latest/getting_started/) — installation, setup, quick example
- [Object API](https://aaiclick.readthedocs.io/en/latest/object/) — operators, aggregations, views, group by
- [Orchestration](https://aaiclick.readthedocs.io/en/latest/orchestration/) — `@task` and `@job` decorators, workers
- [Examples](https://aaiclick.readthedocs.io/en/latest/examples/basic_operators/) — runnable scripts for every feature
- [API Reference](https://aaiclick.readthedocs.io/en/latest/api/data/) — auto-generated from docstrings

## License

MIT License - see [LICENSE](LICENSE) for details.
