# aaiclick

A Python framework that translates Python code into ClickHouse operations for big data computing.

## Overview

aaiclick is a data orchestration framework built with three principles in mind:

1. **Simplicity** — Python-native syntax and dynamic task execution.
2. **Performance** — Utilizes ClickHouse's powerful distributed engine. Data lives in ClickHouse as columnar tables; Python code orchestrates operations — arithmetic, filtering, aggregation, joins — that execute as ClickHouse queries.
3. **AI Lineage Superpower** — Query your data flow. How did this value get here? Why don't we see that value there? Trace lineage across operations and debug pipelines with AI-powered agents.

Local (in-process, zero setup) and distributed (Docker Compose provided) deployments.
Runs locally with embedded chdb + SQLite, or scales out with remote ClickHouse + PostgreSQL.

**Early stage — looking for early adopters to join the ride and provide feedback.**

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
# or all extras:
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

[aaiclick.readthedocs.io](https://aaiclick.readthedocs.io/en/latest/)

## License

MIT License - see [LICENSE](LICENSE) for details.
