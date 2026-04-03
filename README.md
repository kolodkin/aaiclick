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

## Data Operations

All computation runs inside ClickHouse — Python only orchestrates:

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

        # Aggregations
        print(await (await total.mean()).data())  # → 22.0

asyncio.run(main())
```

## Orchestration

Define tasks and jobs with decorators — dependencies are resolved automatically:

```python
from aaiclick.orchestration import job, task, tasks_list

@task
async def extract() -> dict:
    return {"revenue": [100, 200, 300], "region": ["US", "EU", "US"]}

@task
async def transform(data: dict) -> list:
    return [r for r, region in zip(data["revenue"], data["region"]) if region == "US"]

@job("revenue_pipeline")
def revenue_pipeline():
    raw = extract()
    filtered = transform(data=raw)
    return tasks_list(raw, filtered)
```

Run locally for development or deploy with workers for production:

```bash
# Test locally
python -c "from myapp import revenue_pipeline; from aaiclick.orchestration import job_test; job_test(revenue_pipeline)"

# Production: start workers
python -m aaiclick worker start
```

## Documentation

- [Getting Started](https://aaiclick.readthedocs.io/en/latest/getting_started/) — installation, setup, quick example
- [Object API](https://aaiclick.readthedocs.io/en/latest/object/) — operators, aggregations, views, group by
- [Orchestration](https://aaiclick.readthedocs.io/en/latest/orchestration/) — `@task` and `@job` decorators, workers
- [Examples](https://aaiclick.readthedocs.io/en/latest/examples/basic_operators/) — runnable scripts for every feature
- [API Reference](https://aaiclick.readthedocs.io/en/latest/api/data/) — auto-generated from docstrings

## License

MIT License - see [LICENSE](LICENSE) for details.
