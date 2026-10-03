aaiclick
---

A data orchestration framework built to make distributed computing easy, with five principles in mind:

1. **Simplicity** — Write pandas-style Python, never SQL. Pipelines are `@task` and `@job` decorators. `pip install`, run on embedded chdb + SQLite with zero setup, and the same code scales out to ClickHouse + PostgreSQL with N workers.
2. **Dynamic Graphs** — The job graph is built while the job runs, not declared up front. Any task can return new tasks; dependencies come from the values they pass (`b = step_b(x=a)`) or an explicit `a >> b`. Fan out over data you only discover mid-run, and fan in with a `Group` that hands a consumer every member's result.
3. **Performance** — Data never leaves ClickHouse. Arithmetic, filtering, aggregation and joins compile to columnar queries; Python only orchestrates. `map()` / `reduce()` fan work out across workers.
4. **Containerized Runs** — Run a job's tasks as host subprocesses, Docker containers, or Kubernetes Pods. Each task picks its image: a prebuilt one, or your repo built at a given git SHA by a build task inside the job graph. Host and container tasks mix in one job.
5. **Lineage** — Every operation is recorded with its SQL. Ask "how did this value get here?" and your coding agent (Claude Code, Codex, any MCP client) traces it, inspects jobs and task logs, and re-runs tasks over MCP. A web UI covers the same ground for humans.

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
async def analyze(sales) -> dict:
    # GROUP BY + SUM runs as a single ClickHouse query; return a plain dict
    by_region = await sales.group_by("region").sum("amount")
    return await by_region.data()  # → {'region': ['US', 'EU'], 'amount': [730, 500]}

@task
async def report(summary: dict):
    # receives the plain Python dict returned by analyze()
    print(f"Regions: {summary['region']}")  # → Regions: ['US', 'EU']
    print(f"Amounts: {summary['amount']}")  # → Amounts: [730, 500]
    print(f"Total:   {sum(summary['amount'])}")  # → Total: 1230

@job("sales_pipeline")
def sales_pipeline():
    sales = load_sales()                # a Task, not data — nothing runs yet
    summary = analyze(sales=sales)      # gets the sales task's return value at runtime
    return report(summary=summary)      # the dict flows to report()

if __name__ == "__main__":
    from aaiclick.orchestration import job_test
    job_test(sales_pipeline)  # runs all tasks locally for debugging
```

# Data Operation Only Mode

Use `data_context()` directly for interactive work without orchestration.
Decorate an async function to wrap its whole body in a context:

```python
import asyncio
from aaiclick import create_object_from_value
from aaiclick.data.data_context import data_context

@data_context()
async def main():
    prices = await create_object_from_value([10.0, 20.0, 30.0])

    total = prices * 1.1                         # LazyOperator — no DB call yet
    print(await total.data())                    # → [11.0, 22.0, 33.0]
    print(await total.mean().data())             # → 22.0

asyncio.run(main())
```

`data_context()` also works as an `async with` block when you only need part of
a function inside the context.

# Quick Start

```bash
pip install aaiclick
python -m aaiclick setup
```

- [Getting Started](getting_started.md) — installation, setup, environment variables
- [Object API](user_guide/object.md) — operators, aggregations, views, group by
- [Orchestration](user_guide/orchestration.md) — `@task` and `@job` decorators, workers
- [Examples](examples/basic_operators.md) — runnable scripts for every feature
