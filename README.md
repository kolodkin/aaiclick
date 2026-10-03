<img src="docs/assets/favicon.svg" alt="aaiclick logo" width="96" />

# aaiclick

aaiclick is a data orchestration framework built to make distributed computing easy, with five principles in mind:

1. **Simplicity** — Plain Python, no SQL. Decorate functions into tasks and jobs, run with zero setup on your laptop, scale out unchanged.
2. **Dynamic Graphs** — Tasks spawn tasks. The pipeline shapes itself to the data while it runs instead of being declared up front.
3. **Performance** — Data lives and computes in ClickHouse. Python only orchestrates; nothing is shuffled through Python memory.
4. **Containerized Runs** — Pin a container image, by tag or git SHA, per job or per task. Host and container tasks mix in one pipeline.
5. **Lineage over MCP** — Every operation is recorded. Your coding agent (Claude Code, Codex, any MCP client) answers "how did this value get here?" straight from the lineage.

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

For lineage triage with your coding agent (Claude Code, Codex, any MCP client):

```bash
pip install "aaiclick[server]"   # or all extras: pip install "aaiclick[all]"
python -m aaiclick local start   # REST + MCP server on http://127.0.0.1:5255
```

Then point the agent at `http://127.0.0.1:5255/mcp` — see the [Lineage guide](docs/user_guide/lineage.md).

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

```bash
python sales_pipeline.py
```

## Data Operation Only Mode

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

## Documentation

[aaiclick.readthedocs.io](https://aaiclick.readthedocs.io/en/stable/)

## License

MIT License - see [LICENSE](LICENSE) for details.
