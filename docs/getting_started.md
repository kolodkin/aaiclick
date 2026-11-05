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
        objects in chdb are untouched.

=== "Distributed (ClickHouse + PostgreSQL)"

    For a remote ClickHouse server and PostgreSQL:

    ```bash
    pip install "aaiclick[distributed]"
    python -m aaiclick setup
    export AAICLICK_CH_URL="clickhouse://user:pass@host:8123/db"
    export AAICLICK_SQL_URL="postgresql+asyncpg://user:pass@host:5432/db"
    ```

    Percent-encode reserved characters in the user or password (`@` → `%40`, `:` → `%3A`) in either URL.

=== "Lineage via MCP"

    Let your coding agent trace lineage over the MCP server:

    ```bash
    pip install "aaiclick[server]"
    # or everything at once:
    pip install "aaiclick[all]"
    python -m aaiclick local start
    ```

    Then connect the agent to `http://127.0.0.1:5255/mcp` — see
    [Lineage via MCP](user_guide/lineage.md).

# Your First Pipeline

Run a two-task job on a local server and watch it in the UI.

## 1. Install

```bash
mkdir my_project && cd my_project
pip install "aaiclick[server]"
```

## 2. Write a job

Save as `my_pipeline.py`:

```python
from aaiclick.orchestration import job, task


@task
async def add(a: int, b: int) -> int:
    return a + b


@task
async def double(x: int) -> int:
    return x * 2


@job("my_pipeline")
def my_pipeline(a: int = 1, b: int = 2):
    return double(x=add(a=a, b=b))  # double waits for add


if __name__ == "__main__":
    from aaiclick.orchestration import job_test
    job_test(my_pipeline, a=5)  # runs all tasks in-process for debugging
```

`python my_pipeline.py` runs it in-process, no server needed. The next steps run
it on a local server instead.

## 3. Start the server

In a second terminal, from the same directory so the worker can import `my_pipeline`:

```bash
python -m aaiclick local start
```

## 4. Register and run

```bash
python -m aaiclick register-job my_pipeline.my_pipeline
python -m aaiclick run-job my_pipeline --set a=5 --progress
```

```text
| Task        | Status    | Queue | Exec  |
|-------------|-----------|-------|-------|
| my_pipeline | COMPLETED | 445ms | 1.2s  |
| add         | COMPLETED | 26ms  | 19ms  |
| double      | COMPLETED | 58ms  | 23ms  |
```

Registering adds the job to the catalog so you can run it by name, set a
`--schedule`, or launch it from the UI. To skip registration, pass the dotted
path: `run-job my_pipeline.my_pipeline`.

## 5. Open the UI

Browse to <http://127.0.0.1:5255> to see the job, its task graph and logs.
The REST API docs are at <http://127.0.0.1:5255/api/v0/docs>.

# Environment Variables

| Variable                | Default                                    | Description                                                              |
|-------------------------|--------------------------------------------|--------------------------------------------------------------------------|
| `AAICLICK_CH_URL`       | `chdb:///~/.aaiclick/chdb_data`            | ClickHouse connection — `chdb://` for embedded, `clickhouse://` for remote |
| `AAICLICK_SQL_URL`      | `sqlite+aiosqlite:///~/.aaiclick/local.db` | Orchestration DB — SQLite (local) or PostgreSQL (distributed)            |

# Next Steps

- [Orchestration](user_guide/orchestration.md) — `@task` and `@job` decorators, workers
- [Object API](user_guide/object.md) — operators, aggregations, views, group by
- [ObjectContext](user_guide/object_context.md) — lifecycle, persistent objects, data operations without orchestration
- [Container images](user_guide/container_images.md) — published `aaiclick`, `aaiclick-docker`, `aaiclick-kubectl` images and their Kubernetes/Docker runtime requirements
- [Examples](examples/orchestration_basic.md) — runnable scripts for every feature
