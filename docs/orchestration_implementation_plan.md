# aaiclick Orchestration Implementation Plan

## Goal

Implement orchestration backend to support basic job creation and execution, starting with this simple example:

```python
async def task1():
    a = 1
    b = 2
    c = a + b
    print(c)

def main():
    job = create_job("orch_basic_example", task1)
    job.test()

if __name__ == "__main__":
    main()
```

## Implementation Phases

### Phase 1: Database Setup

**Objective**: Set up PostgreSQL schema for basic job/task tracking

**Tasks**:
1. ✅ Add PostgreSQL dependencies to `pyproject.toml`:
   - `sqlmodel` (includes Pydantic as transitive dependency - SQLModel is built on Pydantic)
   - `asyncpg` (async PostgreSQL driver)
   - `alembic` (database migrations)
   - `psycopg2-binary` (for Alembic migrations - sync driver)

2. ✅ Create `aaiclick/orchestration/models.py`:
   - `JobStatus` enum (PENDING, RUNNING, COMPLETED, FAILED)
   - `TaskStatus` enum (PENDING, CLAIMED, RUNNING, COMPLETED, FAILED)
   - `WorkerStatus` enum (ACTIVE, IDLE, STOPPED)
   - `Job` model (minimal fields: id, name, status, created_at)
   - `Task` model (minimal fields: id, job_id, entrypoint, kwargs, status, result_table_id)
   - `Worker` model (minimal fields: id, hostname, pid, status)
   - `Group` model (for task grouping with parent_group_id for nesting)
   - `Dependency` model (unified dependency tracking: previous/next id+type)
   - **Note**: See orchestration.md for ID generation strategy (snowflake IDs from `aaiclick.snowflake`)

3. ✅ Initialize Alembic:
   ```bash
   alembic init aaiclick/orchestration/migrations
   ```

4. ✅ Create initial migration:
   - Configure `alembic.ini` and `env.py` for SQLModel
   - Create migration for Job, Task, Worker, Group, Dependency tables
   - Includes upgrade() and downgrade() functions

5. ✅ Add environment variables to `CLAUDE.md`:
   ```bash
   POSTGRES_HOST=localhost
   POSTGRES_PORT=5432
   POSTGRES_USER=aaiclick
   POSTGRES_PASSWORD=secret
   POSTGRES_DB=aaiclick
   AAICLICK_LOG_DIR=<optional>  # Override default OS-dependent log directory
   ```

6. ✅ Update CI/CD workflow (`.github/workflows/test.yaml`):
   - Add PostgreSQL service (similar to ClickHouse service)
   - Add step to run Alembic migrations before tests
   - Add PostgreSQL environment variables to test and example steps
   - Migration step should gracefully skip if `alembic.ini` doesn't exist yet

**Deliverables**:
- `aaiclick/orchestration/models.py` with basic models
- `aaiclick/orchestration/migrations/` with Alembic setup
- Initial migration script
- Database can be created with `alembic upgrade head`
- CI/CD workflow configured with PostgreSQL service and migrations

---

### Phase 2: Core Factories ✅

**Objective**: Implement `create_task()` and `create_job()` factories

**Implementation**: See `aaiclick/orchestration/factories.py` for complete implementation.

**Tasks**:
1. ✅ `aaiclick/orchestration/__init__.py`:
   - Exports: `create_job`, `create_task`, `Job`, `JobStatus`, `Task`, `TaskStatus`, `Worker`, `WorkerStatus`, `Group`, `Dependency`
   - See: `aaiclick/orchestration/__init__.py`

2. ✅ `aaiclick/orchestration/factories.py`:
   - `create_task(callback: str, kwargs: dict = None) -> Task`
     - Accepts callback as string (e.g., "mymodule.task1")
     - Generates snowflake ID for task using `get_snowflake_id()`
     - Creates Task object (not committed to DB)
     - Defaults kwargs to empty dict

   - `create_job(name: str, entry: Union[str, Task]) -> Job`
     - Accepts callback string or Task object
     - Generates snowflake ID for job using `get_snowflake_id()`
     - Creates Job and initial Task
     - Commits both to PostgreSQL with JSON serialization for kwargs
     - Returns Job object

   - See: `aaiclick/orchestration/factories.py:12-107`

3. ✅ `aaiclick/orchestration/context.py`:
   - Each OrchContext creates its own SQLAlchemy AsyncEngine on `__aenter__`
   - Engine disposed on `__aexit__` (proper async lifecycle)
   - Engine initialized with env vars (POSTGRES_HOST, POSTGRES_PORT, etc.)
   - Each operation creates AsyncSession from context's engine for transactions
   - OrchContext provides access via ContextVar pattern
   - Ensures test isolation without cleanup fixtures
   - See: `aaiclick/orchestration/context.py`

4. ✅ Database persistence in `create_job()`:
   - Uses `get_orch_context_session()` to get AsyncSession
   - Inserts Job and Task using SQLAlchemy ORM (session.add, session.commit)
   - BIGINT IDs for Job and Task (snowflake IDs)
   - JSON-serialized kwargs handled automatically by SQLModel
   - Transaction isolation via session
   - Returns Job with id populated
   - See: `aaiclick/orchestration/factories.py:30-107`

5. ✅ CLI Migration Support:
   - `python -m aaiclick migrate` - runs database migrations
   - `aaiclick/__main__.py` - CLI entry point
   - `aaiclick/orchestration/migrate.py` - programmatic Alembic runner
   - Migrations bundled with package (architecture requirement)
   - See: `aaiclick/__main__.py` and `aaiclick/orchestration/migrate.py`

**Example Usage**:
```python
from aaiclick.orchestration import create_job

# Using callback string
job = await create_job("my_job", "mymodule.task1")
print(f"Job {job.id} created")
```

**Deliverables**:
- ✅ `create_task()` factory working
- ✅ `create_job()` factory working
- ✅ Jobs and tasks persisted to PostgreSQL
- ✅ Migration CLI working
- ✅ Comprehensive unit tests with database verification
- ✅ All 505 tests passing in CI/CD

---

### Phase 3: Job.test() Method

**Objective**: Implement synchronous job testing (similar to Airflow)

**Note**: `job.test()` invokes the worker execute flow - it simulates a worker claiming and executing tasks, but runs synchronously in the current process for testing/debugging.

**Tasks**:
1. Add `test()` method to `Job` model in `models.py`:
   ```python
   def test(self):
       """Execute job synchronously in current process (test mode)

       Invokes the worker execute flow for testing/debugging.
       Similar to Airflow's test execution mode.
       """
       import asyncio
       asyncio.run(self._test_async())
   ```

2. Implement `_test_async()` helper:
   - Create in-process worker simulation
   - Use worker execute flow (claim and execute tasks)
   - Update job status (PENDING → RUNNING → COMPLETED/FAILED)
   - Handle errors

3. Create `aaiclick/orchestration/execution.py`:
   - `execute_task(task: Task) -> Any`
     - Import callback function from entrypoint string
     - Deserialize kwargs (basic support for pyobj type)
     - Capture stdout/stderr to log file
     - Call function with kwargs (await if async function)
     - Return result

4. Implement task logging:
   - Create `aaiclick/orchestration/logging.py`:
     - `get_logs_dir()` - Returns OS-dependent log directory (see orchestration.md)
     - `capture_task_output(task_id: int)` context manager
     - Redirects stdout and stderr to log file
     - Log path: `{get_logs_dir()}/{task_id}.log`
     - For distributed workers: set AAICLICK_LOG_DIR to shared mount
     - Both stdout and stderr write to the same log file
     - Ensure log directory exists before writing
     - Flush logs after each write for real-time visibility

   - Use in `execute_task()`:
     ```python
     async def execute_task(task: Task) -> Any:
         with capture_task_output(task.id):
             # All print() and errors go to {AAICLICK_LOG_DIR}/{task.id}.log
             result = await func(**kwargs)
         return result
     ```

5. Implement task execution loop in `_test_async()`:
   - Query pending tasks for this job (ordered by created_at)
   - For each task:
     - Update status to RUNNING
     - Execute via `execute_task()` (worker execute flow)
     - Store result if applicable
     - Update status to COMPLETED
     - Handle failures (set status to FAILED, store error)

6. Add result handling:
   - If task returns an Object, store table_id in result_table_id
   - For simple values, serialize to JSONB or skip

**Example Flow**:
```python
job = create_job("example", "mymodule.task1")
job.test()  # Blocks until job completes (test mode)
# Job status is now COMPLETED
```

**Deliverables**:
- `Job.test()` executes all tasks in job using worker execute flow
- Task stdout/stderr captured to `{AAICLICK_LOG_DIR}/{task_id}.log`
- Task results captured
- Job status transitions work correctly
- Basic example from goal works end-to-end

---

### Phase 4: OrchContext Integration

**Objective**: Create OrchContext for orchestration and make both contexts available during task execution

**Tasks**:
1. ✅ Created `aaiclick/orchestration/context.py`:
   - Define `OrchContext` class
   - Signature: `def __init__(self)`
   - Creates SQLAlchemy AsyncEngine in `__aenter__` (per-context)
   - Disposes engine in `__aexit__` (proper async cleanup)
   - Each operation creates AsyncSession from context's engine
   - Implements context manager protocol (`__aenter__`, `__aexit__`)
   - **Implementation**: `aaiclick/orchestration/context.py:38-127`

2. ✅ Context-local storage for OrchContext:
   ```python
   # In aaiclick/orchestration/context.py
   from contextvars import ContextVar

   _current_orch_context: ContextVar['OrchContext'] = ContextVar('current_orch_context')

   def get_orch_context() -> OrchContext:
       """Get current OrchContext (for orchestration operations)"""
       try:
           return _current_orch_context.get()
       except LookupError:
           raise RuntimeError("No active OrchContext")
   ```
   **Implementation**: `aaiclick/orchestration/context.py:14-38`

3. ⚠️ Update `execute_task()` to use both contexts (planned):
   ```python
   async def execute_task(task: Task) -> Any:
       # Both contexts available during task execution
       async with DataContext() as data_ctx:
           async with OrchContext() as orch_ctx:
               # Import and execute function
               func = import_callback(task.entrypoint)
               # Task can use both data_ctx and orch_ctx
               result = await func(**task.kwargs)
               return result
   ```

4. ⚠️ Add `apply()` method to OrchContext (planned):
   - Accept Task, Group, or list
   - Create AsyncSession from global engine
   - Generate snowflake IDs for Groups using `get_snowflake_id()` (if not already set)
   - Set job_id on all tasks and groups
   - Insert into PostgreSQL using ORM (session.add, session.commit)
   - Return committed objects

**Deliverables**:
- ✅ Per-context SQLAlchemy AsyncEngine (created on enter, disposed on exit)
- ✅ OrchContext class (no job_id parameter - simplified)
- ⚠️ Tasks execute with both DataContext (data) and OrchContext (orchestration) (planned)
- ✅ OrchContext available via `get_orch_context()`
- ✅ DataContext remains unchanged (backward compatible)
- ⚠️ `orch_ctx.apply()` works for committing tasks (planned)
- ✅ Each operation creates its own AsyncSession from context's engine
- ✅ Proper async lifecycle ensures test isolation without cleanup fixtures

---

### Phase 5: Testing & Examples

**Objective**: Validate basic implementation with tests and examples

**Tasks**:
1. Create `tests/test_orchestration_basic.py`:
   - Test `create_task()` factory
   - Test `create_job()` factory
   - Test `job.test()` execution
   - Test task logging (verify log file created)
   - Test task with ClickHouse Object operations

2. Create `examples/orchestration_basic.py`:
   - Simple arithmetic task (from goal)
   - Task that creates and processes Objects
   - Task that prints results

3. Add documentation to README or docs:
   - Quick start guide
   - Basic usage examples
   - Environment setup instructions

4. Test database migrations:
   - Clean database
   - Run migrations
   - Verify schema
   - Test rollback

**Example Test**:
```python
async def test_basic_job_execution():
    """Test basic job creation and execution"""
    # Create job
    job = await create_job("test_job", "tests.fixtures.simple_task")

    # Test job (invokes worker execute flow synchronously)
    job.test()

    # Verify job completed
    assert job.status == JobStatus.COMPLETED

    # Verify task completed
    tasks = await get_job_tasks(job.id)
    assert len(tasks) == 1
    assert tasks[0].status == TaskStatus.COMPLETED

    # Verify log file created
    from aaiclick.orchestration.logging import get_logs_dir
    log_file = f"{get_logs_dir()}/{tasks[0].id}.log"
    assert os.path.exists(log_file)
    with open(log_file) as f:
        log_content = f.read()
        assert "expected output" in log_content
```

**Deliverables**:
- Basic tests passing
- Example from goal works
- Documentation for basic usage

---

## Out of Scope (Future Phases)

The following features from the full spec are **NOT** included in this basic implementation:

- **Distributed workers**: Multi-process/multi-node execution (Phase 6+)
- **Task claiming with locking**: `FOR UPDATE SKIP LOCKED` (Phase 6+)
- **Groups**: Task grouping and nested groups (Phase 7+)
- **Dependencies**: Task/Group dependencies with `>>` operators (Phase 7+)
- **Dynamic task creation**: Tasks creating new tasks via `map()` (Phase 8+)
- **Worker management**: Worker registration, heartbeat, monitoring (Phase 6+)
- **Retry logic**: Automatic task retry on failure (Phase 9+)
- **Advanced serialization**: View and Object parameter types (Phase 8+)

These will be added incrementally after the basic implementation is stable.

---

## Success Criteria

Phase 1-5 implementation is complete when:

1. ✅ User can define a simple Python function
2. ✅ User can create a job with `create_job(name, callback)`
3. ✅ User can test job synchronously with `job.test()` (invokes worker execute flow)
4. ✅ Task execution has access to Context (ClickHouse + PostgreSQL)
5. ✅ Job and task state persisted to PostgreSQL
6. ✅ Basic tests passing
7. ✅ Example from goal works end-to-end

## Next Steps After Phase 5

Once basic implementation is working:
- Phase 6: Distributed Workers (async task claiming)
- Phase 7: Groups and Dependencies (DAG support)
- Phase 8: Dynamic Task Creation (`map()` operator)
- Phase 9: Advanced Features (retry, monitoring, views)
