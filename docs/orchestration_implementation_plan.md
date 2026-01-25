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
    job_test(job)

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

### Phase 3: job_test() Function ✅

**Objective**: Implement synchronous job testing (similar to Airflow)

**Note**: `job_test(job)` invokes the worker execute flow - it simulates a worker claiming and executing tasks, but runs synchronously in the current process for testing/debugging.

**Implementation**: See the following files for complete implementation:
- `aaiclick/orchestration/debug_execution.py` - `job_test()` and `ajob_test()` functions
- `aaiclick/orchestration/execution.py` - Task execution logic
- `aaiclick/orchestration/logging.py` - Task logging utilities

**Tasks**:
1. ✅ Implement `job_test()` function in `debug_execution.py`:
   - Standalone function (not a method on Job model to avoid coupling)
   - See: `aaiclick/orchestration/debug_execution.py` - `job_test()` function

2. ✅ Implement `ajob_test()` helper:
   - Creates OrchContext and calls `run_job_tasks()`
   - See: `aaiclick/orchestration/debug_execution.py` - `ajob_test()` function

3. ✅ Create `aaiclick/orchestration/execution.py`:
   - `import_callback(entrypoint: str)` - Import function from string
   - `deserialize_task_params(kwargs: dict)` - Deserialize task parameters
   - `execute_task(task: Task)` - Execute single task with logging
   - `run_job_tasks(job: Job)` - Execute all tasks in job
   - See: `aaiclick/orchestration/execution.py`

4. ✅ Implement task logging:
   - `aaiclick/orchestration/logging.py`:
     - `get_logs_dir()` - Returns OS-dependent log directory
     - `capture_task_output(task_id: int)` - Context manager for stdout/stderr capture
   - See: `aaiclick/orchestration/logging.py`

5. ✅ Implement task execution loop in `run_job_tasks()`:
   - Queries pending tasks, executes in order
   - Updates status through RUNNING → COMPLETED/FAILED
   - See: `aaiclick/orchestration/execution.py:82-165`

6. ✅ Add result handling:
   - Converts task return values to Objects via `create_object_from_value()` (data stored in ClickHouse)
   - Stores JSON reference in `task.result` with `object_type` and `table_id` pointing to the Object
   - See: `aaiclick/orchestration/execution.py:166-171`

**Deliverables**:
- ✅ `test_job()` executes all tasks in job using worker execute flow
- ✅ Task stdout/stderr captured to `{AAICLICK_LOG_DIR}/{task_id}.log`
- ✅ Task results captured as Objects via `create_object_from_value()`
- ✅ Job status transitions work correctly
- ✅ Basic example from goal works end-to-end
- ✅ Example: `aaiclick/examples/orchestration_basic.py`
- ✅ Tests: `aaiclick/orchestration/test_orchestration_execution.py`

---

### Phase 4: OrchContext Integration ✅

**Objective**: Create OrchContext for orchestration and make both contexts available during task execution

**Implementation**: See the following files for complete implementation:
- `aaiclick/orchestration/context.py` - OrchContext class and apply() method
- `aaiclick/orchestration/execution.py:85-118` - execute_task() with DataContext

**Tasks**:
1. ✅ Created `aaiclick/orchestration/context.py`:
   - Define `OrchContext` class
   - Signature: `def __init__(self)`
   - Creates SQLAlchemy AsyncEngine in `__aenter__` (per-context)
   - Disposes engine in `__aexit__` (proper async cleanup)
   - Each operation creates AsyncSession from context's engine
   - Implements context manager protocol (`__aenter__`, `__aexit__`)
   - **Implementation**: `aaiclick/orchestration/context.py` - see `OrchContext` class

2. ✅ Context-local storage for OrchContext:
   - **Implementation**: `aaiclick/orchestration/context.py` - see `_current_orch_context` and `get_orch_context()`

3. ✅ Update `execute_task()` to use both contexts:
   - Tasks execute within DataContext for ClickHouse operations
   - OrchContext is available from the outer context (run_job_tasks)
   - **Implementation**: `aaiclick/orchestration/execution.py` - see `execute_task()` function

4. ✅ Add `apply()` method to OrchContext:
   - Accepts Task, Group, or list with job_id parameter
   - Generates snowflake IDs for Groups using `get_snowflake_id()` (if not already set)
   - Sets job_id on all tasks and groups
   - Inserts into PostgreSQL using ORM (session.add, session.commit)
   - Returns committed objects
   - **Implementation**: `aaiclick/orchestration/context.py` - see `OrchContext.apply()` method

**Deliverables**:
- ✅ Per-context SQLAlchemy AsyncEngine (created on enter, disposed on exit)
- ✅ OrchContext class (no job_id parameter - simplified)
- ✅ Tasks execute with both DataContext (data) and OrchContext (orchestration)
- ✅ OrchContext available via `get_orch_context()`
- ✅ DataContext remains unchanged (backward compatible)
- ✅ `orch_ctx.apply()` works for committing tasks
- ✅ Each operation creates its own AsyncSession from context's engine
- ✅ Proper async lifecycle ensures test isolation without cleanup fixtures

---

### Phase 5: Testing & Examples ✅

**Objective**: Validate basic implementation with tests and examples

**Implementation**: See the following files for complete test coverage and examples:
- `aaiclick/orchestration/test_orchestration_factories.py` - Tests for `create_task()` and `create_job()`
- `aaiclick/orchestration/test_orchestration_execution.py` - Tests for task execution, logging, and job_test
- `aaiclick/examples/orchestration_basic.py` - Complete working example

**Tasks**:
1. ✅ Create tests for orchestration:
   - `aaiclick/orchestration/test_orchestration_factories.py`:
     - Test `create_task()` factory (basic, with kwargs, unique IDs)
     - Test `create_job()` factory (with string, with Task object)
     - Test job/task relationship and database persistence
   - `aaiclick/orchestration/test_orchestration_execution.py`:
     - Test `import_callback()` for sync and async functions
     - Test `deserialize_task_params()` validation
     - Test `execute_task()` for sync and async tasks
     - Test `run_job_tasks()` for success and failure cases
     - Test task logging (stdout/stderr capture)
     - Test `job_test()`/`ajob_test()` execution

2. ✅ Create `aaiclick/examples/orchestration_basic.py`:
   - Simple arithmetic task (from goal) - `simple_arithmetic()`
   - Task with parameters - `task_with_params(x, y)`
   - Example job execution via `ajob_test()`
   - Full working example with OrchContext

3. ✅ Documentation in specification files:
   - `docs/orchestration.md` - Full specification with usage examples
   - `docs/orchestration_implementation_plan.md` - Phase-by-phase progress
   - Environment setup in `CLAUDE.md`

4. ✅ Database migrations tested:
   - CI/CD workflow runs migrations before tests (`.github/workflows/test.yaml`)
   - All tests run with fresh database on each CI run
   - Migration CLI available: `python -m aaiclick migrate`

**Deliverables**:
- ✅ All tests passing (factory, execution, logging)
- ✅ Example from goal works end-to-end
- ✅ Documentation in specification files

---

### Phase 6: Distributed Workers ✅

**Objective**: Enable multi-process/multi-node task execution with atomic task claiming

**Implementation**: See the following files for complete implementation:
- `aaiclick/orchestration/worker.py` - Worker lifecycle and main loop
- `aaiclick/orchestration/claiming.py` - Atomic task claiming with FOR UPDATE SKIP LOCKED
- `aaiclick/__main__.py` - CLI commands for worker start/list
- `aaiclick/orchestration/test_worker.py` - Comprehensive tests
- `aaiclick/orchestration/migrations/versions/d7f7e092e80c_add_worker_stats_fields.py` - Worker stats migration

**Tasks**:
1. ✅ Worker management functions in `aaiclick/orchestration/worker.py`:
   - `register_worker()` - Register new worker with hostname/pid
   - `worker_heartbeat(worker_id)` - Update last_heartbeat timestamp
   - `deregister_worker(worker_id)` - Mark worker as STOPPED
   - `list_workers(status)` - List workers by status
   - `get_worker(worker_id)` - Get worker by ID

2. ✅ Atomic task claiming in `aaiclick/orchestration/claiming.py`:
   - `claim_next_task(worker_id)` - Claim using `FOR UPDATE SKIP LOCKED`
   - Prioritizes running jobs over pending (`ORDER BY started_at ASC NULLS LAST`)
   - Atomically updates job status to RUNNING on first claim
   - `update_task_status()` and `update_job_status()` helpers

3. ✅ Worker main loop in `aaiclick/orchestration/worker.py`:
   - `worker_main_loop()` - Main execution loop with graceful shutdown
   - Optional signal handlers (SIGTERM/SIGINT)
   - Heartbeat updates during execution
   - `max_tasks` and `max_empty_polls` for testing

4. ✅ Worker CLI commands in `aaiclick/__main__.py`:
   - `python -m aaiclick worker start [--max-tasks N]`
   - `python -m aaiclick worker list`

5. ✅ Tests in `aaiclick/orchestration/test_worker.py`:
   - Worker registration/deregistration
   - Heartbeat updates
   - Atomic task claiming (concurrent workers)
   - Worker main loop execution and failure handling

**Deliverables**:
- ✅ Worker registration and lifecycle management
- ✅ Atomic task claiming with `FOR UPDATE SKIP LOCKED`
- ✅ Worker main loop for background task execution
- ✅ CLI commands for worker management
- ✅ Tests for concurrent task claiming

---

### Phase 7: Groups and Dependencies ✅

**Objective**: Enable DAG-style workflow definitions with dependency operators

**Implementation**: See the following files for complete implementation:
- `aaiclick/orchestration/models.py` - Task and Group dependency operators (`>>`, `<<`, `__rrshift__`, `__rlshift__`)
- `aaiclick/orchestration/context.py` - Updated `apply()` to save dependencies
- `aaiclick/orchestration/claiming.py` - Dependency-aware task claiming
- `aaiclick/orchestration/test_dependencies.py` - Comprehensive tests

**Tasks**:
1. ✅ Add dependency operators to Task and Group models:
   - `depends_on()` method to declare dependencies
   - `__rshift__` (A >> B: B depends on A)
   - `__lshift__` (A << B: A depends on B)
   - `__rrshift__` for fan-in: [A, B] >> C
   - `__rlshift__` for fan-out: [A, B] << C

2. ✅ Update `apply()` to save dependencies:
   - Collects `_pending_dependencies` from all items
   - Commits Dependency records to database

3. ✅ Update `claim_next_task()` with dependency checking:
   - Task → Task: Task waits for previous task to complete
   - Group → Task: Task waits for all tasks in previous group to complete
   - Task → Group: Tasks in group wait for previous task to complete
   - Group → Group: Tasks in group wait for all tasks in previous group to complete

4. ✅ Tests for dependency operators and claiming:
   - `test_task_rshift_creates_dependency`
   - `test_task_lshift_creates_dependency`
   - `test_task_chained_rshift`
   - `test_task_fanout`
   - `test_task_fanin`
   - `test_group_rshift_creates_dependency`
   - `test_task_rshift_to_group`
   - `test_group_to_group_dependency`
   - `test_apply_saves_dependencies`
   - `test_claim_respects_task_dependency`
   - `test_claim_respects_group_dependency`

**Deliverables**:
- ✅ Airflow-like `>>` and `<<` operators for defining dependencies
- ✅ Support for Task and Group dependencies (all 4 combinations)
- ✅ Fan-in and fan-out patterns
- ✅ Dependency-aware task claiming
- ✅ Comprehensive tests

---

## Out of Scope (Future Phases)

The following features are **NOT** included in Phase 7:

- **Dynamic task creation**: Tasks creating new tasks via `map()` (Phase 8+)
- **Retry logic**: Automatic task retry on failure (Phase 9+)
- **Advanced serialization**: View and Object parameter types (Phase 8+)
- **Orphan task recovery**: Reclaiming tasks from dead workers (Phase 9+)

These will be added incrementally after Phase 7 is stable.

---

## Success Criteria

Phase 1-5 implementation is complete when:

1. ✅ User can define a simple Python function
2. ✅ User can create a job with `create_job(name, callback)`
3. ✅ User can test job synchronously with `job_test(job)` (invokes worker execute flow)
4. ✅ Task execution has access to Context (ClickHouse + PostgreSQL)
5. ✅ Job and task state persisted to PostgreSQL
6. ✅ Basic tests passing
7. ✅ Example from goal works end-to-end

## Next Steps After Phase 7

- Phase 8: Dynamic Task Creation (`map()` operator)
- Phase 9: Advanced Features (retry, monitoring, views)
