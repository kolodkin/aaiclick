# aaiclick Orchestration Implementation Plan

## Goal

Implement orchestration backend to support basic job creation and execution, starting with this simple example:

```python
def task1():
    a = 1
    b = 2
    c = a + b
    print(c)

def main():
    job = create_job("orch_basic_example", task1)
    job.run()

if __name__ == "__main__":
    main()
```

## Implementation Phases

### Phase 1: Database Setup

**Objective**: Set up PostgreSQL schema for basic job/task tracking

**Tasks**:
1. Add PostgreSQL dependencies to `pyproject.toml`:
   - `sqlmodel`
   - `asyncpg`
   - `alembic`
   - `psycopg2-binary` (for Alembic)

2. Create `aaiclick/orchestration/models.py`:
   - `JobStatus` enum (PENDING, RUNNING, COMPLETED, FAILED)
   - `TaskStatus` enum (PENDING, CLAIMED, RUNNING, COMPLETED, FAILED)
   - `WorkerStatus` enum (ACTIVE, IDLE, STOPPED)
   - `Job` model (minimal fields: id, name, status, created_at)
   - `Task` model (minimal fields: id, job_id, entrypoint, kwargs, status, result_table_id)
   - `Worker` model (minimal fields: id, hostname, pid, status)

3. Initialize Alembic:
   ```bash
   alembic init aaiclick/orchestration/migrations
   ```

4. Create initial migration:
   - Configure `alembic.ini` and `env.py` for SQLModel
   - Generate migration for Job, Task, Worker tables
   - Test migration up/down

5. Add environment variables to `CLAUDE.md`:
   ```bash
   POSTGRES_HOST=localhost
   POSTGRES_PORT=5432
   POSTGRES_USER=aaiclick
   POSTGRES_PASSWORD=secret
   POSTGRES_DB=aaiclick
   ```

**Deliverables**:
- `aaiclick/orchestration/models.py` with basic models
- `aaiclick/orchestration/migrations/` with Alembic setup
- Initial migration script
- Database can be created with `alembic upgrade head`

---

### Phase 2: Core Factories

**Objective**: Implement `create_task()` and `create_job()` factories

**Tasks**:
1. Create `aaiclick/orchestration/__init__.py`:
   - Export core functions and classes
   - Import from submodules

2. Create `aaiclick/orchestration/factories.py`:
   - Implement `create_task(callback: str, kwargs: dict = None) -> Task`
     - Accept callback as string (e.g., "mymodule.task1")
     - Create Task object (not committed to DB)
     - Default kwargs to empty dict

   - Implement `create_job(name: str, entry: Union[str, Task]) -> Job`
     - Accept callback string or Task object
     - Create Job and initial Task
     - Commit both to PostgreSQL
     - Return Job object

3. Create `aaiclick/orchestration/database.py`:
   - `get_postgres_engine()` - create async engine from env vars
   - `get_postgres_session()` - create async session
   - Connection pooling configuration

4. Update `create_job()` to commit to database:
   - Create async session
   - Insert Job record
   - Create Task from entry point
   - Set task.job_id
   - Insert Task record
   - Commit transaction
   - Return Job with id populated

**Example Usage**:
```python
from aaiclick.orchestration import create_job

# Using callback string
job = await create_job("my_job", "mymodule.task1")
print(f"Job {job.id} created")
```

**Deliverables**:
- `create_task()` factory working
- `create_job()` factory working
- Jobs and tasks persisted to PostgreSQL

---

### Phase 3: Job.run() Method

**Objective**: Implement synchronous job execution

**Tasks**:
1. Add `run()` method to `Job` model in `models.py`:
   ```python
   def run(self):
       """Execute job synchronously in current process"""
       import asyncio
       asyncio.run(self._run_async())
   ```

2. Implement `_run_async()` helper:
   - Create in-process worker
   - Claim and execute tasks in order
   - Update job status (PENDING → RUNNING → COMPLETED/FAILED)
   - Handle errors

3. Create `aaiclick/orchestration/execution.py`:
   - `execute_task(task: Task) -> Any`
     - Import callback function from entrypoint string
     - Deserialize kwargs (basic support for pyobj type)
     - Call function with kwargs
     - Return result

4. Implement task execution loop in `_run_async()`:
   - Query pending tasks for this job (ordered by created_at)
   - For each task:
     - Update status to RUNNING
     - Execute via `execute_task()`
     - Store result if applicable
     - Update status to COMPLETED
     - Handle failures (set status to FAILED, store error)

5. Add result handling:
   - If task returns an Object, store table_id in result_table_id
   - For simple values, serialize to JSONB or skip

**Example Flow**:
```python
job = create_job("example", "mymodule.task1")
job.run()  # Blocks until job completes
# Job status is now COMPLETED
```

**Deliverables**:
- `Job.run()` executes all tasks in job
- Task results captured
- Job status transitions work correctly
- Basic example from goal works end-to-end

---

### Phase 4: Context Integration

**Objective**: Make Context available during task execution

**Tasks**:
1. Update `aaiclick/context.py`:
   - Add `job_id` parameter to `Context.__init__()`
   - Add `_postgres_session` attribute
   - Initialize PostgreSQL session alongside ClickHouse client

2. Create context-local storage for current context:
   ```python
   # In aaiclick/orchestration/context.py
   from contextvars import ContextVar

   _current_context: ContextVar[Optional[Context]] = ContextVar('context', default=None)

   def get_current_context() -> Context:
       return _current_context.get()

   def set_current_context(ctx: Context):
       _current_context.set(ctx)
   ```

3. Update `execute_task()` to use Context:
   ```python
   async def execute_task(task: Task) -> Any:
       async with Context(job_id=task.job_id) as ctx:
           set_current_context(ctx)
           # Import and execute function
           func = import_callback(task.entrypoint)
           result = await func(**task.kwargs)
           return result
   ```

4. Add `apply()` method to Context:
   - Accept Task, Group, or list
   - Set job_id on all tasks
   - Insert into PostgreSQL
   - Return committed objects

**Deliverables**:
- Tasks execute with Context bound to job_id
- Context available via `get_current_context()`
- Tasks can access ClickHouse client via context
- `context.apply()` works for committing tasks

---

### Phase 5: Testing & Examples

**Objective**: Validate basic implementation with tests and examples

**Tasks**:
1. Create `tests/test_orchestration_basic.py`:
   - Test `create_task()` factory
   - Test `create_job()` factory
   - Test `job.run()` execution
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

    # Run job
    job.run()

    # Verify job completed
    assert job.status == JobStatus.COMPLETED

    # Verify task completed
    tasks = await get_job_tasks(job.id)
    assert len(tasks) == 1
    assert tasks[0].status == TaskStatus.COMPLETED
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
3. ✅ User can run job synchronously with `job.run()`
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
