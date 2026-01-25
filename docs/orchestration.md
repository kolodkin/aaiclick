# aaiclick Orchestration Backend Specification

## Overview

The aaiclick orchestration backend enables distributed execution of data processing workflows across multiple workers. It manages job scheduling, task distribution, and execution coordination using PostgreSQL as the state store.

### Motivation

As aaiclick scales to handle large-scale data processing, we need:
- **Distributed execution**: Parallelize work across multiple workers
- **Dynamic task generation**: Create new tasks during execution (e.g., via `map()` operations)
- **Reliable state management**: Track job progress with crash recovery
- **Ordered execution**: Preserve temporal causality via creation timestamps

## Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                       Global Resources                           │
│  ┌─────────────────────┐                                         │
│  │  ClickHouse Pool    │                                         │
│  │  (urllib3 Pool)     │                                         │
│  └─────────────────────┘                                         │
└──────────────────────────────────────────────────────────────────┘
           │
           │
           ▼
┌──────────────────────┐           ┌──────────────────────────┐
│   DataContext        │           │    OrchContext           │
│  (ClickHouse data)   │           │  (Orchestration state)   │
│  ┌────────────────┐  │           │  ┌────────────────────┐ │
│  │ ClickHouse     │  │           │  │ AsyncEngine        │ │
│  │ Client         │  │           │  │ (per-context)      │ │
│  └────────────────┘  │           │  └────────────────────┘ │
│                      │           │  Creates/disposes on   │
│                      │           │  enter/exit            │
└──────────────────────┘           └──────────────────────────┘
           │                                      │
           │ Objects/Views                        │ Jobs/Tasks/Groups
           ▼                                      ▼
┌────────────────────┐           ┌──────────────────────────────┐
│   ClickHouse DB    │           │      PostgreSQL Database     │
│   (Object data)    │           │  ┌────┐ ┌──────┐ ┌────────┐ │
└────────────────────┘           │  │Jobs│─│Tasks │ │Workers │ │
                                 │  └────┘ └──────┘ └────────┘ │
                                 │  ┌──────┐ ┌──────────────┐  │
                                 │  │Groups│ │Dependencies  │  │
                                 │  └──────┘ └──────────────┘  │
                                 └──────────────────────────────┘
                                             ▲
                                             │ Polls and executes
                                             │
                                 ┌───────────┴────────────────┐
                                 │ Worker Pool (N processes)  │
                                 │ (uses both contexts)       │
                                 └────────────────────────────┘
```

**Dual-Context Architecture**:
- **DataContext** (`aaiclick.data_context`): Manages ClickHouse data operations
  - Handles Objects and Views
  - Uses global urllib3 connection pool
  - Example: `async with DataContext() as data_ctx:`

- **OrchContext** (`aaiclick.orchestration.context`): Manages PostgreSQL orchestration state
  - Handles Jobs, Tasks, Groups, Dependencies
  - Creates fresh SQLAlchemy AsyncEngine on `__aenter__`, disposes on `__aexit__`
  - Ensures proper async lifecycle and test isolation
  - Each operation creates its own AsyncSession for transaction isolation
  - Example: `async with OrchContext() as orch_ctx:`

- **Task Execution**: Workers use **both** contexts
  - DataContext for data operations (Objects)
  - OrchContext for orchestration operations (apply, etc.)
  - Example:
    ```python
    async with DataContext() as data_ctx:
        async with OrchContext(job_id=task.job_id) as orch_ctx:
            # Task has access to both contexts
            result = await func(**task.kwargs)
    ```

**Worker Architecture**: Each worker is a single process that can execute multiple tasks concurrently using async/await. This allows efficient utilization of I/O-bound operations (database queries, network calls) without blocking.

## Technology Stack

- **SQLModel**: Type-safe ORM with Pydantic integration
- **Alembic**: Database migrations for PostgreSQL
- **PostgreSQL**: Orchestration state store (jobs, tasks, groups, dependencies)
- **ClickHouse**: Data storage (Objects and Views)
- **asyncpg** (via SQLModel): Async PostgreSQL driver

### Why Dual-Database Architecture?

**PostgreSQL for Orchestration State:**
- **ACID compliance**: Strong consistency guarantees for job state
- **Row-level locking**: Safe concurrent task claiming by workers (`FOR UPDATE SKIP LOCKED`)
- **JSONB support**: Flexible task parameter storage
- **Mature ecosystem**: Alembic migrations, connection pooling, monitoring
- **Relational integrity**: Foreign keys for job→task→group relationships

**ClickHouse for Data:**
- **Columnar storage**: Efficient for analytical workloads
- **High performance**: Fast aggregations and scans
- **Scalability**: Handles massive datasets
- **aaiclick Objects**: All data processing operates on ClickHouse tables

**Separation of Concerns**:
- **DataContext** is independent - handles only ClickHouse operations
- **OrchContext** is independent - handles only PostgreSQL orchestration
- Both can be used together or separately

**OrchContext Connection Management**:
- **Global SQLAlchemy AsyncEngine**: Shared across all OrchContext instances
  - Engine manages connection pooling internally via asyncpg
  - No separate pool management needed
- OrchContext creates AsyncSessions from the global engine for each operation
- Each operation (`apply()`, `claim_task()`, etc.) uses its own session
- Benefits:
  - Connection pooling handled automatically by SQLAlchemy
  - Better transaction isolation per operation
  - No long-lived transactions
  - Engine lifecycle managed globally (similar to ClickHouse urllib3 pool pattern)

**High-Level Factory APIs**:
- **`create_task(callback, kwargs)`**: Factory for creating Task objects from callback strings
  - Generates snowflake ID for task
  - **Implementation**: `aaiclick/orchestration/factories.py` - see `create_task()` function
- **`create_job(name, entry)`**: Factory for creating Job with single entry point (Task or callback)
  - Generates snowflake ID for job
  - Commits Job and Task to PostgreSQL with JSON-serialized kwargs
  - **Implementation**: `aaiclick/orchestration/factories.py` - see `create_job()` function
- **`orch_ctx.apply(tasks, job_id)`**: Commits DAG (tasks, groups, dependencies) to PostgreSQL
  - Generates snowflake IDs for groups if not already set
  - Sets job_id on all items
  - **Implementation**: `aaiclick/orchestration/context.py` - see `OrchContext.apply()` method
- Factories provide simple interface for common workflows
- IDs generated before database insertion (no round-trip needed)

## Data Models

### ID Generation Strategy

**All entities use Snowflake IDs** generated by the aaiclick framework (not database auto-increment):

- **Job IDs**: Framework-generated snowflake IDs
- **Task IDs**: Framework-generated snowflake IDs
- **Group IDs**: Framework-generated snowflake IDs

**Why Snowflake IDs?**
- **Distributed generation**: IDs can be generated independently across processes without coordination
- **Time-ordered**: IDs encode creation timestamp for temporal ordering
- **No database round-trip**: IDs assigned before database insert
- **Consistency with aaiclick Objects**: Same ID strategy as ClickHouse data tables

**Database Storage**:
- PostgreSQL type: `BIGINT` (signed int64, range: -2^63 to 2^63-1)
- Python/SQLModel type: `int` (unbounded)
- **Important**: PostgreSQL has no native unsigned uint64 type
- **Solution**: Use standard 64-bit Snowflake IDs (bit 63 always 0)
- Snowflake IDs are always positive (max value: 2^63-1 = 9,223,372,036,854,775,807)
- Database enforces uniqueness constraints via primary keys but does not generate IDs

**Snowflake ID Format** (from `aaiclick.snowflake`):
- Bit 63: Sign bit (always 0 for positive integers)
- Bits 62-22: Timestamp (41 bits, milliseconds since epoch)
- Bits 21-12: Machine ID (10 bits, supports 1024 machines)
- Bits 11-0: Sequence (12 bits, 4096 IDs per millisecond)
- Same implementation as ClickHouse Object IDs
- Existing implementation is fully compatible with PostgreSQL BIGINT

**SQLModel Field Definition**:
```python
from sqlmodel import Field, Column
from sqlalchemy import BigInteger

# Correct: ID generated by framework, stored as BIGINT (signed int64)
id: int = Field(sa_column=Column(BigInteger, primary_key=True, autoincrement=False))

# Simplified (SQLModel infers BigInteger from int type):
id: int = Field(primary_key=True)

# Wrong: Optional with default=None triggers auto-increment
id: Optional[int] = Field(default=None, primary_key=True)
```

### Status Enums

**Implementation**: `aaiclick/orchestration/models.py` - see `JobStatus`, `TaskStatus`, `WorkerStatus` enums

**Note**: Actual implementation uses UPPERCASE enum values to match PostgreSQL enum types:

```python
from enum import StrEnum

class JobStatus(StrEnum):
    PENDING = "PENDING"      # PostgreSQL enums are case-sensitive
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"

class TaskStatus(StrEnum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"

class WorkerStatus(StrEnum):
    ACTIVE = "ACTIVE"
    IDLE = "IDLE"
    STOPPED = "STOPPED"
```

### Job

Represents a workflow containing one or more tasks.

**Implementation**: `aaiclick/orchestration/models.py` - see `Job` class

```python
from datetime import datetime
from typing import Optional
from sqlmodel import Field, SQLModel, Column
from sqlalchemy import BigInteger

class Job(SQLModel, table=True):
    __tablename__ = "jobs"

    id: int = Field(sa_column=Column(BigInteger, primary_key=True))
    # uint64 snowflake ID generated by framework (not database auto-increment)
    # Stored as BIGINT (signed int64) in PostgreSQL

    # Job metadata
    name: str = Field(index=True)

    # Status tracking
    status: JobStatus = Field(default=JobStatus.PENDING, index=True)

    # Timestamps
    created_at: datetime = Field(default_factory=datetime.utcnow, index=True)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None

    # Error tracking
    error: Optional[str] = None
```

**Job Status Lifecycle:**
```
PENDING → RUNNING → COMPLETED
                  → FAILED
                  → CANCELLED
```

### Group

Represents a logical grouping of tasks and other groups within a job. Groups can be nested.

**Implementation**: `aaiclick/orchestration/models.py` - see `Group` class

```python
from datetime import datetime
from typing import Optional
from sqlmodel import Field, SQLModel, Column
from sqlalchemy import BigInteger, ForeignKey

class Group(SQLModel, table=True):
    __tablename__ = "groups"

    id: int = Field(sa_column=Column(BigInteger, primary_key=True))
    # uint64 snowflake ID generated by framework
    # Stored as BIGINT (signed int64) in PostgreSQL

    job_id: int = Field(sa_column=Column(BigInteger, ForeignKey("jobs.id"), index=True))
    parent_group_id: Optional[int] = Field(
        default=None,
        sa_column=Column(BigInteger, ForeignKey("groups.id"), index=True, nullable=True)
    )

    # Group metadata
    name: str = Field(index=True)

    # Timestamps
    created_at: datetime = Field(default_factory=datetime.utcnow, index=True)
```

**Nested Group Support:**
- Groups can contain tasks via `group_id` foreign key in Task
- Groups can contain other groups via `parent_group_id` foreign key
- Enables hierarchical workflow organization
- Dependencies can be defined at any level of the hierarchy

### Dependency

Unified dependency table supporting all dependency types between tasks and groups.

**Implementation**: `aaiclick/orchestration/models.py` - see `Dependency` class

```python
from sqlmodel import Field, SQLModel, Column
from sqlalchemy import BigInteger

class Dependency(SQLModel, table=True):
    __tablename__ = "dependencies"

    # Entity that must complete first
    previous_id: int = Field(sa_column=Column(BigInteger, primary_key=True, index=True))
    previous_type: str = Field(primary_key=True)  # 'task' or 'group'

    # Entity that waits (executes after previous completes)
    next_id: int = Field(sa_column=Column(BigInteger, primary_key=True, index=True))
    next_type: str = Field(primary_key=True)  # 'task' or 'group'
```

**Supported Dependency Types:**
- Task → Task: `previous_type='task'`, `next_type='task'`
- Task → Group: `previous_type='task'`, `next_type='group'` (all tasks in next group wait for previous task)
- Group → Task: `previous_type='group'`, `next_type='task'` (next task waits for all tasks in previous group)
- Group → Group: `previous_type='group'`, `next_type='group'` (next group waits for all tasks in previous group)

**Benefits:**
- Single unified table for all dependency relationships
- Intuitive previous/next naming for workflow dependencies
- Flexible schema supporting future entity types
- Simplified querying across all dependency types
- Validation enforced at ORM/application level

### Task

Represents a single executable unit of work within a job.

**Implementation**: `aaiclick/orchestration/models.py` - see `Task` class

```python
from datetime import datetime
from typing import Optional, Dict, Any
from sqlmodel import Field, SQLModel, Column
from sqlalchemy import BigInteger, ForeignKey, JSON

class Task(SQLModel, table=True):
    __tablename__ = "tasks"

    id: int = Field(sa_column=Column(BigInteger, primary_key=True))
    # uint64 snowflake ID generated by framework
    # Stored as BIGINT (signed int64) in PostgreSQL

    job_id: int = Field(sa_column=Column(BigInteger, ForeignKey("jobs.id"), index=True))
    group_id: Optional[int] = Field(
        default=None,
        sa_column=Column(BigInteger, ForeignKey("groups.id"), index=True, nullable=True)
    )

    # Execution specification
    entrypoint: str = Field()
    # Format: "module.submodule.function" (importable callable)
    # Example: "aaiclick.operators.map_function"

    kwargs: Dict[str, Any] = Field(default_factory=dict, sa_column=Column(JSON))
    # Dictionary mapping parameter names to serialized Object/View references
    # All parameters must be aaiclick Objects or Views (no native Python values)
    # See "Task Parameter Serialization" section for format

    # Status tracking
    status: TaskStatus = Field(default=TaskStatus.PENDING, index=True)

    # Result (JSON - same format as kwargs)
    result: Optional[Dict[str, Any]] = Field(default=None, sa_column=Column(JSON, nullable=True))
    # Serialized Object or View reference (see "Task Return Values" section)
    # null if task returns None

    # Logging
    log_path: Optional[str] = None
    # Path to task log file: {get_logs_dir()}/{task_id}.log
    # Captures stdout and stderr during task execution
    # OS-dependent defaults: ~/.aaiclick/logs (macOS), /var/log/aaiclick (Linux)

    # Error tracking
    error: Optional[str] = None

    # Worker assignment
    worker_id: Optional[int] = Field(
        default=None,
        sa_column=Column(BigInteger, ForeignKey("workers.id"), index=True, nullable=True)
    )
    # Identifier of the worker executing this task

    # Timestamps
    created_at: datetime = Field(default_factory=datetime.utcnow, index=True)
    claimed_at: Optional[datetime] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
```

**Task Dependencies and Groups**:
- Each task belongs to one Group (optional, via group_id foreign key)
- Groups can be nested (group contains other groups via parent_group_id)
- All dependencies (task → task, task → group, group → task, group → group) managed via unified Dependency table
- A task can only be claimed if all its direct dependencies are satisfied (completed tasks/groups)

**Task Status Lifecycle:**
```
PENDING → CLAIMED → RUNNING → COMPLETED
                            → FAILED → PENDING (if retries remain)
                                    → FAILED (max retries exceeded)
        → CANCELLED
```

**Dependency Constraints**:
- A task can only be claimed if all its `previous` dependencies (tasks/groups) have status COMPLETED
- Circular dependencies are not allowed
- Dependencies managed via unified Dependency table with previous/next relationships

### Worker

Represents an active worker process.

**Implementation**: `aaiclick/orchestration/models.py` - see `Worker` class

```python
from datetime import datetime
from typing import Optional
from sqlmodel import Field, SQLModel, Column
from sqlalchemy import BigInteger

class Worker(SQLModel, table=True):
    __tablename__ = "workers"

    id: int = Field(sa_column=Column(BigInteger, primary_key=True))
    # uint64 snowflake ID generated by framework
    # Stored as BIGINT (signed int64) in PostgreSQL

    # Worker metadata
    hostname: str = Field(index=True)
    pid: int

    # Status
    status: WorkerStatus = Field(default=WorkerStatus.ACTIVE, index=True)

    # Heartbeat
    last_heartbeat: datetime = Field(default_factory=datetime.utcnow, index=True)

    # Statistics
    tasks_completed: int = Field(default=0)
    tasks_failed: int = Field(default=0)

    # Timestamps
    started_at: datetime = Field(default_factory=datetime.utcnow)
```

## Task Parameter Serialization

Task kwargs are stored as JSONB. All parameters must be aaiclick Objects or Views - native Python values are not supported. This ensures type safety and enables distributed processing where data remains in ClickHouse.

### Object Parameters

Reference to a full aaiclick Object (entire ClickHouse table):

```json
{
    "object_type": "object",
    "table_id": "t123456789"
}
```

Worker deserializes to an `Object` instance with the specified table.

### View Parameters

Reference to a subset/view of an Object with query constraints:

```json
{
    "object_type": "view",
    "table_id": "t123456789",
    "offset": 0,
    "limit": 10000,
    "where": "value > 100"
}
```

Worker deserializes to a `View` instance. All constraint fields are optional except `table_id`. Default ordering is `aai_id ASC`.

### Example Task Kwargs

```python
# Task with Object and View parameters
task_kwargs = {
    "input_data": {
        "object_type": "view",
        "table_id": "t987654321",
        "offset": 10000,
        "limit": 10000
    },
    "reference_table": {
        "object_type": "object",
        "table_id": "t111222333"
    }
}
```

## Task Return Values

Task functions can return any value. The execution flow automatically converts return values to aaiclick Objects:

- **`None`**: Task produces no output data (`task.result` is `null`)
- **Any other value**: Automatically converted to Object via `create_object_from_value()`

The return value is serialized to JSON in `Task.result`:

```json
// Object return value (auto-converted from any Python value)
{
    "object_type": "object",
    "table_id": "t123456789"
}

// None return value
null
```

```python
# Task that returns a computed value (auto-converted to Object)
async def compute_sum(data: Object):
    values = await data.data()
    total = sum(values)
    return total  # Auto-converted via create_object_from_value(total)

# Task that returns a list (auto-converted to Object)
async def process_data(data: Object):
    results = [x * 2 for x in await data.data()]
    return results  # Auto-converted via create_object_from_value(results)

# Task that returns None (side-effect only)
async def log_summary(data: Object):
    count = await data.count()
    print(f"Processed {count} rows")
    # task.result is null
```

**Note**: If a task already returns an Object, it is stored directly without re-conversion.

## Task Execution Flow

### 1. Job Creation ✅ IMPLEMENTED

**See**: Factory APIs section above for `create_job()` and `create_task()` usage
**Implementation**: `aaiclick/orchestration/factories.py` - see `create_job()` function

### 1.1 Job Testing ✅ IMPLEMENTED

Execute a job synchronously for testing/debugging:

```python
from aaiclick.orchestration import create_job, run_job_test

job = await create_job("my_job", "mymodule.task1")
run_job_test(job)  # Blocks until job completes
# Job status is now COMPLETED or FAILED
```

**Implementation**: `aaiclick/orchestration/debug_execution.py` - see `run_job_test()` function
- `aaiclick/orchestration/execution.py` - Task execution logic
- `aaiclick/orchestration/logging.py` - Task logging utilities

### 2. Dynamic Task Creation ⚠️ NOT YET IMPLEMENTED (Phase 8+)

Tasks will be able to create additional tasks during execution via aaiclick operators:

```python
# In aaiclick/operators.py
async def map(callback: str, obj: Object) -> Object:
    """
    Apply callback to each element of obj in parallel.
    Creates one task per chunk of data using offset/limit.
    """
    from aaiclick.orchestration import create_task, get_current_context

    # Get current context (has access to job_id and PostgreSQL session)
    ctx = get_current_context()

    # Get total row count without reading data
    total_rows = await obj.count()
    chunk_size = 10000  # Configurable chunk size

    # Create task for each chunk using create_task factory
    tasks = []
    for offset in range(0, total_rows, chunk_size):
        task = create_task(
            callback=callback,
            kwargs={
                "chunk": {
                    "object_type": "view",
                    "table_id": obj.table_id,
                    "offset": offset,
                    "limit": chunk_size
                }
            }
        )
        tasks.append(task)

    # Commit all tasks to database (context automatically sets job_id)
    await ctx.apply(tasks)

    # Return handle to future results
    num_chunks = (total_rows + chunk_size - 1) // chunk_size
    return await create_result_collector(ctx.job_id, num_chunks)
```

### 3. Worker Task Execution Loop ⚠️ NOT YET IMPLEMENTED (Phase 6+)

The following describes planned worker functionality:

```python
# Worker main loop (planned for Phase 6+)
async def worker_main_loop(worker_id: str):
    while True:
        # Claim next available task (atomic operation)
        # Prioritizes tasks from oldest running jobs
        task = await claim_next_task(worker_id)

        if task is None:
            await asyncio.sleep(1)  # No tasks available
            continue

        try:
            # Update task status
            await update_task_status(task.id, "running")

            # Set up task logging
            log_path = f"{get_logs_dir()}/{task.id}.log"
            await update_task_log_path(task.id, log_path)

            # Execute task with Context bound to job
            async with Context(job_id=task.job_id) as ctx:
                # Capture stdout/stderr to log file
                with capture_task_output(task.id):
                    # Import and execute entrypoint
                    func = import_function(task.entrypoint)

                    # Deserialize task kwargs (see Task Parameter Serialization section)
                    # Converts object_type formats to Object/View/native Python instances
                    task_kwargs = deserialize_task_params(task.kwargs, ctx)

                    # All print() and errors write to {get_logs_dir()}/{task.id}.log
                    result_obj = await func(**task_kwargs)

            # Store result
            await update_task_result(
                task.id,
                result_table_id=result_obj.table_id,
                status=TaskStatus.COMPLETED
            )

        except Exception as e:
            # Handle failure (error also logged to task log file)
            await handle_task_failure(task, error=str(e))
```

### 4. Task Claiming (Atomic)

Uses PostgreSQL row-level locking for safe concurrent access:

```sql
-- Implemented via SQLModel/SQLAlchemy
-- Prioritizes tasks from oldest running jobs first
-- Respects all dependency types via unified dependency table
-- Also marks job as started and running when first task is claimed
WITH claimed_task AS (
    UPDATE tasks
    SET
        status = 'claimed',
        worker_id = :worker_id,
        claimed_at = NOW()
    WHERE id = (
        SELECT t.id FROM tasks t
        JOIN jobs j ON t.job_id = j.id
        WHERE t.status = 'pending'
        -- Check previous task → next task dependencies
        AND NOT EXISTS (
            SELECT 1 FROM dependencies d
            JOIN tasks prev ON d.previous_id = prev.id
            WHERE d.next_id = t.id
            AND d.next_type = 'task'
            AND d.previous_type = 'task'
            AND prev.status != 'completed'
        )
        -- Check previous group → next task dependencies (all tasks in previous group must be completed)
        AND NOT EXISTS (
            SELECT 1 FROM dependencies d
            JOIN tasks prev ON d.previous_id = prev.group_id
            WHERE d.next_id = t.id
            AND d.next_type = 'task'
            AND d.previous_type = 'group'
            AND prev.status != 'completed'
        )
        -- Check previous task → next group dependencies (if task is in next group that waits for previous task)
        AND NOT EXISTS (
            SELECT 1 FROM dependencies d
            JOIN tasks prev ON d.previous_id = prev.id
            WHERE d.next_id = t.group_id
            AND d.next_type = 'group'
            AND d.previous_type = 'task'
            AND prev.status != 'completed'
            AND t.group_id IS NOT NULL
        )
        -- Check previous group → next group dependencies (if task is in next group that waits for previous group)
        AND NOT EXISTS (
            SELECT 1 FROM dependencies d
            JOIN tasks prev ON d.previous_id = prev.group_id
            WHERE d.next_id = t.group_id
            AND d.next_type = 'group'
            AND d.previous_type = 'group'
            AND prev.status != 'completed'
            AND t.group_id IS NOT NULL
        )
        ORDER BY j.started_at ASC
        LIMIT 1
        FOR UPDATE SKIP LOCKED
    )
    RETURNING *
)
UPDATE jobs
SET
    started_at = COALESCE(started_at, NOW()),
    status = CASE WHEN started_at IS NULL THEN 'running' ELSE status END
WHERE id = (SELECT job_id FROM claimed_task)
RETURNING (SELECT * FROM claimed_task);
```

**Key features:**
- `FOR UPDATE SKIP LOCKED`: Skip rows locked by other workers
- `NOT EXISTS`: Only claim tasks where all previous dependencies (tasks/groups) are completed
- `ORDER BY j.started_at ASC`: Prioritize tasks from oldest running jobs
- `COALESCE(started_at, NOW())`: Atomically set job's started_at when first task is claimed
- `CASE WHEN started_at IS NULL`: Set job status to 'running' on first task claim
- Atomic update: Prevents race conditions
- Unified dependency checking: Single table handles all dependency types (task→task, task→group, group→task, group→group)

## API / Interfaces

### Job Management

**Implemented (Phase 2):**
- ✅ `create_task()` - See Factory APIs section
- ✅ `create_job()` - See Factory APIs section
- **Implementation**: `aaiclick/orchestration/factories.py`

**Implemented (Phase 3):**
- ✅ `run_job_test(job)` - Execute job synchronously for testing
  - **Implementation**: `aaiclick/orchestration/debug_execution.py` - see `run_job_test()` function

**Not Yet Implemented (Phase 4+):**
- ⚠️ `get_job(job_id)` - Get job status and details
- ⚠️ `list_jobs(status)` - List jobs by status
- ⚠️ `cancel_job(job_id)` - Cancel a running job

### Task Management ⚠️ NOT YET IMPLEMENTED (Phase 4+)

```python
from aaiclick.orchestration import (
    add_task_to_job,
    get_task,
    retry_failed_tasks
)

# Add task to existing job
await add_task_to_job(
    job_id=job.id,
    entrypoint="myapp.process",
    kwargs={"data": "tbl_xyz"}
)

# Get task details
task = await get_task(task_id)

# Retry failed tasks
await retry_failed_tasks(job_id)
```

### Context API for DAG Construction ✅ IMPLEMENTED (Phase 4)

**Implementation**: `aaiclick/orchestration/context.py` - see `OrchContext.apply()` method

```python
from aaiclick.orchestration import OrchContext, Task, Group, create_task

# Create orchestration context
async with OrchContext() as ctx:
    # Define tasks in memory
    task1 = create_task("myapp.func1", kwargs={...})
    task2 = create_task("myapp.func2", kwargs={...})
    group1 = Group(name="processing")

    # Commit tasks and groups to database with job_id
    await ctx.apply(task1, job_id=job.id)  # Apply single task
    await ctx.apply([task1, task2, group1], job_id=job.id)  # Apply multiple

# ctx.apply() performs:
# - Sets job_id on all items
# - Generates snowflake IDs for Groups if not set
# - Inserts Task and Group records in database
# - Returns committed objects with IDs assigned
```

**`context.apply()` Signature:**
```python
async def apply(
    self,
    items: Task | Group | list[Task | Group],
    job_id: int,
) -> Task | Group | list[Task | Group]:
    """
    Commit tasks, groups, and their dependencies to the database.

    Args:
        items: Single Task/Group or list of Task/Group objects
        job_id: Job ID to assign to all items

    Returns:
        Same items with database IDs populated
    """
```

### DAG Construction with Dependency Operators ⚠️ NOT YET IMPLEMENTED (Phase 7+)

Planned Airflow-like syntax for defining dependencies between tasks and groups:

```python
from aaiclick.orchestration import Task, Group

# Task → Task dependencies
task1 = Task(entrypoint="myapp.extract", kwargs={...})
task2 = Task(entrypoint="myapp.transform", kwargs={...})
task3 = Task(entrypoint="myapp.load", kwargs={...})

# task2 depends on task1 (task1 executes before task2)
task1 >> task2

# task3 depends on task2
task2 >> task3

# Equivalent chaining
task1 >> task2 >> task3

# Reverse syntax (task1 depends on task2)
task2 << task1  # Same as: task1 >> task2

# Group → Group dependencies
extract_group = Group(name="extract")
transform_group = Group(name="transform")
load_group = Group(name="load")

extract_group >> transform_group >> load_group

# Task → Group dependencies (task completes before all tasks in group start)
validation_task = Task(entrypoint="myapp.validate", kwargs={...})
validation_task >> transform_group

# Group → Task dependencies (all tasks in group complete before task starts)
transform_group >> final_report_task

# Mixed dependencies
task1 >> group1 >> task2 >> group2 >> task3

# Multiple dependencies (fan-out and fan-in)
# Fan-out: task1 must complete before task2, task3, and task4 can start
task1 >> [task2, task3, task4]

# Fan-in: task5 waits for task2, task3, and task4 to complete
[task2, task3, task4] >> task5

# Complex DAG
source_task >> extract_group >> [transform_task1, transform_task2]
[transform_task1, transform_task2] >> load_group >> final_task

# Commit all tasks, groups, and dependencies to the database
await context.apply([source_task, extract_group, transform_task1, transform_task2, load_group, final_task])
# Or pass individual items
await context.apply(source_task)
await context.apply(extract_group)
```

**Using `context.apply()` to commit DAGs:**

```python
from aaiclick.orchestration import Context, Task, Group

# Create context for a job
context = Context(job_id=job.id)

# Define tasks
extract = Task(entrypoint="myapp.extract", kwargs={...})
transform = Task(entrypoint="myapp.transform", kwargs={...})
load = Task(entrypoint="myapp.load", kwargs={...})

# Define dependencies
extract >> transform >> load

# Commit all tasks and dependencies to database
await context.apply([extract, transform, load])
# context.apply() saves:
# - All Task/Group objects
# - All Dependency records created by >> and << operators
# - Validates no circular dependencies exist
```

**Operator Semantics:**
- `A >> B`: B depends on A (A is previous, B is next)
- `A << B`: A depends on B (B is previous, A is next)
- `A >> [B, C, D]`: B, C, and D all depend on A (fan-out)
- `[A, B, C] >> D`: D depends on all of A, B, and C (fan-in)
- Works with any combination of Task and Group objects
- Dependencies are stored in the unified Dependency table
- Circular dependencies are detected and rejected

**How Fan-In Works (`[A, B] >> C`):**

Fan-in is **not** a built-in Python feature. It uses Python's **reverse operator** mechanism:

1. Python evaluates `[A, B] >> C`
2. Python first tries `list.__rshift__([A, B], C)` - but lists don't support `>>`
3. Python falls back to the **reverse operator**: `C.__rrshift__([A, B])`
4. The `__rrshift__` method on `C` receives the list `[A, B]` and calls `C.depends_on(A)` and `C.depends_on(B)`

This means:
- `__rshift__`: Normal operator called on left operand (`A >> B` → `A.__rshift__(B)`)
- `__rrshift__`: Reverse operator called on right operand when left doesn't support operation (`[A, B] >> C` → `C.__rrshift__([A, B])`)
- Same pattern for `__lshift__` and `__rlshift__`

**Implementation:**
```python
class Task(SQLModel, table=True):
    # ... fields ...

    def depends_on(self, other: Union["Task", "Group"]) -> "Task":
        """
        Declare that this task depends on another task or group.
        Creates a Dependency record in the database.

        Args:
            other: Task or Group that must complete before this task

        Returns:
            self (for chaining)
        """
        dependency = Dependency(
            previous_id=other.id,
            previous_type="task" if isinstance(other, Task) else "group",
            next_id=self.id,
            next_type="task"
        )
        session.add(dependency)
        return self

    def __rshift__(self, other: Union["Task", "Group", list]) -> Union["Task", "Group", list]:
        """A >> B: B depends on A"""
        if isinstance(other, list):
            for item in other:
                item.depends_on(self)
            return other
        else:
            other.depends_on(self)
            return other

    def __lshift__(self, other: Union["Task", "Group", list]) -> "Task":
        """A << B: A depends on B"""
        if isinstance(other, list):
            for item in other:
                self.depends_on(item)
        else:
            self.depends_on(other)
        return self

    def __rrshift__(self, other: Union["Task", "Group", list]) -> "Task":
        """Reverse: [A, B] >> C means C depends on A and B (fan-in)"""
        if isinstance(other, list):
            for item in other:
                self.depends_on(item)
        else:
            self.depends_on(other)
        return self

    def __rlshift__(self, other: Union["Task", "Group", list]) -> Union["Task", "Group", list]:
        """Reverse: [A, B] << C means A and B depend on C (fan-out)"""
        if isinstance(other, list):
            for item in other:
                item.depends_on(self)
            return other
        else:
            other.depends_on(self)
            return other

class Group(SQLModel, table=True):
    # ... fields ...

    def depends_on(self, other: Union[Task, "Group"]) -> "Group":
        """
        Declare that this group depends on a task or another group.
        Creates a Dependency record in the database.

        Args:
            other: Task or Group that must complete before tasks in this group

        Returns:
            self (for chaining)
        """
        dependency = Dependency(
            previous_id=other.id,
            previous_type="task" if isinstance(other, Task) else "group",
            next_id=self.id,
            next_type="group"
        )
        session.add(dependency)
        return self

    def __rshift__(self, other: Union[Task, "Group", list]) -> Union[Task, "Group", list]:
        """A >> B: B depends on A"""
        if isinstance(other, list):
            for item in other:
                item.depends_on(self)
            return other
        else:
            other.depends_on(self)
            return other

    def __lshift__(self, other: Union[Task, "Group", list]) -> "Group":
        """A << B: A depends on B"""
        if isinstance(other, list):
            for item in other:
                self.depends_on(item)
        else:
            self.depends_on(other)
        return self

    def __rrshift__(self, other: Union[Task, "Group", list]) -> "Group":
        """Reverse: [A, B] >> C means C depends on A and B (fan-in)"""
        if isinstance(other, list):
            for item in other:
                self.depends_on(item)
        else:
            self.depends_on(other)
        return self

    def __rlshift__(self, other: Union[Task, "Group", list]) -> Union[Task, "Group", list]:
        """Reverse: [A, B] << C means A and B depend on C (fan-out)"""
        if isinstance(other, list):
            for item in other:
                item.depends_on(self)
            return other
        else:
            other.depends_on(self)
            return other
```

### Worker Management ⚠️ NOT YET IMPLEMENTED (Phase 6+)

```python
from aaiclick.orchestration import (
    register_worker,
    worker_heartbeat,
    deregister_worker,
    list_workers
)

# Register worker
worker = await register_worker(hostname="worker-01", pid=12345)

# Heartbeat (periodic)
await worker_heartbeat(worker.id)

# Deregister
await deregister_worker(worker.id)

# List active workers
workers = await list_workers(status=WorkerStatus.ACTIVE)
```

## Configuration

### Environment Variables

```bash
# PostgreSQL connection
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_USER=aaiclick
POSTGRES_PASSWORD=secret
POSTGRES_DB=aaiclick_orchestration

# Task logging (optional - defaults via get_logs_dir())
AAICLICK_LOG_DIR=<custom-path>  # Override default log directory

# Worker settings
WORKER_HEARTBEAT_INTERVAL=30  # seconds
WORKER_TASK_TIMEOUT=3600      # seconds
WORKER_MAX_RETRIES=3

# Job settings
JOB_DEFAULT_TIMEOUT=86400     # seconds (24 hours)
```

**Log Directory Resolution**:

The log directory is resolved via `get_logs_dir()` which provides OS-dependent defaults:

```python
def get_logs_dir() -> str:
    """
    Get task log directory with OS-dependent defaults.

    Defaults:
    - macOS: ${HOME}/.aaiclick/logs
    - Linux: /var/log/aaiclick

    Returns:
        Log directory path (creates if doesn't exist)
    """
    if custom_dir := os.getenv("AAICLICK_LOG_DIR"):
        return custom_dir

    if sys.platform == "darwin":  # macOS
        return os.path.expanduser("~/.aaiclick/logs")
    else:  # Linux
        return "/var/log/aaiclick"
```

**Notes**:
- For distributed workers, use a shared mount (NFS, EFS, etc.) and set `AAICLICK_LOG_DIR` to the mount path
- Single-machine deployments can use local filesystem with defaults
- Directory is created automatically if it doesn't exist

### Database Connection

**Implementation**: `aaiclick/orchestration/database.py`

The orchestration backend uses a global asyncpg connection pool (not SQLAlchemy engine):

```python
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

# Global async engine (SQLAlchemy manages connection pooling internally)
_engine: list[Optional[AsyncEngine]] = [None]

def _get_engine() -> AsyncEngine:
    """Get or create the global SQLAlchemy AsyncEngine."""
    if _engine[0] is None:
        database_url = f"postgresql+asyncpg://{user}:{password}@{host}:{port}/{database}"
        _engine[0] = create_async_engine(database_url, echo=False)
    return _engine[0]

# Usage pattern - create sessions from the global engine
async with OrchContext():
    async with get_orch_context_session() as session:
        result = await session.execute(select(Job))
        jobs = result.scalars().all()
```

**Implementation**: `aaiclick/orchestration/context.py` defines the global engine and context management.

## Implementation Plan

**See**: `docs/orchestration_implementation_plan.md` for detailed phase-by-phase implementation plan

**Current Status**:
- ✅ Phase 1: Database Setup (complete)
- ✅ Phase 2: Core Factories (complete)
- ✅ Phase 3: run_job_test() Function (complete)
- ✅ Phase 4: OrchContext Integration (complete)
- ⚠️ Phase 5: Testing & Examples (in progress)
- ⚠️ Phase 6+: Distributed Workers, Groups, Dependencies, Dynamic Task Creation

## Monitoring & Observability

### Metrics to Track

```python
# Job metrics
- jobs_created_total
- jobs_completed_total
- jobs_failed_total
- job_duration_seconds

# Task metrics
- tasks_created_total
- tasks_completed_total
- tasks_failed_total
- task_duration_seconds
- task_queue_depth

# Worker metrics
- workers_active
- worker_task_execution_time
- worker_heartbeat_age
```

### Health Checks

```python
async def health_check():
    """Check orchestration backend health."""
    checks = {
        "database": await check_database_connection(),
        "workers": await check_active_workers(),
        "stale_tasks": await check_stale_tasks(),
    }
    return all(checks.values())
```

## Error Handling

### Task Failures

1. **Transient errors**: Retry up to `max_retries`
2. **Permanent errors**: Mark task as failed, fail parent job
3. **Timeout**: Kill worker, retry task on another worker

### Worker Failures

1. **Heartbeat timeout**: Mark worker as stopped
2. **Orphaned tasks**: Reclaim tasks from stopped workers
3. **Recovery**: Reset orphaned tasks to pending status

### Job Failures

1. **Task failure**: Propagate to job if critical
2. **Cancellation**: Cancel all pending tasks
3. **Cleanup**: Remove temporary resources

## Packaging Consideration

Database migrations are bundled with the aaiclick package and executable via CLI:

**Implementation**: `aaiclick/__main__.py` and `aaiclick/orchestration/migrate.py`

```bash
# Run migrations
python -m aaiclick migrate

# Show current revision
python -m aaiclick migrate current

# Show migration history
python -m aaiclick migrate history

# Upgrade to specific revision
python -m aaiclick migrate upgrade <revision>

# Downgrade to specific revision
python -m aaiclick migrate downgrade <revision>
```

**Key Features**:
- Migrations bundled with package (no separate Alembic installation needed)
- Programmatic Alembic execution via `aaiclick/orchestration/migrate.py`
- CLI entry point via `aaiclick/__main__.py`
- All Alembic commands supported (upgrade, downgrade, current, history, etc.)
- Locates `alembic.ini` and migrations relative to package installation

This ensures users can initialize and upgrade the orchestration database schema without manual migration management.

## References

- [SQLModel Documentation](https://sqlmodel.tiangolo.com/)
- [Alembic Documentation](https://alembic.sqlalchemy.org/)
- [PostgreSQL Locking](https://www.postgresql.org/docs/current/explicit-locking.html)
- [aaiclick Architecture](./aaiclick.md)
