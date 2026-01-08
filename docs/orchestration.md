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
┌─────────────┐
│  aaiclick   │
│  operators  │ ──┐
└─────────────┘   │
                  │  Creates tasks dynamically
                  ▼
┌──────────────────────────────────────┐
│         PostgreSQL Database          │
│  ┌────────┐  ┌──────┐  ┌──────────┐ │
│  │  Jobs  │──│Tasks │  │ Workers  │ │
│  └────────┘  └──────┘  └──────────┘ │
└──────────────────────────────────────┘
                  ▲
                  │  Polls and executes
                  │
┌─────────────────┴─────────────────┐
│  Worker Pool (N processes/nodes)  │
└───────────────────────────────────┘
```

## Technology Stack

- **SQLModel**: Type-safe ORM with Pydantic integration
- **Alembic**: Database migrations
- **PostgreSQL**: Persistent state store
- **asyncpg** (via SQLModel): Async database driver

### Why PostgreSQL?

- **ACID compliance**: Strong consistency guarantees for job state
- **Row-level locking**: Safe concurrent task claiming by workers
- **JSONB support**: Flexible task parameter storage
- **Mature ecosystem**: Alembic, connection pooling, monitoring

## Data Models

### Job

Represents a workflow containing one or more tasks.

```python
from datetime import datetime
from typing import Optional
from sqlmodel import Field, SQLModel, Relationship

class Job(SQLModel, table=True):
    __tablename__ = "jobs"

    id: Optional[int] = Field(default=None, primary_key=True)

    # Job metadata
    name: str = Field(index=True)
    description: Optional[str] = None

    # Status tracking
    status: str = Field(default="pending", index=True)
    # Status values: pending, running, completed, failed, cancelled

    # Timestamps
    created_at: datetime = Field(default_factory=datetime.utcnow, index=True)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None

    # Relationship
    tasks: list["Task"] = Relationship(back_populates="job")
```

**Job Status Lifecycle:**
```
pending → running → completed
                 → failed
                 → cancelled
```

### Task

Represents a single executable unit of work within a job.

```python
from datetime import datetime
from typing import Optional, Dict, Any
from sqlmodel import Field, SQLModel, Relationship, Column, JSON

class Task(SQLModel, table=True):
    __tablename__ = "tasks"

    id: Optional[int] = Field(default=None, primary_key=True)
    job_id: int = Field(foreign_key="jobs.id", index=True)

    # Execution specification
    entrypoint: str = Field()
    # Format: "module.submodule.function" (importable callable)
    # Example: "aaiclick.operators.map_function"

    kwargs: Dict[str, Any] = Field(default_factory=dict, sa_column=Column(JSON))
    # Dictionary mapping parameter names to Object table IDs
    # Example: {"input_obj": "tbl_abc123", "threshold": 0.5}

    # Status tracking
    status: str = Field(default="pending", index=True)
    # Status values: pending, claimed, running, completed, failed, cancelled

    # Result
    result_table_id: Optional[str] = None
    # ClickHouse table ID of the result Object

    # Error tracking
    error_message: Optional[str] = None
    retry_count: int = Field(default=0)
    max_retries: int = Field(default=3)

    # Worker assignment
    worker_id: Optional[str] = Field(default=None, index=True)
    # Identifier of the worker executing this task

    # Timestamps
    created_at: datetime = Field(default_factory=datetime.utcnow, index=True)
    claimed_at: Optional[datetime] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None

    # Relationship
    job: Job = Relationship(back_populates="tasks")
```

**Task Status Lifecycle:**
```
pending → claimed → running → completed
                           → failed → pending (if retries remain)
                                   → failed (max retries exceeded)
       → cancelled
```

### Worker

Represents an active worker process.

```python
from datetime import datetime
from typing import Optional
from sqlmodel import Field, SQLModel

class Worker(SQLModel, table=True):
    __tablename__ = "workers"

    id: str = Field(primary_key=True)
    # Format: "{hostname}:{pid}:{timestamp}"

    # Worker metadata
    hostname: str = Field(index=True)
    pid: int

    # Status
    status: str = Field(default="active", index=True)
    # Status values: active, idle, stopped

    # Heartbeat
    last_heartbeat: datetime = Field(default_factory=datetime.utcnow, index=True)

    # Statistics
    tasks_completed: int = Field(default=0)
    tasks_failed: int = Field(default=0)

    # Timestamps
    started_at: datetime = Field(default_factory=datetime.utcnow)
```

## Task Execution Flow

### 1. Job Creation

```python
from aaiclick.orchestration import create_job

# Create job with initial task
job = await create_job(
    name="data_processing_pipeline",
    tasks=[
        {
            "entrypoint": "myapp.processors.load_and_process_data",
            "kwargs": {"source": "dataset_v1"}
        }
    ]
)

# The initial task can dynamically create follow-up tasks during execution:
# async def load_and_process_data(source: str):
#     data_obj = await load_data(source)
#     # Create validation task with actual table ID
#     await add_task_to_current_job(
#         entrypoint="myapp.processors.validate_data",
#         kwargs={"input_obj": data_obj.table_id}
#     )
```

### 2. Dynamic Task Creation

Tasks can create additional tasks during execution via aaiclick operators:

```python
# In aaiclick/operators.py
async def map(callback: str, obj: Object) -> Object:
    """
    Apply callback to each element of obj in parallel.
    Creates one task per chunk of data using offset/limit.
    """
    from aaiclick.orchestration import add_task_to_current_job

    # Get current job context
    job_id = get_current_job_id()

    # Get total row count without reading data
    total_rows = await obj.count()
    chunk_size = 10000  # Configurable chunk size

    # Create task for each chunk using offset/limit
    # Note: Requires View concept (see Known Design Gaps)
    for offset in range(0, total_rows, chunk_size):
        await add_task_to_current_job(
            job_id=job_id,
            entrypoint=callback,
            kwargs={
                "table_id": obj.table_id,
                "offset": offset,
                "limit": chunk_size
            }
        )

    # Return handle to future results
    num_chunks = (total_rows + chunk_size - 1) // chunk_size
    return await create_result_collector(job_id, num_chunks)
```

### 3. Worker Task Execution Loop

```python
# Worker main loop
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

            # Execute task with Context bound to job
            async with Context(job_id=task.job_id) as ctx:
                # Import and execute entrypoint
                func = import_function(task.entrypoint)
                result_obj = await func(**task.kwargs)

            # Store result
            await update_task_result(
                task.id,
                result_table_id=result_obj.table_id,
                status="completed"
            )

        except Exception as e:
            # Handle failure
            await handle_task_failure(task, error=str(e))
```

### 4. Task Claiming (Atomic)

Uses PostgreSQL row-level locking for safe concurrent access:

```sql
-- Implemented via SQLModel/SQLAlchemy
-- Prioritizes tasks from oldest running jobs first
-- Also marks job as started when first task is claimed
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
        ORDER BY j.started_at ASC
        LIMIT 1
        FOR UPDATE SKIP LOCKED
    )
    RETURNING *
)
UPDATE jobs
SET started_at = COALESCE(started_at, NOW())
WHERE id = (SELECT job_id FROM claimed_task)
RETURNING (SELECT * FROM claimed_task);
```

**Key features:**
- `FOR UPDATE SKIP LOCKED`: Skip rows locked by other workers
- `ORDER BY j.started_at ASC`: Prioritize tasks from oldest running jobs
- `COALESCE(started_at, NOW())`: Atomically set job's started_at when first task is claimed
- Atomic update: Prevents race conditions

## API / Interfaces

### Job Management

```python
from aaiclick.orchestration import (
    create_job,
    get_job,
    list_jobs,
    cancel_job
)

# Create job
job = await create_job(name="pipeline", tasks=[...])

# Get job status
job = await get_job(job_id)
print(f"Status: {job.status}, Progress: {job.completed_tasks}/{job.total_tasks}")

# List jobs
jobs = await list_jobs(status="running")

# Cancel job
await cancel_job(job_id)
```

### Task Management

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

### Worker Management

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
workers = await list_workers(status="active")
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

# Worker settings
WORKER_HEARTBEAT_INTERVAL=30  # seconds
WORKER_TASK_TIMEOUT=3600      # seconds
WORKER_MAX_RETRIES=3

# Job settings
JOB_DEFAULT_TIMEOUT=86400     # seconds (24 hours)
```

### Database Connection

```python
from sqlmodel import create_engine
from sqlalchemy.ext.asyncio import create_async_engine

DATABASE_URL = "postgresql+asyncpg://{user}:{password}@{host}:{port}/{db}"

engine = create_async_engine(
    DATABASE_URL,
    echo=False,
    pool_size=20,
    max_overflow=40
)
```

## Implementation Plan

### Phase 1: Core Infrastructure
1. Set up SQLModel models (Job, Task, Worker)
2. Configure Alembic migrations
3. Implement database connection management
4. Create basic CRUD operations

### Phase 2: Worker Implementation
1. Implement task claiming logic (with locking)
2. Build worker main loop
3. Add heartbeat mechanism
4. Implement error handling and retries

### Phase 3: Job Management
1. Job creation API
2. Task scheduling logic
3. Job status tracking
4. Progress monitoring

### Phase 4: Dynamic Task Creation
1. Add context tracking for current job
2. Implement `add_task_to_current_job()`
3. Integrate with aaiclick operators (`map`, `filter`, etc.)
4. Handle result collection

### Phase 5: Integration & Testing
1. Integrate with Context
2. Add comprehensive tests
3. Performance benchmarking
4. Documentation

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

## Known Design Gaps

### View vs Object

**Problem**: Distributed operations like `map()` need to partition data across tasks without copying or reading all data.

**Limitation**: The `Object` class represents entire ClickHouse tables. To process data in parallel chunks, we need a way to reference subsets of an Object's data without materializing separate table copies.

## References

- [SQLModel Documentation](https://sqlmodel.tiangolo.com/)
- [Alembic Documentation](https://alembic.sqlalchemy.org/)
- [PostgreSQL Locking](https://www.postgresql.org/docs/current/explicit-locking.html)
- [aaiclick Architecture](./aaiclick.md)
