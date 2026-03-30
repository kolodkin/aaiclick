"""Tests for orchestration factory functions."""

from datetime import datetime

from sqlalchemy import select

from aaiclick.orchestration.decorators import job, task
from aaiclick.orchestration import TaskResult
from aaiclick.orchestration.factories import create_job, create_task, data_list, task_result, tasks_list
from aaiclick.orchestration.models import Job, JobStatus, Task, TaskStatus
from aaiclick.orchestration.orch_context import get_sql_session


async def test_create_task_basic(orch_ctx):
    """Test basic task creation."""
    task = create_task("mymodule.task1")

    assert task.id > 0  # Snowflake ID should be positive
    assert task.entrypoint == "mymodule.task1"
    assert task.name == "task1"
    assert task.kwargs == {}
    assert task.status == TaskStatus.PENDING
    assert isinstance(task.created_at, datetime)
    assert task.job_id is None  # Not assigned yet


async def test_create_task_with_kwargs(orch_ctx):
    """Test task creation with kwargs."""
    kwargs = {"param1": "value1", "param2": 42}
    task = create_task("mymodule.task2", kwargs)

    assert task.entrypoint == "mymodule.task2"
    assert task.name == "task2"
    assert task.kwargs == kwargs
    assert task.status == TaskStatus.PENDING


async def test_create_task_with_custom_name(orch_ctx):
    """Test task creation with explicit name."""
    t = create_task("mymodule.task1", name="custom_name")

    assert t.entrypoint == "mymodule.task1"
    assert t.name == "custom_name"


async def test_task_decorator_bare(orch_ctx):
    """Test @task bare decorator defaults name to function name."""

    @task
    async def my_func():
        pass

    t = my_func()
    assert t.name == "my_func"


async def test_task_decorator_with_name(orch_ctx):
    """Test @task(name="custom") sets custom name."""

    @task(name="custom")
    async def my_func():
        pass

    t = my_func()
    assert t.name == "custom"


async def test_task_decorator_with_name_and_retries(orch_ctx):
    """Test @task(name="custom", max_retries=3) sets both."""

    @task(name="custom", max_retries=3)
    async def my_func():
        pass

    t = my_func()
    assert t.name == "custom"
    assert t.max_retries == 3


async def test_job_decorator_bare(orch_ctx):
    """Test @job bare decorator defaults name to function name."""

    @job
    def my_pipeline():
        return tasks_list(create_task("mymodule.task1"))

    assert my_pipeline.name == "my_pipeline"


async def test_job_decorator_with_name_kwarg(orch_ctx):
    """Test @job(name="custom") sets custom name."""

    @job(name="custom")
    def my_pipeline():
        return tasks_list(create_task("mymodule.task1"))

    assert my_pipeline.name == "custom"


async def test_create_task_unique_ids(orch_ctx):
    """Test that each task gets a unique snowflake ID."""
    task1 = create_task("mymodule.task1")
    task2 = create_task("mymodule.task2")

    assert task1.id != task2.id
    assert task1.id > 0
    assert task2.id > 0


async def test_create_job_with_string(orch_ctx):
    """Test job creation with string callback."""
    job = await create_job("test_job", "mymodule.task1")

    assert job.id > 0
    assert job.name == "test_job"
    assert job.status == JobStatus.PENDING
    assert isinstance(job.created_at, datetime)
    assert job.started_at is None
    assert job.completed_at is None
    assert job.error is None

    # Verify job was persisted to database using ORM
    async with get_sql_session() as session:
        # Query for the job
        result = await session.execute(select(Job).where(Job.id == job.id))
        db_job = result.scalar_one_or_none()
        assert db_job is not None
        assert db_job.name == "test_job"
        assert db_job.status == JobStatus.PENDING

        # Verify task was created and persisted
        result = await session.execute(select(Task).where(Task.job_id == job.id))
        tasks = result.scalars().all()
        assert len(tasks) == 1
        assert tasks[0].entrypoint == "mymodule.task1"
        assert tasks[0].status == TaskStatus.PENDING
        assert tasks[0].kwargs == {}


async def test_create_job_with_task(orch_ctx):
    """Test job creation with Task object."""
    task = create_task("mymodule.task2", {"param": "value"})
    job = await create_job("test_job_2", task)

    assert job.id > 0
    assert job.name == "test_job_2"

    # Verify task has job_id assigned using ORM
    async with get_sql_session() as session:
        result = await session.execute(select(Task).where(Task.id == task.id))
        db_task = result.scalar_one_or_none()
        assert db_task is not None
        assert db_task.job_id == job.id
        assert db_task.entrypoint == "mymodule.task2"
        assert db_task.kwargs == {"param": "value"}


async def test_create_job_unique_ids(orch_ctx):
    """Test that each job gets a unique snowflake ID."""
    job1 = await create_job("job1", "mymodule.task1")
    job2 = await create_job("job2", "mymodule.task2")

    assert job1.id != job2.id
    assert job1.id > 0
    assert job2.id > 0


async def test_job_task_relationship(orch_ctx):
    """Test that job and task have correct relationship."""
    job = await create_job("relationship_test", "mymodule.task3")

    # Verify job and task relationship using ORM
    async with get_sql_session() as session:
        # Get job
        result = await session.execute(select(Job).where(Job.id == job.id))
        db_job = result.scalar_one_or_none()
        assert db_job is not None

        # Get tasks for this job
        result = await session.execute(select(Task).where(Task.job_id == job.id))
        tasks = result.scalars().all()
        assert len(tasks) == 1
        assert tasks[0].job_id == job.id


def test_task_result_factory(orch_ctx):
    """Test task_result() creates TaskResult with data and tasks."""
    t = create_task("mod.fn")
    result = task_result(data="value", tasks=[t])
    assert isinstance(result, TaskResult)
    assert result.data == "value"
    assert result.tasks == [t]


def test_data_list_single(orch_ctx):
    """Test data_list() with a single item returns TaskResult with that item as data."""
    result = data_list("only")
    assert isinstance(result, TaskResult)
    assert result.data == "only"
    assert result.tasks == []


def test_data_list_multiple(orch_ctx):
    """Test data_list() with multiple items returns TaskResult with a list."""
    result = data_list("a", "b", "c")
    assert isinstance(result, TaskResult)
    assert result.data == ["a", "b", "c"]
    assert result.tasks == []


def test_tasks_list_factory(orch_ctx):
    """Test tasks_list() creates TaskResult with tasks and no data."""
    t1 = create_task("mod.fn1")
    t2 = create_task("mod.fn2")
    result = tasks_list(t1, t2)
    assert isinstance(result, TaskResult)
    assert result.data is None
    assert result.tasks == [t1, t2]


def test_tasks_list_empty(orch_ctx):
    """Test tasks_list() with no arguments returns empty tasks list."""
    result = tasks_list()
    assert isinstance(result, TaskResult)
    assert result.data is None
    assert result.tasks == []
