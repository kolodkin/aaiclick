"""Tests for orchestration factory functions."""

from datetime import datetime

import pytest
from sqlalchemy import select

from aaiclick.orchestration.execution.image_build_task import IMAGE_BUILD_ENTRYPOINT
from aaiclick.orchestration.factories import create_built_job, create_job, create_task
from aaiclick.orchestration.jobs import get_task
from aaiclick.orchestration.models import JOB_PENDING, TASK_PENDING, Job, Task
from aaiclick.orchestration.orch_context import get_sql_session
from aaiclick.orchestration.result import data_list
from aaiclick.orchestration.runner_config import (
    ENTRY_MODULE,
    ENTRY_SHELL,
    DockerRunner,
    ImageBuild,
    ImagePrebuilt,
    dump_image_source,
)


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
    assert job.status == JOB_PENDING
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
        assert db_job.status == JOB_PENDING

        # Verify task was created and persisted
        result = await session.execute(select(Task).where(Task.job_id == job.id))
        tasks = result.scalars().all()
        assert len(tasks) == 1
        assert tasks[0].entrypoint == "mymodule.task1"
        assert tasks[0].status == TASK_PENDING
        assert tasks[0].kwargs == {}


async def test_create_job_with_task(orch_ctx):
    """Test job creation with Task object."""
    task = create_task("mymodule.task2", {"param": "value"})
    job = await create_job("test_job_2", task)

    assert job.id > 0
    assert job.name == "test_job_2"

    # Verify task has job_id assigned via the public query helper
    db_task = await get_task(task.id)
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


def test_data_list_single(orch_ctx):
    """Test data_list() with a single item returns TaskResult with that item as data."""
    result = data_list("only")
    assert result.data == "only"
    assert result.tasks == []


def test_data_list_multiple(orch_ctx):
    """Test data_list() with multiple items returns TaskResult with a list."""
    result = data_list("a", "b", "c")
    assert result.data == ["a", "b", "c"]
    assert result.tasks == []


def test_create_task_module_default_explicit():
    t = create_task("mod.fn", {"a": 1}, entry_type=ENTRY_MODULE)
    assert t.entry_type == ENTRY_MODULE
    assert t.command is None


def test_create_task_jvm_takes_class_name_string():
    task = create_task("com.example.Pipeline#aggregate", {"window": 7}, entry_type="jvm")
    assert task.entrypoint == "com.example.Pipeline#aggregate"
    assert task.name == "Pipeline#aggregate"


def test_create_task_jvm_rejects_callable():
    with pytest.raises(ValueError, match="jvm.*class name"):
        create_task(lambda: None, entry_type="jvm")


def test_create_task_image_kwarg_sets_prebuilt_source():
    t = create_task("m.f", image="ghcr.io/x/y:1")
    assert t.image_source == {"type": "prebuilt", "image_tag": "ghcr.io/x/y:1"}


def test_create_task_git_kwargs_set_build_source():
    t = create_task("m.f", git_remote="https://example.com/r.git", git_sha="a" * 40, dockerfile="Dockerfile.gpu")
    assert t.image_source is not None
    assert t.image_source["type"] == "build"
    assert t.image_source["git_sha"] == "a" * 40


def test_create_task_image_and_git_mutually_exclusive():
    with pytest.raises(ValueError, match="mutually exclusive"):
        create_task("m.f", image="ghcr.io/x/y:1", git_sha="a" * 40)


def test_create_task_build_requires_remote_and_sha():
    with pytest.raises(ValueError, match="git_remote and git_sha"):
        create_task("m.f", git_sha="a" * 40)


def test_create_task_shell_carries_command():
    t = create_task(None, name="run", entry_type=ENTRY_SHELL, command=["python", "main.py"], command_env={"K": "v"})
    assert t.entry_type == ENTRY_SHELL
    assert t.command == ["python", "main.py"]
    assert t.command_env == {"K": "v"}
    assert t.entrypoint == ""  # shell tasks have no module entrypoint


async def _task_entrypoints(job_id: int) -> list[str]:
    async with get_sql_session() as session:
        rows = (await session.execute(select(Task).where(Task.job_id == job_id))).scalars().all()
    return [t.entrypoint for t in rows]


async def test_prebuilt_job_injects_no_build_task(orch_ctx_no_ch):
    source = ImagePrebuilt(image_tag="python:3.12")
    job = await create_built_job(
        name="j", entrypoint="", runner=DockerRunner(), image_source=source, entry_type="shell", command=["echo", "hi"]
    )
    assert await _task_entrypoints(job.id) == [""]
    assert job.runner == {"type": "docker"}


async def test_build_job_injects_no_build_task_without_registry(orch_ctx_no_ch, monkeypatch):
    """Without a registry the build is inline at launch — no build task in
    the graph (spec: docs/designs/orchestration.md "Image source", "No registry")."""
    monkeypatch.delenv("AAICLICK_REGISTRY", raising=False)
    source = ImageBuild(git_remote="git@x:r.git", git_sha="c" * 40)
    job = await create_built_job(
        name="j", entrypoint="mod.fn", runner=DockerRunner(), image_source=source, entry_type="module"
    )
    entrypoints = await _task_entrypoints(job.id)
    assert entrypoints == ["mod.fn"]


async def test_create_built_job_stamps_entry_and_injects_build_task(orch_ctx_no_ch, monkeypatch):
    monkeypatch.setenv("AAICLICK_REGISTRY", "registry.example:5000")
    source = ImageBuild(git_remote="https://example.com/r.git", git_sha="c" * 40)
    job = await create_built_job(name="j", entrypoint="m.entry", runner=DockerRunner(), image_source=source)
    async with get_sql_session() as session:
        rows = (await session.execute(select(Task).where(Task.job_id == job.id))).scalars().all()
    by_entry = {t.entrypoint: t for t in rows}
    assert by_entry["m.entry"].image_source == dump_image_source(source)
    assert IMAGE_BUILD_ENTRYPOINT in by_entry


async def test_create_built_job_prebuilt_injects_nothing(orch_ctx_no_ch, monkeypatch):
    monkeypatch.setenv("AAICLICK_REGISTRY", "registry.example:5000")
    source = ImagePrebuilt(image_tag="ghcr.io/x/y:1")
    job = await create_built_job(name="j", entrypoint="m.entry", runner=DockerRunner(), image_source=source)
    async with get_sql_session() as session:
        rows = (await session.execute(select(Task).where(Task.job_id == job.id))).scalars().all()
    assert [t.entrypoint for t in rows] == ["m.entry"]
    assert rows[0].image_source == dump_image_source(source)
