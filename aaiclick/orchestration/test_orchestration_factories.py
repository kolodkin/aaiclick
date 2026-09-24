"""Tests for orchestration factory functions."""

from datetime import datetime

import pytest
from sqlalchemy import select

from aaiclick.orchestration.execution.image_build_task import IMAGE_BUILD_ENTRYPOINT
from aaiclick.orchestration.factories import create_built_job, create_job, create_task
from aaiclick.orchestration.jobs import get_task
from aaiclick.orchestration.models import (
    JOB_PENDING,
    TASK_PENDING,
    Job,
    Task,
)
from aaiclick.orchestration.orch_context import get_sql_session
from aaiclick.orchestration.result import data_list
from aaiclick.orchestration.runner_config import (
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


@pytest.mark.parametrize(
    "items, expected",
    [
        # A single item becomes the data itself.
        pytest.param(("only",), "only", id="single"),
        # Multiple items become a list.
        pytest.param(("a", "b", "c"), ["a", "b", "c"], id="multiple"),
    ],
)
def test_data_list(orch_ctx, items, expected):
    """data_list() returns a TaskResult carrying the items as data and no tasks."""
    result = data_list(*items)
    assert result.data == expected
    assert result.tasks == []


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


@pytest.mark.parametrize(
    "image_kwargs, match",
    [
        pytest.param({"image": "ghcr.io/x/y:1", "git_sha": "a" * 40}, "mutually exclusive", id="image-and-git"),
        # Container code reaches create_task directly, so it must not be the one unvalidated surface.
        pytest.param(
            {"git_remote": "https://example.com/r.git", "git_sha": "--upload-pack=touch /tmp/pwned"},
            "40-char lowercase hex",
            id="git-option-as-sha",
        ),
        pytest.param({"git_sha": "a" * 40}, "git_remote and git_sha", id="build-without-remote"),
    ],
)
def test_create_task_rejects_invalid_image_kwargs(image_kwargs, match):
    with pytest.raises(ValueError, match=match):
        create_task("m.f", **image_kwargs)


def test_create_task_shell_has_no_entrypoint_and_is_named_after_command():
    t = create_task(None, entry_type=ENTRY_SHELL, command=["python", "main.py"])
    assert t.entrypoint == ""  # shell tasks have no module entrypoint
    assert t.name == "python"


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


async def test_build_job_injects_build_task_without_registry(orch_ctx_no_ch, monkeypatch):
    """Submission never reads the build env: the build task is in the graph
    either way and the worker picks registry vs local when it runs."""
    monkeypatch.delenv("AAICLICK_REGISTRY", raising=False)
    monkeypatch.delenv("AAICLICK_LOCAL_BUILD", raising=False)
    source = ImageBuild(git_remote="git@x:r.git", git_sha="c" * 40)
    job = await create_built_job(
        name="j", entrypoint="mod.fn", runner=DockerRunner(), image_source=source, entry_type="module"
    )
    assert sorted(await _task_entrypoints(job.id)) == sorted([IMAGE_BUILD_ENTRYPOINT, "mod.fn"])


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
