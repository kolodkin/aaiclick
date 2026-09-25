"""Tests for commit-time image stamping, validation, and build-task injection."""

import pytest
from sqlmodel import select

from .execution.execution_worker_context import set_current_task_info
from .execution.image_build_task import IMAGE_BUILD_ENTRYPOINT
from .factories import create_job, create_task
from .image_injection import stamp_inherited_image, validate_image_sources, validate_jvm_tasks
from .models import RUNNER_DOCKER, RUNNER_KUBERNETES, RUNNER_SUBPROCESS, Dependency, Job, Task
from .orch_context import commit_tasks, get_sql_session
from .runner_config import ImageBuild, ImagePrebuilt, dump_image_source

BUILD_A = dump_image_source(ImageBuild(git_remote="https://example.com/r.git", git_sha="a" * 40))
BUILD_B = dump_image_source(ImageBuild(git_remote="https://example.com/r.git", git_sha="b" * 40))
PREBUILT = dump_image_source(ImagePrebuilt(image_tag="ghcr.io/example/pipeline:1.0"))
OBJECT_REF = {"object_type": "object", "table": "t_1", "persistent": False}


def test_stamp_inherited_image_fills_only_undeclared():
    declared = create_task("m.f1")
    declared.image_source = BUILD_B
    inherited = create_task("m.f2")
    jvm = create_task("com.example.Pipeline", entry_type="jvm")
    stamp_inherited_image([declared, inherited, jvm], BUILD_A)
    assert declared.image_source == BUILD_B
    assert inherited.image_source == BUILD_A
    assert jvm.image_source is None


def test_stamp_inherited_image_none_parent_is_noop():
    t = create_task("m.f")
    stamp_inherited_image([t], None)
    assert t.image_source is None


@pytest.mark.parametrize(
    "runner_mode, match",
    [
        pytest.param(RUNNER_SUBPROCESS, "subprocess", id="subprocess-job"),
        pytest.param(RUNNER_KUBERNETES, "AAICLICK_REGISTRY", id="kubernetes-build-without-registry"),
    ],
)
def test_validate_rejects_image_source(monkeypatch, runner_mode, match):
    monkeypatch.delenv("AAICLICK_REGISTRY", raising=False)
    t = create_task("m.f")
    t.image_source = BUILD_A
    with pytest.raises(ValueError, match=match):
        validate_image_sources([t], runner_mode)


def _jvm_task(kwargs: dict | None = None, image_source: dict | None = PREBUILT) -> Task:
    t = create_task("com.example.Pipeline", kwargs, entry_type="jvm")
    t.image_source = image_source
    return t


def test_validate_jvm_accepts_plain_kwargs():
    validate_jvm_tasks([_jvm_task({"date": "2026-08-20", "window": 7})])


@pytest.mark.parametrize(
    "kwargs, image_source, match",
    [
        pytest.param(None, None, "image_source", id="no-image-source"),
        pytest.param({"inputs": [{"nested": OBJECT_REF}]}, PREBUILT, "plain values only", id="nested-object-ref"),
    ],
)
def test_validate_jvm_rejects(kwargs, image_source, match):
    with pytest.raises(ValueError, match=match):
        validate_jvm_tasks([_jvm_task(kwargs, image_source)])


def test_validate_jvm_requires_entrypoint():
    t = _jvm_task()
    t.entrypoint = ""
    with pytest.raises(ValueError, match="class name"):
        validate_jvm_tasks([t])


def test_validate_jvm_ignores_non_jvm_tasks():
    t = create_task("m.f", {"obj": OBJECT_REF})
    validate_jvm_tasks([t])


async def _create_docker_job() -> int:
    job = await create_job("j", "m.entry")
    async with get_sql_session() as session:
        row = (await session.execute(select(Job).where(Job.id == job.id))).scalar_one()
        row.runner_mode = RUNNER_DOCKER
        await session.commit()
    return job.id


async def _build_tasks_and_edges(job_id: int) -> tuple[list[Task], set[tuple[int, int]]]:
    """Persisted image-build tasks of ``job_id`` and every persisted ``(previous_id, next_id)`` edge."""
    async with get_sql_session() as session:
        builds = (await session.execute(select(Task).where(Task.job_id == job_id, Task.is_image_build))).scalars().all()
        deps = (await session.execute(select(Dependency))).scalars().all()
    return list(builds), {(d.previous_id, d.next_id) for d in deps}


async def test_commit_tasks_injects_one_build_task_per_image(orch_ctx_no_ch, monkeypatch):
    monkeypatch.setenv("AAICLICK_REGISTRY", "registry.example:5000")
    job_id = await _create_docker_job()
    t1, t2, t3 = create_task("m.f1"), create_task("m.f2"), create_task("m.f3")
    t1.image_source, t2.image_source, t3.image_source = BUILD_A, BUILD_A, BUILD_B

    await commit_tasks([t1, t2, t3], job_id)

    builds, edges = await _build_tasks_and_edges(job_id)
    assert len(builds) == 2
    assert all(b.entrypoint == IMAGE_BUILD_ENTRYPOINT and b.image_source is None for b in builds)
    assert all(b.max_retries == 2 for b in builds)
    # every dependent got an edge to its image's build task
    by_sha = {b.kwargs["git_sha"]: b.id for b in builds}
    assert (by_sha["a" * 40], t1.id) in edges
    assert (by_sha["a" * 40], t2.id) in edges
    assert (by_sha["b" * 40], t3.id) in edges


async def test_commit_tasks_reuses_existing_build_task_in_job(orch_ctx_no_ch, monkeypatch):
    monkeypatch.setenv("AAICLICK_REGISTRY", "registry.example:5000")
    job_id = await _create_docker_job()
    first = create_task("m.f1")
    first.image_source = BUILD_A
    await commit_tasks(first, job_id)

    second = create_task("m.f2")
    second.image_source = BUILD_A
    await commit_tasks(second, job_id)

    builds, edges = await _build_tasks_and_edges(job_id)
    assert len(builds) == 1
    assert {(builds[0].id, first.id), (builds[0].id, second.id)} <= edges


async def test_commit_tasks_stamps_and_injects_for_docker_job(orch_ctx_no_ch, monkeypatch):
    """commit_tasks on a docker job: undeclared tasks inherit the committing
    task's image, and a build task + edges appear in the same commit."""
    monkeypatch.setenv("AAICLICK_REGISTRY", "registry.example:5000")
    job_id = await _create_docker_job()
    async with get_sql_session() as session:
        entry = (await session.execute(select(Task).where(Task.job_id == job_id))).scalar_one()
        entry.image_source = BUILD_A
        await session.commit()
        entry_id = entry.id

    set_current_task_info(task_id=entry_id, job_id=job_id, image_source=BUILD_A)
    child = create_task("m.child")
    await commit_tasks(child, job_id)

    async with get_sql_session() as session:
        rows = (await session.execute(select(Task).where(Task.job_id == job_id))).scalars().all()
    by_entry = {t.entrypoint: t for t in rows}
    assert by_entry["m.child"].image_source == BUILD_A
    build = by_entry[IMAGE_BUILD_ENTRYPOINT]
    async with get_sql_session() as session:
        deps = (
            (await session.execute(select(Dependency).where(Dependency.next_id == by_entry["m.child"].id)))
            .scalars()
            .all()
        )
    assert build.id in {d.previous_id for d in deps}


async def test_commit_tasks_rejects_jvm_task_without_own_image(orch_ctx_no_ch):
    """The committing task's image is not a substitute for a jvm task's own."""
    job = await create_job("j", "m.entry")
    set_current_task_info(task_id=1, job_id=job.id, image_source=BUILD_A)
    with pytest.raises(ValueError, match="no image_source"):
        await commit_tasks(create_task("com.example.Pipeline", entry_type="jvm"), job.id)


async def test_commit_tasks_subprocess_job_rejects_image(orch_ctx_no_ch):
    job = await create_job("j", "m.entry")
    t = create_task("m.child")
    t.image_source = BUILD_A
    with pytest.raises(ValueError, match="subprocess"):
        await commit_tasks(t, job.id)
