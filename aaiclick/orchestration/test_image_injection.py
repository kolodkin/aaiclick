"""Tests for commit-time image stamping, validation, and build-task injection."""

import pytest
from sqlmodel import select

from .execution.execution_worker_context import set_current_task_info
from .execution.image_build_task import IMAGE_BUILD_ENTRYPOINT
from .factories import create_job, create_task
from .image_injection import inject_build_tasks, stamp_inherited_image, validate_image_sources
from .models import RUNNER_DOCKER, RUNNER_SUBPROCESS, Dependency, Job, Task
from .orch_context import commit_tasks, get_sql_session
from .runner_config import ImageBuild, dump_image_source

BUILD_A = dump_image_source(ImageBuild(git_remote="https://example.com/r.git", git_sha="a" * 40))
BUILD_B = dump_image_source(ImageBuild(git_remote="https://example.com/r.git", git_sha="b" * 40))


def test_stamp_inherited_image_fills_only_undeclared():
    declared = create_task("m.f1")
    declared.image_source = BUILD_B
    inherited = create_task("m.f2")
    stamp_inherited_image([declared, inherited], BUILD_A)
    assert declared.image_source == BUILD_B
    assert inherited.image_source == BUILD_A


def test_stamp_inherited_image_none_parent_is_noop():
    t = create_task("m.f")
    stamp_inherited_image([t], None)
    assert t.image_source is None


def test_validate_rejects_image_on_subprocess_job():
    t = create_task("m.f")
    t.image_source = BUILD_A
    with pytest.raises(ValueError, match="subprocess"):
        validate_image_sources([t], RUNNER_SUBPROCESS)


def test_validate_rejects_kubernetes_build_without_registry(monkeypatch):
    monkeypatch.delenv("AAICLICK_REGISTRY", raising=False)
    t = create_task("m.f")
    t.image_source = BUILD_A
    with pytest.raises(ValueError, match="AAICLICK_REGISTRY"):
        validate_image_sources([t], "kubernetes")


async def test_inject_creates_one_build_task_per_image(orch_ctx_no_ch, monkeypatch):
    monkeypatch.setenv("AAICLICK_REGISTRY", "registry.example:5000")
    job = await create_job("j", "m.entry")
    async with get_sql_session() as session:
        row = (await session.execute(select(Job).where(Job.id == job.id))).scalar_one()
        row.runner_mode = RUNNER_DOCKER
        t1, t2, t3 = create_task("m.f1"), create_task("m.f2"), create_task("m.f3")
        t1.image_source, t2.image_source, t3.image_source = BUILD_A, BUILD_A, BUILD_B
        for t in (t1, t2, t3):
            t.job_id = job.id
        injected = await inject_build_tasks(session, [t1, t2, t3], row)
        assert len(injected) == 2
        assert all(b.entrypoint == IMAGE_BUILD_ENTRYPOINT and b.image_source is None for b in injected)
        assert all(b.max_retries == 2 for b in injected)
        # every dependent got an edge to its image's build task
        edges = {(d.previous_id, d.next_id) for t in (t1, t2, t3) for d in t.previous_dependencies}
        by_sha = {b.kwargs["git_sha"]: b.id for b in injected}
        assert (by_sha["a" * 40], t1.id) in edges
        assert (by_sha["a" * 40], t2.id) in edges
        assert (by_sha["b" * 40], t3.id) in edges


async def test_inject_dedups_against_existing_build_task_in_job(orch_ctx_no_ch, monkeypatch):
    monkeypatch.setenv("AAICLICK_REGISTRY", "registry.example:5000")
    job = await create_job("j", "m.entry")
    async with get_sql_session() as session:
        row = (await session.execute(select(Job).where(Job.id == job.id))).scalar_one()
        row.runner_mode = RUNNER_DOCKER
        first = create_task("m.f1")
        first.image_source = BUILD_A
        first.job_id = job.id
        injected1 = await inject_build_tasks(session, [first], row)
        for obj in (*injected1, first):
            session.add(obj)
        await session.commit()
    async with get_sql_session() as session:
        row = (await session.execute(select(Job).where(Job.id == job.id))).scalar_one()
        second = create_task("m.f2")
        second.image_source = BUILD_A
        second.job_id = job.id
        injected2 = await inject_build_tasks(session, [second], row)
        assert injected2 == []  # existing build task reused
        assert second.previous_dependencies[0].previous_id == injected1[0].id


async def test_commit_tasks_stamps_and_injects_for_docker_job(orch_ctx_no_ch, monkeypatch):
    """commit_tasks on a docker job: undeclared tasks inherit the committing
    task's image, and a build task + edges appear in the same commit."""
    monkeypatch.setenv("AAICLICK_REGISTRY", "registry.example:5000")
    job = await create_job("j", "m.entry")
    async with get_sql_session() as session:
        row = (await session.execute(select(Job).where(Job.id == job.id))).scalar_one()
        row.runner_mode = RUNNER_DOCKER
        entry = (await session.execute(select(Task).where(Task.job_id == job.id))).scalar_one()
        entry.image_source = BUILD_A
        await session.commit()
        entry_id = entry.id

    set_current_task_info(task_id=entry_id, job_id=job.id, image_source=BUILD_A)
    child = create_task("m.child")
    await commit_tasks(child, job.id)

    async with get_sql_session() as session:
        rows = (await session.execute(select(Task).where(Task.job_id == job.id))).scalars().all()
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


async def test_commit_tasks_subprocess_job_rejects_image(orch_ctx_no_ch):
    job = await create_job("j", "m.entry")
    t = create_task("m.child")
    t.image_source = BUILD_A
    with pytest.raises(ValueError, match="subprocess"):
        await commit_tasks(t, job.id)


async def test_inject_noop_without_registry(orch_ctx_no_ch, monkeypatch):
    monkeypatch.delenv("AAICLICK_REGISTRY", raising=False)
    job = await create_job("j", "m.entry")
    async with get_sql_session() as session:
        row = (await session.execute(select(Job).where(Job.id == job.id))).scalar_one()
        row.runner_mode = RUNNER_DOCKER
        t = create_task("m.f")
        t.image_source = BUILD_A
        t.job_id = job.id
        assert await inject_build_tasks(session, [t], row) == []
        assert t.previous_dependencies == []
