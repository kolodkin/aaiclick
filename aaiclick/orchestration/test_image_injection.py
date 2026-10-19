"""Tests for commit-time image stamping, validation, and build-task injection."""

import pytest
from sqlmodel import select

from aaiclick.data.models import ViewSchema
from aaiclick.data.object.refs import callable_ref, group_results_ref, native_ref, pydantic_ref, upstream_ref

from .execution.execution_worker_context import TaskInfo, current_task_info
from .execution.image_build_task import IMAGE_BUILD_ENTRYPOINT
from .factories import create_job, create_task
from .image_injection import stamp_inherited_image, validate_plain_data_plane_tasks
from .models import Dependency, Job, Task
from .orch_context import commit_tasks, get_sql_session
from .runner_config import ImageBuild, ImagePrebuilt, dump_image_source

BUILD_A = dump_image_source(ImageBuild(git_remote="https://example.com/r.git", git_sha="a" * 40))
BUILD_B = dump_image_source(ImageBuild(git_remote="https://example.com/r.git", git_sha="b" * 40))
PREBUILT = dump_image_source(ImagePrebuilt(image_tag="ghcr.io/example/pipeline:1.0"))
OBJECT_REF = {"_aaiclick_type": "object", "table": "t_1"}


def test_stamp_inherited_image_fills_only_undeclared():
    declared = create_task("m.f1")
    declared.image_source = BUILD_B
    inherited = create_task("m.f2")
    stamp_inherited_image([declared, inherited], BUILD_A, "module")
    assert declared.image_source == BUILD_B
    assert inherited.image_source == BUILD_A


@pytest.mark.parametrize(
    "parent_entry_type, child, inherits",
    [
        # A Python image has no JVM entrypoint; a JVM image has no Python.
        pytest.param("module", create_task("com.example.Pipeline", entry_type="jvm"), False, id="jvm-of-module"),
        pytest.param("jvm", create_task("m.f"), False, id="module-of-jvm"),
        pytest.param("jvm", create_task("com.example.Worker", entry_type="jvm"), True, id="jvm-of-jvm"),
        # A shell argv runs in whatever image the parent had.
        pytest.param("jvm", create_task(None, entry_type="shell", command=["true"]), True, id="shell-of-jvm"),
        pytest.param("shell", create_task("m.f"), True, id="module-of-shell"),
    ],
)
def test_stamp_inherited_image_by_entry_type(parent_entry_type, child, inherits):
    stamp_inherited_image([child], BUILD_A, parent_entry_type)
    assert (child.image_source == BUILD_A) is inherits


def test_stamp_inherited_image_none_parent_is_noop():
    t = create_task("m.f")
    stamp_inherited_image([t], None, "module")
    assert t.image_source is None


def _jvm_task(kwargs: dict | None = None, image_source: dict | None = PREBUILT) -> Task:
    t = create_task("com.example.Pipeline", kwargs, entry_type="jvm")
    t.image_source = image_source
    return t


@pytest.mark.parametrize(
    "kwargs",
    [
        pytest.param({"date": "2026-08-20", "window": 7}, id="plain"),
        pytest.param({"rows": [upstream_ref(5)]}, id="nested-upstream-ref"),
        # The kwargs map is never a ref, so a parameter may carry a ref key's name.
        pytest.param({"_aaiclick_type": "csv", "value": "x"}, id="ref-key-as-param-name"),
        # native is opaque to the shim, like to Python's deserializer.
        pytest.param({"cfg": native_ref(OBJECT_REF)}, id="native-wrapper"),
    ],
)
def test_validate_plain_data_plane_accepts(kwargs):
    validate_plain_data_plane_tasks([_jvm_task(kwargs)])


@pytest.mark.parametrize(
    "kwargs, image_source, match",
    [
        pytest.param(None, None, "image_source", id="no-image-source"),
        pytest.param(
            {"inputs": [{"nested": OBJECT_REF}]}, PREBUILT, "object ref in kwarg .inputs.", id="nested-object-ref"
        ),
        pytest.param({"fn": callable_ref("m.f")}, PREBUILT, "callable ref in kwarg 'fn'", id="callable-ref"),
        pytest.param({"parts": group_results_ref(9)}, PREBUILT, "group_results ref", id="group-ref"),
        pytest.param(
            {"m": pydantic_ref(ViewSchema(fieldtype="a", columns={}))}, PREBUILT, "pydantic ref", id="pydantic-ref"
        ),
        # ``_aaiclick_type`` is reserved: an unknown value is an error, not plain data.
        pytest.param({"cfg": {"_aaiclick_type": "x"}}, PREBUILT, "Unknown _aaiclick_type", id="unknown-nested-type"),
    ],
)
def test_validate_plain_data_plane_rejects(kwargs, image_source, match):
    with pytest.raises(ValueError, match=match):
        validate_plain_data_plane_tasks([_jvm_task(kwargs, image_source)])


def test_validate_plain_data_plane_rejects_shell_object_ref():
    """Shell tasks share the jvm data plane: no Object refs, no image needed."""
    t = create_task(None, {"data": OBJECT_REF}, entry_type="shell", command=["true"])
    with pytest.raises(ValueError, match="shell task 'true' receives a object ref in kwarg 'data'"):
        validate_plain_data_plane_tasks([t])


def test_validate_plain_data_plane_jvm_requires_entrypoint():
    t = _jvm_task()
    t.entrypoint = ""
    with pytest.raises(ValueError, match="class name"):
        validate_plain_data_plane_tasks([t])


def test_validate_plain_data_plane_ignores_module_tasks():
    t = create_task("m.f", {"obj": OBJECT_REF})
    validate_plain_data_plane_tasks([t])


async def _build_tasks_and_edges(job_id: int) -> tuple[list[Task], set[tuple[int, int]]]:
    """Persisted image-build tasks of ``job_id`` and every persisted ``(previous_id, next_id)`` edge."""
    async with get_sql_session() as session:
        builds = (await session.execute(select(Task).where(Task.job_id == job_id, Task.is_image_build))).scalars().all()
        deps = (await session.execute(select(Dependency))).scalars().all()
    return list(builds), {(d.previous_id, d.next_id) for d in deps}


async def test_commit_tasks_injects_one_build_task_per_image(orch_ctx_no_ch, monkeypatch):
    monkeypatch.setenv("AAICLICK_REGISTRY", "registry.example:5000")
    job_id = (await create_job("j", "m.entry")).id
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
    job_id = (await create_job("j", "m.entry")).id
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
    job_id = (await create_job("j", "m.entry")).id
    async with get_sql_session() as session:
        entry = (await session.execute(select(Task).where(Task.job_id == job_id))).scalar_one()
        entry.image_source = BUILD_A
        await session.commit()
        entry_id = entry.id

    with current_task_info(TaskInfo(task_id=entry_id, job_id=job_id, image_source=BUILD_A)):
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
    with current_task_info(TaskInfo(task_id=1, job_id=job.id, image_source=BUILD_A)):
        with pytest.raises(ValueError, match="no image_source"):
            await commit_tasks(create_task("com.example.Pipeline", entry_type="jvm"), job.id)


async def test_create_job_leaves_no_job_when_tasks_rejected(orch_ctx_no_ch):
    """Job and tasks commit together: a rejected entry task leaves no job row."""
    with pytest.raises(ValueError, match="no image_source"):
        await create_job("rejected_entry", create_task("com.example.Pipeline", entry_type="jvm"))
    async with get_sql_session() as session:
        job = (await session.execute(select(Job).where(Job.name == "rejected_entry"))).scalar_one_or_none()
    assert job is None
