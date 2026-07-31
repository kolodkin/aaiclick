"""Tests for per-task runner dispatch (dispatch.py)."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from ..models import RUNNER_DOCKER, RUNNER_KUBERNETES, RUNNER_SUBPROCESS, Task
from ..runner_config import ImageBuild, ImagePrebuilt, dump_image_source
from . import dispatch, docker_build
from .docker_build import resolve_launch_image
from .execution_worker import JobDispatch

BUILD_A = dump_image_source(ImageBuild(git_remote="https://example.com/r.git", git_sha="a" * 40))
PREBUILT = dump_image_source(ImagePrebuilt(image_tag="ghcr.io/x/y:1"))


def _task(entrypoint="user.module.entry", task_id=42, job_id=1, image_source=None) -> Task:
    return Task(id=task_id, job_id=job_id, entrypoint=entrypoint, name="test", image_source=image_source)


class _FakeResult:
    def __init__(self, job):
        self._job = job

    def scalar_one_or_none(self):
        return self._job


class _FakeSession:
    def __init__(self, job):
        self._job = job

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return None

    async def execute(self, *a, **k):
        return _FakeResult(self._job)


def test_jobdispatch_carries_entry_fields():
    d = JobDispatch(
        runner_mode="docker",
        kubernetes_config=None,
        entry_type="shell",
        command=["echo", "hi"],
        command_env={"K": "v"},
    )
    assert d.entry_type == "shell"
    assert d.command == ["echo", "hi"]


async def test_null_image_source_dispatches_subprocess_even_on_docker_job():
    """A NULL-image task runs on the host regardless of job runner_mode —
    the rule that host-pins injected build tasks. No job query needed."""
    resolved = await dispatch._resolve_dispatch(_task(image_source=None))
    assert resolved.runner_mode == RUNNER_SUBPROCESS
    assert resolved.image_source is None


async def test_prebuilt_image_source_dispatches_docker_with_source(monkeypatch):
    user_task = _task(task_id=100, job_id=200, image_source=PREBUILT)

    class _FakeJob:
        runner_mode = RUNNER_DOCKER
        runner = {"type": "docker"}

    monkeypatch.setattr(dispatch, "get_sql_session", lambda: _FakeSession(_FakeJob()))
    resolved = await dispatch._resolve_dispatch(user_task)
    assert resolved.runner_mode == RUNNER_DOCKER
    assert isinstance(resolved.image_source, ImagePrebuilt)


async def test_resolve_launch_image_prebuilt_tag_verbatim():
    source = ImagePrebuilt(image_tag="ghcr.io/x/y:1")
    assert await resolve_launch_image(source, task_id=1) == "ghcr.io/x/y:1"


async def test_resolve_launch_image_computes_registry_tag(monkeypatch):
    monkeypatch.setenv("AAICLICK_REGISTRY", "registry.example:5000")
    source = ImageBuild(git_remote="https://example.com/r.git", git_sha="a" * 40)
    tag = await resolve_launch_image(source, task_id=1)
    assert tag == "registry.example:5000/aaiclick-job:" + "a" * 40


async def test_resolve_launch_image_rejects_missing_source():
    with pytest.raises(ValueError, match="no image_source"):
        await resolve_launch_image(None, task_id=42)


async def test_resolve_launch_image_builds_inline_without_registry(monkeypatch):
    calls = []

    async def fake_build(source, image_tag):
        calls.append(image_tag)

    monkeypatch.delenv("AAICLICK_REGISTRY", raising=False)
    monkeypatch.setattr(docker_build, "build_image_to_tag", fake_build)
    source = ImageBuild(git_remote="https://example.com/r.git", git_sha="a" * 40)
    tag = await resolve_launch_image(source, task_id=1)
    assert calls == ["aaiclick-job:" + "a" * 40]
    assert tag == "aaiclick-job:" + "a" * 40


async def test_resolve_launch_image_skips_build_with_registry(monkeypatch):
    calls = []

    async def fake_build(source, image_tag):
        calls.append(image_tag)

    monkeypatch.setenv("AAICLICK_REGISTRY", "registry.example:5000")
    monkeypatch.setattr(docker_build, "build_image_to_tag", fake_build)
    source = ImageBuild(git_remote="https://example.com/r.git", git_sha="a" * 40)
    tag = await resolve_launch_image(source, task_id=1)
    assert calls == []  # the dependency edge guaranteed the push
    assert tag == "registry.example:5000/aaiclick-job:" + "a" * 40


async def test_dispatch_execute_routes_docker_to_container_runner(monkeypatch):
    user_task = _task()
    spec = JobDispatch(RUNNER_DOCKER, None)
    monkeypatch.setattr(dispatch, "_resolve_dispatch", AsyncMock(return_value=spec))
    in_container = AsyncMock(return_value=(True, None, None, None))
    monkeypatch.setitem(dispatch._IMAGE_RUNNERS, RUNNER_DOCKER, in_container)

    await dispatch.dispatch_execute(user_task, execution_worker_id=1)
    in_container.assert_awaited_once_with(user_task, 1, spec)


async def test_dispatch_execute_routes_subprocess_to_mp_child(monkeypatch):
    user_task = _task()
    spec = JobDispatch(RUNNER_SUBPROCESS, None)
    monkeypatch.setattr(dispatch, "_resolve_dispatch", AsyncMock(return_value=spec))
    in_child = AsyncMock(return_value=(True, None, None, None))
    monkeypatch.setattr(dispatch, "_run_task_in_child", in_child)

    await dispatch.dispatch_execute(user_task, execution_worker_id=2)
    in_child.assert_awaited_once_with(user_task, 2)


async def test_dispatch_execute_routes_kubernetes_to_pod_runner(monkeypatch):
    user_task = _task()
    spec = JobDispatch(RUNNER_KUBERNETES, {"namespace": "ml"})
    monkeypatch.setattr(dispatch, "_resolve_dispatch", AsyncMock(return_value=spec))
    in_pod = AsyncMock(return_value=(True, None, None, None))
    monkeypatch.setitem(dispatch._IMAGE_RUNNERS, RUNNER_KUBERNETES, in_pod)

    await dispatch.dispatch_execute(user_task, execution_worker_id=3)
    in_pod.assert_awaited_once_with(user_task, 3, spec)
