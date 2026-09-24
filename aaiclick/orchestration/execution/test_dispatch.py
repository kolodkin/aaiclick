"""Tests for per-task runner dispatch (dispatch.py)."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from ..models import RUNNER_DOCKER, RUNNER_KUBERNETES, RUNNER_SUBPROCESS, Task
from ..runner_config import ImageBuild, ImagePrebuilt, dump_image_source
from . import dispatch
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


async def test_jvm_without_container_runner_fails_instead_of_python_child():
    task = _task(entrypoint="com.example.Pipeline", image_source=None)
    task.entry_type = "jvm"
    success, result_ref, error = await dispatch.dispatch_execute(task, execution_worker_id=1)
    assert success is False
    assert result_ref is None
    assert "jvm" in (error or "")


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


@pytest.mark.parametrize(
    "runner_mode, kubernetes_config",
    [
        pytest.param(RUNNER_DOCKER, None, id="docker_to_container_runner"),
        pytest.param(RUNNER_KUBERNETES, {"namespace": "ml"}, id="kubernetes_to_pod_runner"),
    ],
)
async def test_dispatch_execute_routes_image_runner(monkeypatch, runner_mode, kubernetes_config):
    user_task = _task()
    spec = JobDispatch(runner_mode, kubernetes_config)
    monkeypatch.setattr(dispatch, "_resolve_dispatch", AsyncMock(return_value=spec))
    runner = AsyncMock(return_value=(True, None, None, None))
    monkeypatch.setitem(dispatch._IMAGE_RUNNERS, runner_mode, runner)

    await dispatch.dispatch_execute(user_task, execution_worker_id=1)
    runner.assert_awaited_once_with(user_task, 1, spec)
