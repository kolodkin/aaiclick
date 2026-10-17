"""Tests for per-task runner dispatch (dispatch.py)."""

from __future__ import annotations

from unittest.mock import ANY, AsyncMock

import pytest

from ..kubernetes_config import ENV_NAMESPACE, KubernetesConfig
from ..models import Task
from ..runner_config import ImageBuild, ImagePrebuilt, dump_image_source
from . import dispatch
from .execution_worker import JobDispatch
from .runner import DispatchError
from .runner_env import ENV_WORKER_RUNNER, RUNNER_DOCKER, RUNNER_KUBERNETES

BUILD_A = dump_image_source(ImageBuild(git_remote="https://example.com/r.git", git_sha="a" * 40))
PREBUILT = dump_image_source(ImagePrebuilt(image_tag="ghcr.io/x/y:1"))


def _task(entrypoint="user.module.entry", task_id=42, job_id=1, image_source=None) -> Task:
    return Task(id=task_id, job_id=job_id, entrypoint=entrypoint, name="test", image_source=image_source)


class _FakeResult:
    def __init__(self, value):
        self._value = value

    def scalar_one_or_none(self):
        return self._value


class _FakeSession:
    def __init__(self, value):
        self._value = value

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return None

    async def execute(self, *a, **k):
        return _FakeResult(self._value)


async def test_jvm_without_container_runner_fails_instead_of_python_child():
    task = _task(entrypoint="com.example.Pipeline", image_source=None)
    task.entry_type = "jvm"
    success, result_ref, error = await dispatch.dispatch_execute(task, execution_worker_id=1)
    assert success is False
    assert result_ref is None
    assert "jvm" in (error or "")


async def test_null_image_source_dispatches_subprocess_whatever_the_worker_runner(monkeypatch):
    monkeypatch.setenv(ENV_WORKER_RUNNER, "kubernetes")
    resolved = await dispatch._resolve_dispatch(_task(image_source=None))
    assert resolved.runner is None
    assert resolved.image_source is None


async def test_resolve_dispatch_without_worker_runner_raises(monkeypatch):
    monkeypatch.delenv(ENV_WORKER_RUNNER, raising=False)
    with pytest.raises(DispatchError, match="AAICLICK_RUNNER"):
        await dispatch._resolve_dispatch(_task(image_source=PREBUILT))


@pytest.mark.parametrize(
    "runner, resources, pod_config",
    [
        pytest.param(RUNNER_DOCKER, {"limits": {"cpu": "2"}}, None, id="docker"),
        pytest.param(RUNNER_KUBERNETES, {"limits": {"cpu": "2"}}, KubernetesConfig("ml", None, None), id="kubernetes"),
    ],
)
async def test_resolve_dispatch_uses_worker_runner_and_job_resources(monkeypatch, runner, resources, pod_config):
    """Both container runners read the job row for ``resources``; only
    kubernetes resolves a ``pod_config`` (cluster env)."""
    monkeypatch.setenv(ENV_WORKER_RUNNER, runner)
    monkeypatch.setenv(ENV_NAMESPACE, "ml")
    monkeypatch.setattr(dispatch, "get_sql_session", lambda: _FakeSession(resources))
    resolved = await dispatch._resolve_dispatch(_task(image_source=PREBUILT))
    assert resolved.runner == runner
    assert isinstance(resolved.image_source, ImagePrebuilt)
    assert resolved.resources == resources
    assert resolved.pod_config == pod_config


async def test_dispatch_shell_on_docker_uses_the_shell_vehicle(monkeypatch):
    """Container shell tasks run host-side (the file plane is multi-step), under
    a host-registered log run; only the subprocess runner uses the mp child."""
    monkeypatch.setattr(
        dispatch, "_resolve_dispatch", AsyncMock(return_value=JobDispatch(RUNNER_DOCKER, None, "shell"))
    )
    monkeypatch.setattr(dispatch, "register_host_log_run", AsyncMock(return_value=99))
    vehicle = AsyncMock(return_value=(True, None, None))
    monkeypatch.setitem(dispatch._SHELL_RUNNERS, RUNNER_DOCKER, vehicle)
    child = AsyncMock()
    monkeypatch.setattr(dispatch, "_run_task_in_child", child)

    assert await dispatch.dispatch_execute(_task(), execution_worker_id=1) == (True, None, None)

    vehicle.assert_awaited_once_with(ANY, 1, ANY, 99)
    child.assert_not_awaited()


async def test_resolve_dispatch_jvm_command_is_the_class_name(monkeypatch):
    """A jvm task is a shell task whose argv is the class name: the image's
    ENTRYPOINT (the aaiclick-task-api shim) receives it."""
    monkeypatch.setenv(ENV_WORKER_RUNNER, RUNNER_DOCKER)
    monkeypatch.setattr(dispatch, "get_sql_session", lambda: _FakeSession(None))
    task = _task(entrypoint="com.example.Pipeline", image_source=PREBUILT)
    task.entry_type = "jvm"
    resolved = await dispatch._resolve_dispatch(task)
    assert (resolved.entry_type, resolved.command) == ("jvm", ["com.example.Pipeline"])


async def test_dispatch_jvm_uses_shell_vehicle(monkeypatch):
    resolved = JobDispatch(RUNNER_DOCKER, None, "jvm", ["com.example.Pipeline"], image_source=PREBUILT)
    monkeypatch.setattr(dispatch, "_resolve_dispatch", AsyncMock(return_value=resolved))
    monkeypatch.setattr(dispatch, "register_host_log_run", AsyncMock(return_value=99))
    vehicle = AsyncMock(return_value=(True, None, None))
    monkeypatch.setitem(dispatch._SHELL_RUNNERS, RUNNER_DOCKER, vehicle)

    assert await dispatch.dispatch_execute(_task(), execution_worker_id=1) == (True, None, None)

    vehicle.assert_awaited_once_with(ANY, 1, resolved, 99)
