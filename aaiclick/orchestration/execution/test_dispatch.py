"""Tests for per-task runner dispatch (dispatch.py)."""

from __future__ import annotations

from unittest.mock import ANY, AsyncMock

import pytest

from ..kubernetes_config import ENV_NAMESPACE, KubernetesConfig
from ..models import Task
from ..runner_config import ImageBuild, ImagePrebuilt, dump_image_source
from . import dispatch
from .execution_worker import JobDispatch
from .runner import ShellSpec
from .runner_env import ENV_WORKER_RUNNER, RUNNER_DOCKER, RUNNER_KUBERNETES

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


async def test_null_image_source_dispatches_subprocess_whatever_the_worker_runner(monkeypatch):
    monkeypatch.setenv(ENV_WORKER_RUNNER, "kubernetes")
    resolved = await dispatch._resolve_dispatch(_task(image_source=None))
    assert resolved.runner is None
    assert resolved.image_source is None


async def test_resolve_dispatch_without_worker_runner_raises(monkeypatch):
    monkeypatch.delenv(ENV_WORKER_RUNNER, raising=False)
    with pytest.raises(dispatch.DispatchError, match="AAICLICK_RUNNER"):
        await dispatch._resolve_dispatch(_task(image_source=PREBUILT))


@pytest.mark.parametrize(
    "runner, resources",
    [
        pytest.param(RUNNER_DOCKER, None, id="docker"),
        pytest.param(RUNNER_KUBERNETES, {"limits": {"cpu": "2"}}, id="kubernetes"),
    ],
)
async def test_resolve_dispatch_uses_worker_runner_and_job_resources(monkeypatch, runner, resources):
    monkeypatch.setenv(ENV_WORKER_RUNNER, runner)
    monkeypatch.setenv(ENV_NAMESPACE, "ml")
    job = type("FakeJob", (), {"resources": resources})()
    monkeypatch.setattr(dispatch, "get_sql_session", lambda: _FakeSession(job))
    resolved = await dispatch._resolve_dispatch(_task(image_source=PREBUILT))
    assert resolved.runner == runner
    assert isinstance(resolved.image_source, ImagePrebuilt)
    if runner == RUNNER_KUBERNETES:
        assert resolved.pod_config == KubernetesConfig("ml", None, None, resources)
    else:
        assert resolved.pod_config is None


@pytest.mark.parametrize("runner", [RUNNER_DOCKER, RUNNER_KUBERNETES])
async def test_dispatch_execute_routes_image_runner(monkeypatch, runner):
    user_task = _task()
    spec = JobDispatch(runner, None)
    monkeypatch.setattr(dispatch, "_resolve_dispatch", AsyncMock(return_value=spec))
    handler = AsyncMock(return_value=(True, None, None, None))
    monkeypatch.setitem(dispatch._IMAGE_RUNNERS, runner, handler)

    await dispatch.dispatch_execute(user_task, execution_worker_id=1)
    handler.assert_awaited_once_with(user_task, 1, spec, None)


async def test_dispatch_execute_removes_shell_env_file(monkeypatch, tmp_path):
    """The docker shell wrapper's ``--env-file`` holds ``command_env`` values;
    the dispatcher owns its lifetime, so it is gone once the child returns."""
    env_file = tmp_path / "task.env"
    env_file.write_text("K=v\n")
    spec = ShellSpec(["docker", "run", "--env-file", str(env_file), "img", "true"], None, env_file=str(env_file))
    monkeypatch.setattr(
        dispatch, "_resolve_dispatch", AsyncMock(return_value=JobDispatch(RUNNER_DOCKER, None, "shell"))
    )
    monkeypatch.setattr(dispatch, "build_shell_spec", AsyncMock(return_value=spec))
    child = AsyncMock(return_value=(True, None, None))
    monkeypatch.setattr(dispatch, "_run_task_in_child", child)

    assert await dispatch.dispatch_execute(_task(), execution_worker_id=1) == (True, None, None)

    child.assert_awaited_once_with(ANY, 1, shell_spec=spec)
    assert not env_file.exists()
