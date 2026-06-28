"""Tests for per-task runner dispatch (dispatch.py)."""

from __future__ import annotations

from unittest.mock import AsyncMock

from ..docker_config import BUILD_TASK_ENTRYPOINT
from ..models import RUNNER_DOCKER, RUNNER_KUBERNETES, RUNNER_SUBPROCESS, Task
from . import dispatch
from .worker import JobDispatch


def _task(entrypoint="user.module.entry", task_id=42, job_id=1) -> Task:
    return Task(id=task_id, job_id=job_id, entrypoint=entrypoint, name="test")


def test_jobdispatch_carries_entry_fields():
    d = JobDispatch(
        runner_mode="docker", image_tag="python:3.12", kubernetes_config=None,
        entry_type="shell", command=["echo", "hi"], command_env={"K": "v"},
    )
    assert d.entry_type == "shell"
    assert d.command == ["echo", "hi"]


async def test_resolve_dispatch_build_task_always_subprocess():
    """The auto-injected build task entrypoint is hardcoded to subprocess
    even when its job is in docker/kubernetes mode."""
    build_task = _task(entrypoint=BUILD_TASK_ENTRYPOINT, task_id=99)
    assert (await dispatch._resolve_dispatch(build_task)).runner_mode == RUNNER_SUBPROCESS


async def test_resolve_dispatch_reads_job_runner_mode_and_spec(monkeypatch):
    """User tasks inherit the job's runner_mode and snapshot its launch spec
    from the typed ``Job.runner`` config (not the flat columns)."""
    user_task = _task(task_id=100, job_id=200)

    class _FakeJob:
        runner_mode = RUNNER_DOCKER
        runner = {"type": "docker", "image": {"type": "prebuilt", "image_tag": "aaiclick-job:abc"}}

    class _FakeResult:
        def scalar_one_or_none(self):
            return _FakeJob()

    class _FakeSession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return None

        async def execute(self, *a, **k):
            return _FakeResult()

    monkeypatch.setattr(dispatch, "get_sql_session", lambda: _FakeSession())
    resolved = await dispatch._resolve_dispatch(user_task)
    assert resolved.runner_mode == RUNNER_DOCKER
    assert resolved.image_tag == "aaiclick-job:abc"


async def test_dispatch_execute_routes_docker_to_container_runner(monkeypatch):
    user_task = _task()
    spec = JobDispatch(RUNNER_DOCKER, "aaiclick-job:abc", None)
    monkeypatch.setattr(dispatch, "_resolve_dispatch", AsyncMock(return_value=spec))
    in_container = AsyncMock(return_value=(True, None, None, None))
    monkeypatch.setitem(dispatch._IMAGE_RUNNERS, RUNNER_DOCKER, in_container)

    await dispatch.dispatch_execute(user_task, worker_id=1)
    in_container.assert_awaited_once_with(user_task, 1, spec)


async def test_dispatch_execute_routes_subprocess_to_mp_child(monkeypatch):
    user_task = _task()
    spec = JobDispatch(RUNNER_SUBPROCESS, None, None)
    monkeypatch.setattr(dispatch, "_resolve_dispatch", AsyncMock(return_value=spec))
    in_child = AsyncMock(return_value=(True, None, None, None))
    monkeypatch.setattr(dispatch, "_run_task_in_child", in_child)

    await dispatch.dispatch_execute(user_task, worker_id=2)
    in_child.assert_awaited_once_with(user_task, 2)


async def test_dispatch_execute_routes_kubernetes_to_pod_runner(monkeypatch):
    user_task = _task()
    spec = JobDispatch(RUNNER_KUBERNETES, "aaiclick-job:abc", {"namespace": "ml"})
    monkeypatch.setattr(dispatch, "_resolve_dispatch", AsyncMock(return_value=spec))
    in_pod = AsyncMock(return_value=(True, None, None, None))
    monkeypatch.setitem(dispatch._IMAGE_RUNNERS, RUNNER_KUBERNETES, in_pod)

    await dispatch.dispatch_execute(user_task, worker_id=3)
    in_pod.assert_awaited_once_with(user_task, 3, spec)
