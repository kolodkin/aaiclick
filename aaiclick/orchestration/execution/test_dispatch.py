"""Tests for per-task runner dispatch (dispatch.py)."""

from __future__ import annotations

from unittest.mock import AsyncMock

from ..docker_config import BUILD_TASK_ENTRYPOINT
from ..models import RUNNER_DOCKER, RUNNER_KUBERNETES, RUNNER_SUBPROCESS, Task
from . import dispatch


def _task(entrypoint="user.module.entry", task_id=42, job_id=1) -> Task:
    return Task(id=task_id, job_id=job_id, entrypoint=entrypoint, name="test")


async def test_resolve_runner_build_task_always_subprocess():
    """The auto-injected build task entrypoint is hardcoded to subprocess
    even when its job is in docker/kubernetes mode."""
    build_task = _task(entrypoint=BUILD_TASK_ENTRYPOINT, task_id=99)
    assert await dispatch._resolve_runner(build_task) == RUNNER_SUBPROCESS


async def test_resolve_runner_reads_job_runner_mode(monkeypatch):
    """User tasks inherit the job's runner_mode."""
    user_task = _task(task_id=100, job_id=200)

    class _FakeJob:
        runner_mode = RUNNER_DOCKER

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
    assert await dispatch._resolve_runner(user_task) == RUNNER_DOCKER


async def test_dispatch_execute_routes_docker_to_container_runner(monkeypatch):
    user_task = _task()
    monkeypatch.setattr(dispatch, "_resolve_runner", AsyncMock(return_value=RUNNER_DOCKER))
    in_container = AsyncMock(return_value=(True, None, None, None))
    monkeypatch.setattr(dispatch, "_run_task_in_container", in_container)

    await dispatch.dispatch_execute(user_task, worker_id=1)
    in_container.assert_awaited_once_with(user_task, 1)


async def test_dispatch_execute_routes_subprocess_to_mp_child(monkeypatch):
    user_task = _task()
    monkeypatch.setattr(dispatch, "_resolve_runner", AsyncMock(return_value=RUNNER_SUBPROCESS))
    in_child = AsyncMock(return_value=(True, None, None, None))
    monkeypatch.setattr(dispatch, "_run_task_in_child", in_child)

    await dispatch.dispatch_execute(user_task, worker_id=2)
    in_child.assert_awaited_once_with(user_task, 2)


async def test_dispatch_execute_routes_kubernetes_to_pod_runner(monkeypatch):
    user_task = _task()
    monkeypatch.setattr(dispatch, "_resolve_runner", AsyncMock(return_value=RUNNER_KUBERNETES))
    in_pod = AsyncMock(return_value=(True, None, None, None))
    monkeypatch.setattr(dispatch, "_run_task_in_pod", in_pod)

    await dispatch.dispatch_execute(user_task, worker_id=3)
    in_pod.assert_awaited_once_with(user_task, 3)
