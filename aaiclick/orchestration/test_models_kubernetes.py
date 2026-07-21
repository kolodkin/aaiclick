"""Schema tests for the Kubernetes runner mode and models."""

from __future__ import annotations

from typing import get_args

from aaiclick.orchestration.models import (
    RUNNER_KUBERNETES,
    RUNNER_MODES,
    Job,
    RegisteredJob,
    RemoteTaskResult,
    RunnerMode,
)


def test_kubernetes_is_a_runner_mode():
    assert RUNNER_KUBERNETES == "kubernetes"
    assert "kubernetes" in get_args(RunnerMode)
    assert RUNNER_KUBERNETES in RUNNER_MODES


def test_jobs_carry_kubernetes_config_in_runner():
    job = Job(
        name="j",
        run_type="MANUAL",
        runner_mode="kubernetes",
        runner={"type": "kubernetes", "namespace": "ml"},
    )
    assert job.runner == {"type": "kubernetes", "namespace": "ml"}
    reg = RegisteredJob(name="r", entrypoint="m.f")
    assert reg.kubernetes_config is None  # defaults to None


def test_remote_task_result_fields():
    row = RemoteTaskResult(
        task_id=42,
        run_epoch=3,
        success=True,
        result_ref={"native_value": 1},
        error=None,
    )
    assert row.task_id == 42
    assert row.run_epoch == 3
    assert row.success is True
    assert row.result_ref == {"native_value": 1}
    assert RemoteTaskResult.__tablename__ == "remote_task_results"
