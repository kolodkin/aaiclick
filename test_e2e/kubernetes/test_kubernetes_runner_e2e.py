"""End-to-end tests for the Kubernetes runner.

Drives the full ``register-job`` → ``run-job`` → build → Pod → result path
against a real minikube cluster, a local registry, a test pypi serving the
wheel under test, and the CI ``git daemon`` (the ``kubernetes_e2e_user_repo``
fixture publishes the user repo into it). Both registration and submission go
through the ``python -m aaiclick`` CLI from the user-repo working tree, exactly
as an external user would.

The flows shared with the docker suite live in ``runner_flow``; the tests
here add only what is kubernetes-specific.

Marked ``kubernetes_e2e`` so it opts out of the default test run; the workflow
passes ``test_e2e/kubernetes/`` with ``-m kubernetes_e2e``."""

from __future__ import annotations

import subprocess

import pytest
from runner_flow import run_shell_command_env_flow, run_smoke_flow

from aaiclick.orchestration.execution.kubernetes_worker import _pod_name


@pytest.mark.kubernetes_e2e
async def test_kubernetes_runner_smoke(orch_ctx, kubernetes_e2e_user_repo):
    """Build the fixture image, run the entry task's chain as Pods, assert it
    completes and the Objects flowed through ClickHouse across Pods."""
    await run_smoke_flow("k8s_e2e_smoke", kubernetes_e2e_user_repo)


@pytest.mark.kubernetes_e2e
async def test_kubernetes_runner_shell_command_env(orch_ctx, tmp_path):
    """A shell command as a ``kubectl run`` Pod proves the per-attempt Secret
    path end to end; afterwards the Secret must be gone (``cleanup_argv``
    deletes it with the Pod)."""
    task = await run_shell_command_env_flow("k8s_e2e_shell_command_env", tmp_path)

    secret = _pod_name(task.id, task.run_epoch)
    probe = subprocess.run(["kubectl", "get", "secret", secret], capture_output=True, text=True, check=False)
    assert probe.returncode != 0 and "NotFound" in probe.stderr, probe
