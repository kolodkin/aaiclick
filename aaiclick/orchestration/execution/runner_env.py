"""Environment forwarded into a remote task executor (Docker container / k8s Pod).

Shared by the docker and kubernetes runners so neither owns the other's env
logic. ``build_runner_env`` collects the always-passed framework vars plus any
operator-listed extras."""

from __future__ import annotations

import os
from typing import Literal

# --- worker runner: how this worker launches container tasks ---------------
RUNNER_DOCKER = "docker"
RUNNER_KUBERNETES = "kubernetes"
WorkerRunner = Literal["docker", "kubernetes"]
WORKER_RUNNERS: list[WorkerRunner] = [RUNNER_DOCKER, RUNNER_KUBERNETES]
ENV_WORKER_RUNNER = "AAICLICK_RUNNER"
"""Deployment-level choice, set once per worker (the compose scaffold sets
``docker``, the helm chart ``kubernetes``). Unset means the worker runs
subprocess tasks only. Tasks without an ``image_source`` run as a host
subprocess whatever the value."""


def get_worker_runner() -> WorkerRunner | None:
    """The worker's container runner from ``AAICLICK_RUNNER``, or None when unset."""
    value = os.environ.get(ENV_WORKER_RUNNER) or None
    if value is None:
        return None
    if value not in WORKER_RUNNERS:
        raise ValueError(f"{ENV_WORKER_RUNNER}={value!r} is not one of {', '.join(WORKER_RUNNERS)}")
    return value  # type: ignore[return-value]


def validate_worker_runner() -> None:
    """Fail worker startup on an inconsistent runner env.

    Raises ``ValueError`` for an unknown ``AAICLICK_RUNNER`` value, and for
    ``kubernetes`` combined with ``AAICLICK_LOCAL_BUILD`` — a locally built
    image lives only in this host's daemon, which the cluster cannot pull."""
    runner = get_worker_runner()
    if runner == RUNNER_KUBERNETES and os.environ.get("AAICLICK_LOCAL_BUILD"):
        raise ValueError(
            f"{ENV_WORKER_RUNNER}=kubernetes cannot be combined with AAICLICK_LOCAL_BUILD: "
            "the cluster cannot pull from this worker's docker daemon; set AAICLICK_REGISTRY instead"
        )


ALWAYS_PASSED_ENV_VARS = (
    "AAICLICK_SQL_URL",
    "AAICLICK_CH_URL",
    "AAICLICK_TASK_TIMEOUT",
    "AAICLICK_DEFAULT_PRESERVATION_MODE",
    "AAICLICK_REGISTRY",
    "AAICLICK_TASK_LOGS",
)
"""Env vars always copied into the remote executor without opt-in.

The executor can't function without SQL and CH URLs; the timeout var must
propagate so child tasks honor the same wall-clock cap; the preservation-mode
default must propagate so subjobs the user spawns inherit the same setting;
the registry must propagate because dynamic ``commit_tasks`` runs inside
containers and ``compute_image_tag`` derives build-image tags from it; the
task-log destination must propagate so in-container capture writes where the
worker does."""


def build_runner_env() -> dict[str, str]:
    """Collect env vars to forward into the remote executor.

    Always-passed vars + comma-separated extras from
    ``AAICLICK_PASSTHROUGH_ENV``."""
    env: dict[str, str] = {}
    for key in ALWAYS_PASSED_ENV_VARS:
        value = os.environ.get(key)
        if value is not None:
            env[key] = value

    extras = os.environ.get("AAICLICK_PASSTHROUGH_ENV", "")
    for raw in extras.split(","):
        key = raw.strip()
        if key and key in os.environ:
            env[key] = os.environ[key]
    return env
