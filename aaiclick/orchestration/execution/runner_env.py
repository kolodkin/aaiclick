"""Environment forwarded into a remote task executor (Docker container / k8s Pod).

Shared by the docker and kubernetes runners so neither owns the other's env
logic. ``build_runner_env`` collects the always-passed framework vars plus any
operator-listed extras."""

from __future__ import annotations

import os

ALWAYS_PASSED_ENV_VARS = (
    "AAICLICK_SQL_URL",
    "AAICLICK_CH_URL",
    "AAICLICK_TASK_TIMEOUT",
    "AAICLICK_DEFAULT_PRESERVATION_MODE",
    "AAICLICK_REGISTRY",
)
"""Env vars always copied into the remote executor without opt-in.

The executor can't function without SQL and CH URLs; the timeout var must
propagate so child tasks honor the same wall-clock cap; the preservation-mode
default must propagate so subjobs the user spawns inherit the same setting;
the registry must propagate because dynamic ``commit_tasks`` runs inside
containers and build-task injection checks it at commit time — without it a
dynamic child declaring a new build image would silently skip injection and
later fail pulling an unpushed tag."""


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
