"""Fixture for the build-secret leak e2e: returns the environment the
Dockerfile's ``RUN`` step saw."""

from __future__ import annotations

from pathlib import Path

from aaiclick.orchestration import task


@task
async def read_build_env() -> str:
    return Path("/build-env.txt").read_text()
