"""Fixture for the default-Dockerfile e2e: no Dockerfile, no packaging. The
entrypoint resolves from the default image's ``WORKDIR /src``."""

from __future__ import annotations

import os
import pwd
from pathlib import Path

from aaiclick.orchestration import task


@task
async def entry_task() -> dict:
    """Prove the default image's contract: cwd is the copied repo, and it is
    writable by the base image's non-root user (``COPY --chown``)."""
    Path("probe.txt").write_text("ok")
    return {"cwd": os.getcwd(), "user": pwd.getpwuid(os.getuid()).pw_name}
