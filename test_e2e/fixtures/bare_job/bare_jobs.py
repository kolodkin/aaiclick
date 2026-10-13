"""Fixture for the default-Dockerfile e2e: no Dockerfile, no packaging, a
``requirements.txt`` naming a package the base image lacks. The entrypoint
resolves from the default image's ``WORKDIR /src``."""

from __future__ import annotations

import importlib.metadata
import os
import pwd
from pathlib import Path

from aaiclick.orchestration import task


@task
async def entry_task() -> dict:
    """Prove the default image's contract: cwd is the copied repo, it is
    writable by the base image's non-root user (``COPY --chown``), and
    ``requirements.txt`` was installed."""
    Path("probe.txt").write_text("ok")
    return {
        "cwd": os.getcwd(),
        "user": pwd.getpwuid(os.getuid()).pw_name,
        "tabulate": importlib.metadata.version("tabulate"),
    }
