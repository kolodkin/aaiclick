"""Job module for the default-Dockerfile e2e: a user repo with nothing but
this file — no Dockerfile, no packaging. The build task writes the default
Dockerfile into the checkout, and the entrypoint resolves from its
``WORKDIR /src`` the way the host CLI resolves it from the repo root."""

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
