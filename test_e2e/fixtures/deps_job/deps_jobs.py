"""Fixture for the requirements-manifest e2e: no Dockerfile, a ``requirements.txt``
the default build image installs. ``tabulate`` is not in ``aaiclick[all]``."""

from __future__ import annotations

import importlib.metadata

from aaiclick.orchestration import task


@task
async def entry_task() -> dict:
    """Prove the default image installed ``requirements.txt``."""
    return {"tabulate": importlib.metadata.version("tabulate")}
