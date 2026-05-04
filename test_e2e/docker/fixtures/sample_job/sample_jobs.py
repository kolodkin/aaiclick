"""Sample task module installed into the docker-runner e2e test image.

The e2e container resolves entrypoints via ``importlib`` against this
module's dotted path (``sample_jobs.entry_task``). The Dockerfile installs
this directory as a real package so ``pip install /src`` puts the module
on ``sys.path`` — the same pattern a production user follows."""

from __future__ import annotations

import os


def entry_task() -> dict[str, str | None]:
    """Trivial task that proves we ran inside a container built by the
    framework's build task — returns the build-arg-emitted env vars so
    the test can assert their values match what the host submitted."""
    return {
        "git_remote": os.environ.get("GIT_REMOTE"),
        "git_sha": os.environ.get("GIT_SHA"),
        "git_branch": os.environ.get("GIT_BRANCH"),
        "build_context": os.environ.get("BUILD_CONTEXT"),
    }
