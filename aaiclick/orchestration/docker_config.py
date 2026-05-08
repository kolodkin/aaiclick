"""Docker runner configuration helpers.

Resolves the three-layer Docker config (run_job kwarg → RegisteredJob
default → auto-detect) into the snapshot stored on a ``Job`` row, and
houses small primitives used by the build task and host runner.
"""

from __future__ import annotations

import asyncio
import os
import re
from typing import NamedTuple

from .models import RegisteredJob

BUILD_TASK_ENTRYPOINT = "aaiclick.orchestration.execution.docker_build.build_image"
"""Entrypoint of the auto-injected build task. Used by `_resolve_runner`
to keep the build task on the host (subprocess) runner regardless of the
job's configured runner_mode."""


_SHA_RE = re.compile(r"^[0-9a-f]{40}$")


class DockerJobConfig(NamedTuple):
    """Snapshot of Docker config resolved at submission time.

    Maps 1:1 to the new ``Job`` columns. ``run_job`` writes these onto
    the Job row; the build task and host runner read them back."""

    git_remote: str
    git_sha: str
    git_branch: str | None
    build_context: str | None
    dockerfile: str | None
    image_tag: str


class GitDetectionError(RuntimeError):
    """Raised when auto-detecting git_remote/git_sha/git_branch fails."""


async def _git(*args: str) -> str:
    proc = await asyncio.create_subprocess_exec(
        "git",
        *args,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await proc.communicate()
    if proc.returncode != 0:
        raise GitDetectionError(f"git {' '.join(args)} failed (exit {proc.returncode}): {stderr.decode().strip()}")
    return stdout.decode().strip()


async def auto_detect_git_remote() -> str:
    """Read the working directory's ``origin`` remote URL."""
    return await _git("config", "--get", "remote.origin.url")


async def auto_detect_git_sha() -> str:
    """Read the working directory's ``HEAD`` SHA, refusing dirty trees
    and unpushed commits.

    A docker job is reproducible only when the SHA exists on the remote
    that the build task will clone from — otherwise the build will fail
    halfway through with a confusing 'commit not found' error."""
    status = await _git("status", "--porcelain")
    if status:
        raise GitDetectionError(
            "git working tree is dirty; commit or stash before submitting "
            "a docker-runner job, or pass git_sha= explicitly"
        )
    sha = await _git("rev-parse", "HEAD")
    try:
        await _git("branch", "--remotes", "--contains", sha)
    except GitDetectionError as e:
        raise GitDetectionError(
            f"HEAD ({sha[:8]}) is not pushed to any remote; push the "
            "branch before submitting a docker-runner job, or pass "
            "git_sha= explicitly"
        ) from e
    return sha


async def auto_detect_git_branch() -> str | None:
    """Read the current branch name, returning ``None`` on detached HEAD."""
    branch = await _git("rev-parse", "--abbrev-ref", "HEAD")
    return None if branch == "HEAD" else branch


def compute_image_tag(git_sha: str) -> str:
    """``[<registry>/]aaiclick-job:<sha>``."""
    registry = os.environ.get("AAICLICK_DOCKER_REGISTRY")
    prefix = f"{registry}/" if registry else ""
    return f"{prefix}aaiclick-job:{git_sha}"


def _validate_sha(sha: str) -> str:
    if not _SHA_RE.match(sha):
        raise ValueError(f"git_sha must be a 40-char lowercase hex string; got {sha!r}")
    return sha


async def resolve_docker_config(
    registered: RegisteredJob | None,
    *,
    git_remote: str | None = None,
    git_sha: str | None = None,
    git_branch: str | None = None,
    build_context: str | None = None,
    dockerfile: str | None = None,
) -> DockerJobConfig:
    """Resolve docker config for a single ``run_job`` call.

    Precedence (highest first) for each field:

    1. Explicit ``run_job`` kwarg (passed in to this function)
    2. ``registered`` default (when set on the RegisteredJob row)
    3. Auto-detect rule (where one applies)

    Returns the snapshot to write onto the Job row."""
    remote = git_remote
    if remote is None and registered is not None:
        remote = registered.git_remote
    if remote is None:
        remote = await auto_detect_git_remote()

    sha = _validate_sha(git_sha) if git_sha else await auto_detect_git_sha()

    branch = git_branch
    if branch is None:
        branch = await auto_detect_git_branch()

    ctx = build_context
    if ctx is None and registered is not None:
        ctx = registered.build_context

    dfile = dockerfile
    if dfile is None and registered is not None:
        dfile = registered.dockerfile

    return DockerJobConfig(
        git_remote=remote,
        git_sha=sha,
        git_branch=branch,
        build_context=ctx,
        dockerfile=dfile,
        image_tag=compute_image_tag(sha),
    )
