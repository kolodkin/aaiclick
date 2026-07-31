"""Docker runner configuration helpers.

Resolves the three-layer Docker config (run_job kwarg → RegisteredJob
default → auto-detect) into the snapshot stored on a ``Job`` row, and
houses small primitives used by the build task and host runner.
"""

from __future__ import annotations

import hashlib
import os
import re

from .execution import cli
from .models import RegisteredJob, RunnerMode
from .runner_config import (
    DockerRunner,
    ImageBuild,
    ImagePrebuilt,
    ImageSourceT,
    KubernetesRunner,
    RunnerConfigT,
)

_SHA_RE = re.compile(r"^[0-9a-f]{40}$")


class GitDetectionError(RuntimeError):
    """Raised when auto-detecting git_remote/git_sha/git_branch fails."""


async def _git(*args: str) -> str:
    rc, stdout, stderr = await cli.run("git", *args, check=False, stream=False)
    if rc != 0:
        raise GitDetectionError(f"git {' '.join(args)} failed (exit {rc}): {stderr.strip()}")
    return stdout.strip()


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


def get_registry() -> str | None:
    """The configured image registry (``AAICLICK_REGISTRY``), or None.

    The single owner of the registry-mode decision — commit-time injection,
    tag computation, launch-time pull-vs-build, and the build task body all
    read it here, so submission and workers agree on one semantics."""
    return os.environ.get("AAICLICK_REGISTRY") or None


def compute_image_tag(git_sha: str) -> str:
    """``[<registry>/]aaiclick-job:<sha>``."""
    registry = get_registry()
    prefix = f"{registry}/" if registry else ""
    return f"{prefix}aaiclick-job:{git_sha}"


def image_key(source: ImageBuild) -> str:
    """Stable sha256 identity of a build image over ``(git_remote, git_sha,
    dockerfile)``. ``git_branch`` is deliberately excluded — it does not change
    the built image, only where the SHA was found. This is the dedup key for
    ``build_tasks``."""
    parts = "\x00".join([source.git_remote, source.git_sha, source.dockerfile or ""])
    return hashlib.sha256(parts.encode("utf-8")).hexdigest()


async def resolve_image_source(
    registered: RegisteredJob | None,
    *,
    image: str | None = None,
    git_remote: str | None = None,
    git_sha: str | None = None,
    git_branch: str | None = None,
    dockerfile: str | None = None,
) -> ImageSourceT:
    """Resolve the image source a run's entry task is stamped with.

    Prebuilt when an explicit ``image`` is given (here or as the registered
    job's default); otherwise resolve the build coordinates via the existing
    precedence (explicit kwarg → registered default → git auto-detect)."""
    if image is not None:
        return ImagePrebuilt(image_tag=image)
    if registered is not None and registered.image is not None:
        return ImagePrebuilt(image_tag=registered.image)

    remote = git_remote
    if remote is None and registered is not None:
        remote = registered.git_remote
    if remote is None:
        remote = await auto_detect_git_remote()
    sha = _validate_sha(git_sha) if git_sha else await auto_detect_git_sha()
    branch = git_branch if git_branch is not None else await auto_detect_git_branch()
    dfile = dockerfile
    if dfile is None and registered is not None:
        dfile = registered.dockerfile
    return ImageBuild(git_remote=remote, git_sha=sha, git_branch=branch, dockerfile=dfile)


def resolve_runner_config(
    *,
    runner_mode: RunnerMode,
    kubernetes_config: dict | None = None,
) -> RunnerConfigT:
    """Resolve the per-run runner (cluster/vehicle) config. Image resolution
    is separate (``resolve_image_source``) — the image is a task property."""
    if runner_mode == "kubernetes":
        kc = kubernetes_config or {}
        return KubernetesRunner(
            namespace=kc.get("namespace"),
            service_account=kc.get("service_account"),
            image_pull_secret=kc.get("image_pull_secret"),
            resources=kc.get("resources"),
        )
    return DockerRunner()


def add_host_flags(env_var: str) -> list[str]:
    """``--add-host`` flags for the comma-separated entries in ``env_var``.

    Used by both the build task and the host runner to let containers
    reach services on the host (e.g. a CI-local pypiserver / ClickHouse
    on ``host.docker.internal``). Empty / unset → no flags."""
    flags: list[str] = []
    for entry in (os.environ.get(env_var) or "").split(","):
        entry = entry.strip()
        if entry:
            flags.extend(["--add-host", entry])
    return flags


def _validate_sha(sha: str) -> str:
    if not _SHA_RE.match(sha):
        raise ValueError(f"git_sha must be a 40-char lowercase hex string; got {sha!r}")
    return sha
