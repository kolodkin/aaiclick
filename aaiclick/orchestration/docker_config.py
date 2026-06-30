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


def compute_image_tag(git_sha: str) -> str:
    """``[<registry>/]aaiclick-job:<sha>``."""
    registry = os.environ.get("AAICLICK_REGISTRY")
    prefix = f"{registry}/" if registry else ""
    return f"{prefix}aaiclick-job:{git_sha}"


def image_key(source: ImageBuild) -> str:
    """Stable sha256 identity of a build image over ``(git_remote, git_sha,
    dockerfile)``. ``git_branch`` is deliberately excluded — it does not change
    the built image, only where the SHA was found. This is the dedup key for
    ``build_tasks``."""
    parts = "\x00".join([source.git_remote, source.git_sha, source.dockerfile or ""])
    return hashlib.sha256(parts.encode("utf-8")).hexdigest()


def effective_image_tag(runner: RunnerConfigT) -> str | None:
    """The image a container/pod actually runs: prebuilt tag verbatim, or the
    computed ``aaiclick-job:<sha>`` for a build source. ``None`` for subprocess."""
    image = getattr(runner, "image", None)
    if image is None:
        return None
    if isinstance(image, ImagePrebuilt):
        return image.image_tag
    return compute_image_tag(image.git_sha)


def _registered_image(registered: RegisteredJob | None) -> str | None:
    """Prebuilt image_tag default from a RegisteredJob's runner config, if any."""
    if registered is None or registered.runner is None:
        return None
    img = registered.runner.get("image") if isinstance(registered.runner, dict) else None
    if isinstance(img, dict) and img.get("type") == "prebuilt":
        return img.get("image_tag")
    return None


async def _resolve_image_source(
    registered: RegisteredJob | None,
    *,
    image: str | None,
    git_remote: str | None,
    git_sha: str | None,
    git_branch: str | None,
    dockerfile: str | None,
) -> ImageSourceT:
    """Prebuilt when an explicit ``image`` is given (here or on the registered
    job); otherwise resolve the build coordinates via the existing precedence."""
    registered_image = _registered_image(registered)
    if image is not None:
        return ImagePrebuilt(image_tag=image)
    if registered_image is not None:
        return ImagePrebuilt(image_tag=registered_image)

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


async def resolve_runner_config(
    registered: RegisteredJob | None,
    *,
    runner_mode: RunnerMode,
    image: str | None = None,
    git_remote: str | None = None,
    git_sha: str | None = None,
    git_branch: str | None = None,
    dockerfile: str | None = None,
    kubernetes_config: dict | None = None,
) -> RunnerConfigT:
    """Resolve the per-run runner config. ``image`` (prebuilt) and the git
    fields (build) are mutually exclusive; the caller enforces that."""
    source = await _resolve_image_source(
        registered,
        image=image,
        git_remote=git_remote,
        git_sha=git_sha,
        git_branch=git_branch,
        dockerfile=dockerfile,
    )
    if runner_mode == "kubernetes":
        kc = kubernetes_config or {}
        return KubernetesRunner(
            image=source,
            namespace=kc.get("namespace"),
            service_account=kc.get("service_account"),
            image_pull_secret=kc.get("image_pull_secret"),
            resources=kc.get("resources"),
        )
    return DockerRunner(image=source)


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
