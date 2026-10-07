"""Image-source resolution and docker build helpers.

Resolves a run's image source (run kwarg → RegisteredJob default → git
auto-detect) for stamping onto the entry task, and houses small primitives
used by the build task and host runner.
"""

from __future__ import annotations

import hashlib
import os
from typing import Literal

from .execution import cli
from .models import RegisteredJob
from .runner_config import (
    IMAGE_BUILD,
    IMAGE_PREBUILT,
    ImageBuild,
    ImageKind,
    ImagePrebuilt,
    ImageSourceT,
    build_requested,
)

BUILD_MODE_REGISTRY = "registry"
BUILD_MODE_LOCAL = "local"
BuildMode = Literal["registry", "local"]


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

    Owns the tag prefix and the pull/push decisions; ``get_build_mode`` owns
    the registry-vs-local choice a build task makes."""
    return os.environ.get("AAICLICK_REGISTRY") or None


def get_build_mode() -> BuildMode:
    """Which of the two mutually exclusive build modes the worker runs in.

    ``AAICLICK_REGISTRY=<host>`` → ``"registry"``: the build task pushes and
    every host pulls. ``AAICLICK_LOCAL_BUILD=<any>`` → ``"local"``: the build
    task leaves the image in this host's daemon (single-host deployments).
    Both or neither set is a configuration error, raised here so the build
    task fails loudly rather than guessing."""
    registry = get_registry() is not None
    local = bool(os.environ.get("AAICLICK_LOCAL_BUILD"))
    if registry and local:
        raise RuntimeError("AAICLICK_REGISTRY and AAICLICK_LOCAL_BUILD are mutually exclusive; set exactly one")
    if registry:
        return BUILD_MODE_REGISTRY
    if local:
        return BUILD_MODE_LOCAL
    raise RuntimeError(
        "no image build mode configured: set AAICLICK_REGISTRY=<host> so built images are pushed "
        "for every worker to pull, or AAICLICK_LOCAL_BUILD=1 to keep them in this host's docker daemon"
    )


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


def requested_image_kind(
    registered: RegisteredJob | None,
    *,
    image: str | None = None,
    build: bool = False,
    git_remote: str | None = None,
    git_sha: str | None = None,
    git_branch: str | None = None,
    dockerfile: str | None = None,
) -> ImageKind | None:
    """Which image source a run asks for, or None for a host subprocess.
    Pure — no git, no I/O — so callers can gate on it before resolving.

    First match wins: run ``image`` → prebuilt; run ``build`` or any build
    modifier (``git_*`` / ``dockerfile``) → build; registration ``image`` →
    prebuilt; registration ``build`` → build; else None."""
    if image is not None:
        return IMAGE_PREBUILT
    if build_requested(build, git_remote, git_sha, git_branch, dockerfile):
        return IMAGE_BUILD
    if registered is None:
        return None
    if registered.image is not None:
        return IMAGE_PREBUILT
    return IMAGE_BUILD if registered.build else None


async def resolve_image_source(
    registered: RegisteredJob | None,
    *,
    image: str | None = None,
    build: bool = False,
    git_remote: str | None = None,
    git_sha: str | None = None,
    git_branch: str | None = None,
    dockerfile: str | None = None,
) -> ImageSourceT | None:
    """Resolve the image source a run's entry task is stamped with, or None
    for a host subprocess (``requested_image_kind`` decides which). Build
    coordinates fall through run kwarg → registration default → git
    auto-detect."""
    kind = requested_image_kind(
        registered,
        image=image,
        build=build,
        git_remote=git_remote,
        git_sha=git_sha,
        git_branch=git_branch,
        dockerfile=dockerfile,
    )
    if kind is None:
        return None
    if kind == IMAGE_PREBUILT:
        tag = image if image is not None else registered.image if registered is not None else None
        if tag is None:  # unreachable: requested_image_kind saw a tag
            raise RuntimeError("prebuilt image source without an image tag")
        return ImagePrebuilt(image_tag=tag)

    remote = git_remote
    if remote is None and registered is not None:
        remote = registered.git_remote
    if remote is None:
        remote = await auto_detect_git_remote()
    sha = git_sha or await auto_detect_git_sha()
    branch = git_branch if git_branch is not None else await auto_detect_git_branch()
    dfile = dockerfile
    if dfile is None and registered is not None:
        dfile = registered.dockerfile
    return ImageBuild(git_remote=remote, git_sha=sha, git_branch=branch, dockerfile=dfile)


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
