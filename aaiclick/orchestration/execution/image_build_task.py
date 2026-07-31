"""Registry-mode image-build task body.

An ordinary module task injected at commit points (see
``orchestration.image_injection``) for every distinct build-source image in a
docker/kubernetes job. It runs on the dispatching worker host
(``image_source=NULL`` ⇒ subprocess vehicle) because it needs the docker CLI
and daemon socket. The body is pull-first via ``build_image_to_tag``: pull
from the registry (someone already pushed this SHA → done), else clone +
build + push. Cross-job dedup is the registry itself — a lost race
double-builds, which is wasteful but correct.
"""

from __future__ import annotations

import os

from ..docker_config import compute_image_tag
from ..runner_config import ImageBuild
from .docker_build import build_image_to_tag

IMAGE_BUILD_ENTRYPOINT = "aaiclick.orchestration.execution.image_build_task.run_image_build"


def is_image_build_task(entrypoint: str) -> bool:
    """True for the injected image-build task; drives UI styling and the
    per-job injection dedup lookup."""
    return entrypoint == IMAGE_BUILD_ENTRYPOINT


def build_task_name(git_sha: str) -> str:
    """Display name for an injected build task."""
    return f"build-image:{git_sha[:8]}"


async def run_image_build(
    *,
    image_key: str,
    git_remote: str,
    git_sha: str,
    git_branch: str | None = None,
    dockerfile: str | None = None,
) -> None:
    """Ensure the image for these build coordinates is pushed to the registry.

    ``image_key`` is carried in kwargs for the injection dedup lookup; the
    body itself only needs the build coordinates. Build tasks are only
    injected when submission saw ``AAICLICK_REGISTRY``; a worker without it
    would build an image no other host could use, so fail loudly on the
    env-layer mismatch instead.
    """
    if not os.environ.get("AAICLICK_REGISTRY"):
        raise RuntimeError(
            "image build task requires AAICLICK_REGISTRY on the worker; "
            "submission-side and worker-side env must agree "
            "(see docs/designs/orchestration.md, 'Image source')"
        )
    source = ImageBuild(git_remote=git_remote, git_sha=git_sha, git_branch=git_branch, dockerfile=dockerfile)
    await build_image_to_tag(source, compute_image_tag(git_sha))
