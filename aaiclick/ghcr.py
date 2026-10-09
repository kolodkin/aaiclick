"""GHCR image references for the installed aaiclick version.

Shared by the deploy scaffolds (compose, helm) and the git-build default
Dockerfile, so the tag rule lives in one place."""

from __future__ import annotations

BASE_IMAGE_REPO = "ghcr.io/kolodkin/aaiclick"


def image_tag(version: str) -> str:
    """``v<version>``, as the release workflow tags images.

    The PEP 440 local segment (``+g<sha>.d<date>`` on an install from a git
    checkout: a developer's worker, or CI running the e2e from source) is
    dropped: ``+`` is not a legal Docker tag character. No image exists for such
    a version, but a valid reference lets the rendered Dockerfile or compose
    file parse and run against ``AAICLICK_BASE_IMAGE`` or a locally tagged image
    — the worker fails on "tag not found" instead of "invalid reference
    format"."""
    return f"v{version.split('+', 1)[0]}"
