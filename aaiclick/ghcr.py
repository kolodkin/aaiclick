"""GHCR image references for the installed aaiclick version.

Shared by the deploy scaffolds (compose, helm) and the git-build default
Dockerfile, so the tag rule lives in one place."""

from __future__ import annotations

BASE_IMAGE_REPO = "ghcr.io/kolodkin/aaiclick"


def image_tag(version: str) -> str:
    """``v<version>``, as the release workflow tags images.

    The PEP 440 local segment (``+g<sha>.d<date>`` on dev checkouts) is dropped:
    ``+`` is not a legal Docker tag character."""
    return f"v{version.split('+', 1)[0]}"
