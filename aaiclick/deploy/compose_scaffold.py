"""Scaffold a starter docker-compose stack for docker-runner deployments.

Mirrors ``docker_scaffold.py``: the framework writes a sensible starter into
the user's directory and the user owns it from there. The template ships as
package data (``templates/compose/docker-compose.yaml``) and is rendered on
write by replacing the image-tag token with the installed aaiclick version,
so the scaffold pins to the GHCR images matching the wheel the user
installed.

Invoked via ``python -m aaiclick compose init``."""

from __future__ import annotations

from importlib.metadata import version
from importlib.resources import files
from pathlib import Path

IMAGE_TAG_TOKEN = "__AAICLICK_IMAGE_TAG__"

_TEMPLATES = files("aaiclick.deploy") / "templates"


class ComposeFileExists(FileExistsError):
    """Raised when the target path already exists and ``force`` is False."""


def default_image_tag() -> str:
    """GHCR image tag matching the installed aaiclick version."""
    return f"v{version('aaiclick')}"


def init_compose(target: Path, *, image_tag: str | None = None, force: bool = False) -> Path:
    """Write the starter docker-compose file to ``target``.

    Returns the resolved target path. Raises :class:`ComposeFileExists`
    when the file already exists and ``force`` is False."""
    if target.exists() and not force:
        raise ComposeFileExists(f"{target} already exists. Pass --force to overwrite.")
    template = (_TEMPLATES / "compose" / "docker-compose.yaml").read_text()
    target.write_text(template.replace(IMAGE_TAG_TOKEN, image_tag or default_image_tag()))
    return target.resolve()
