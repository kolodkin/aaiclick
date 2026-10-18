"""Scaffold a starter docker-compose stack for docker-runner deployments.

See docs/designs/deploy_scaffold.md — "Scaffold package and CLI", "Compose template (docker-runner setup)".

Mirrors ``docker_scaffold.py``: the framework writes a sensible starter into
the user's directory and the user owns it from there. The template ships as
package data (``templates/compose/docker-compose.yaml``) and is rendered on
write by replacing the image-tag token with the installed aaiclick version,
so the scaffold pins to the GHCR images matching the wheel the user
installed, and by filling every ``change-me-...`` credential with a random value.

Invoked via ``python -m aaiclick compose init``.
"""

from __future__ import annotations

from importlib.metadata import version
from importlib.resources import files
from pathlib import Path

from .. import ghcr
from .placeholders import ADMIN_PASSWORD_PLACEHOLDER, Scaffold, fill_placeholders

IMAGE_TAG_TOKEN = "__AAICLICK_IMAGE_TAG__"

_TEMPLATES = files("aaiclick.deploy") / "templates"


class ComposeFileExists(FileExistsError):
    """Raised when the target path already exists and ``force`` is False."""


def default_image_tag() -> str:
    """GHCR image tag matching the installed aaiclick version."""
    return ghcr.image_tag(version("aaiclick"))


def init_compose(target: Path, *, image_tag: str | None = None, force: bool = False) -> Scaffold:
    """Write the starter docker-compose file to ``target``.

    Raises :class:`ComposeFileExists` when the file already exists and
    ``force`` is False."""
    if target.exists() and not force:
        raise ComposeFileExists(f"{target} already exists. Pass --force to overwrite.")
    template = (_TEMPLATES / "compose" / "docker-compose.yaml").read_text()
    values: dict[str, str] = {}
    target.write_text(fill_placeholders(template.replace(IMAGE_TAG_TOKEN, image_tag or default_image_tag()), values))
    return Scaffold(target.resolve(), values[ADMIN_PASSWORD_PLACEHOLDER])
