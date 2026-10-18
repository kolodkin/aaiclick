"""Scaffold a starter helm chart for kubernetes deployments.

See docs/designs/deploy_scaffold.md — "Scaffold package and CLI", "Helm chart template (kubernetes setup)".

Mirrors ``compose_scaffold.py``: the chart ships as package data
(``templates/helm/aaiclick``) and every file is rendered on write by
replacing the image-tag token, so ``Chart.yaml``'s ``appVersion`` and the
``values.yaml`` image tags pin to the installed aaiclick version; every
``change-me-...`` credential is filled with a random value.

Invoked via ``python -m aaiclick k8s init``.
"""

from __future__ import annotations

import sys

if sys.version_info >= (3, 11):
    from importlib.resources.abc import Traversable
else:
    from importlib.abc import Traversable

from importlib.resources import files
from pathlib import Path

from .compose_scaffold import IMAGE_TAG_TOKEN, default_image_tag
from .placeholders import ADMIN_PASSWORD_PLACEHOLDER, Scaffold, fill_placeholders

_CHART_ROOT = files("aaiclick.deploy") / "templates" / "helm" / "aaiclick"


class HelmChartExists(FileExistsError):
    """Raised when the target directory already exists and ``force`` is False."""


def _render_tree(src: Traversable, dst: Path, image_tag: str, values: dict[str, str]) -> None:
    dst.mkdir(parents=True, exist_ok=True)
    for entry in src.iterdir():
        if entry.is_dir():
            _render_tree(entry, dst / entry.name, image_tag, values)
        else:
            (dst / entry.name).write_text(
                fill_placeholders(entry.read_text().replace(IMAGE_TAG_TOKEN, image_tag), values)
            )


def init_helm(target_dir: Path, *, image_tag: str | None = None, force: bool = False) -> Scaffold:
    """Write the starter helm chart into ``target_dir``.

    Raises :class:`HelmChartExists` when the directory already exists and
    ``force`` is False."""
    if target_dir.exists() and not force:
        raise HelmChartExists(f"{target_dir} already exists. Pass --force to overwrite.")
    values: dict[str, str] = {}
    _render_tree(_CHART_ROOT, target_dir, image_tag or default_image_tag(), values)
    return Scaffold(target_dir.resolve(), values[ADMIN_PASSWORD_PLACEHOLDER])
