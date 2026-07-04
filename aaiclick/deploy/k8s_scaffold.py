"""Scaffold a starter helm chart for kubernetes deployments.

Mirrors ``compose_scaffold.py``: the chart ships as package data
(``templates/helm/aaiclick``) and every file is rendered on write by
replacing the image-tag token, so ``Chart.yaml``'s ``appVersion`` and the
``values.yaml`` image tags pin to the installed aaiclick version.

Invoked via ``python -m aaiclick k8s init``."""

from __future__ import annotations

from importlib.abc import Traversable
from importlib.resources import files
from pathlib import Path

from .compose_scaffold import IMAGE_TAG_TOKEN, default_image_tag

_CHART_ROOT = files("aaiclick.deploy") / "templates" / "helm" / "aaiclick"


class HelmChartExists(FileExistsError):
    """Raised when the target directory already exists and ``force`` is False."""


def _render_tree(src: Traversable, dst: Path, image_tag: str) -> None:
    dst.mkdir(parents=True, exist_ok=True)
    for entry in src.iterdir():
        if entry.is_dir():
            _render_tree(entry, dst / entry.name, image_tag)
        else:
            (dst / entry.name).write_text(entry.read_text().replace(IMAGE_TAG_TOKEN, image_tag))


def init_helm(target_dir: Path, *, image_tag: str | None = None, force: bool = False) -> Path:
    """Write the starter helm chart into ``target_dir``.

    Returns the resolved target path. Raises :class:`HelmChartExists`
    when the directory already exists and ``force`` is False."""
    if target_dir.exists() and not force:
        raise HelmChartExists(f"{target_dir} already exists. Pass --force to overwrite.")
    _render_tree(_CHART_ROOT, target_dir, image_tag or default_image_tag())
    return target_dir.resolve()
