"""Tests for the helm chart scaffold command."""

from __future__ import annotations

import subprocess
import sys

import pytest
import yaml

from .compose_scaffold import IMAGE_TAG_TOKEN
from .k8s_scaffold import HelmChartExists, init_helm

EXPECTED_TEMPLATES = {
    "server.yaml",
    "worker.yaml",
    "background.yaml",
    "rbac.yaml",
    "clickhouse.yaml",
    "postgres.yaml",
}


def test_init_helm_writes_chart_tree(tmp_path):
    target = tmp_path / "aaiclick-chart"
    written = init_helm(target, image_tag="v9.9.9")
    assert written == target.resolve()
    assert (target / "Chart.yaml").is_file()
    assert (target / "values.yaml").is_file()
    assert {p.name for p in (target / "templates").iterdir()} == EXPECTED_TEMPLATES


def test_init_helm_renders_version_into_chart_and_values(tmp_path):
    target = tmp_path / "aaiclick-chart"
    init_helm(target, image_tag="v9.9.9")

    chart = yaml.safe_load((target / "Chart.yaml").read_text())
    assert chart["appVersion"] == "v9.9.9"

    values = yaml.safe_load((target / "values.yaml").read_text())
    assert values["images"]["server"]["tag"] == "v9.9.9"
    assert values["images"]["worker"]["tag"] == "v9.9.9"
    assert values["images"]["worker"]["repository"] == "ghcr.io/kolodkin/aaiclick-kubectl"


def test_init_helm_leaves_no_token_anywhere(tmp_path):
    target = tmp_path / "aaiclick-chart"
    init_helm(target, image_tag="v1.0.0")
    for path in target.rglob("*.yaml"):
        assert IMAGE_TAG_TOKEN not in path.read_text(), path


def test_init_helm_refuses_overwrite(tmp_path):
    target = tmp_path / "aaiclick-chart"
    target.mkdir()
    (target / "Chart.yaml").write_text("# user's own chart\n")

    with pytest.raises(HelmChartExists, match="already exists"):
        init_helm(target)

    assert (target / "Chart.yaml").read_text() == "# user's own chart\n"


def test_init_helm_force_overwrites(tmp_path):
    target = tmp_path / "aaiclick-chart"
    target.mkdir()
    (target / "Chart.yaml").write_text("# stale\n")

    init_helm(target, image_tag="v1.0.0", force=True)

    chart = yaml.safe_load((target / "Chart.yaml").read_text())
    assert chart["name"] == "aaiclick"


def test_cli_k8s_init_writes_chart(tmp_path):
    result = subprocess.run(
        [sys.executable, "-m", "aaiclick", "k8s", "init", "--path", "chart", "--image-tag", "v1.2.3"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    assert (tmp_path / "chart" / "Chart.yaml").is_file()
