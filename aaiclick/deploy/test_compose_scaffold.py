"""Tests for the docker-compose scaffold command."""

from __future__ import annotations

import subprocess
import sys

import pytest
import yaml

from .compose_scaffold import (
    IMAGE_TAG_TOKEN,
    ComposeFileExists,
    default_image_tag,
    init_compose,
)

EXPECTED_SERVICES = {"clickhouse", "postgres", "registry", "migrate", "server", "worker", "background"}


def test_init_compose_writes_rendered_file(tmp_path):
    target = tmp_path / "docker-compose.yaml"
    written = init_compose(target, image_tag="v9.9.9")
    assert written == target.resolve()
    content = target.read_text()
    assert IMAGE_TAG_TOKEN not in content
    assert "ghcr.io/kolodkin/aaiclick:v9.9.9" in content
    assert "ghcr.io/kolodkin/aaiclick-docker:v9.9.9" in content


def test_init_compose_output_is_valid_yaml_with_expected_services(tmp_path):
    target = tmp_path / "docker-compose.yaml"
    init_compose(target, image_tag="v1.0.0")
    parsed = yaml.safe_load(target.read_text())
    assert set(parsed["services"]) == EXPECTED_SERVICES
    assert "/var/run/docker.sock:/var/run/docker.sock" in parsed["services"]["worker"]["volumes"]


def test_init_compose_defaults_tag_to_installed_version(tmp_path):
    target = tmp_path / "docker-compose.yaml"
    init_compose(target)
    assert f"ghcr.io/kolodkin/aaiclick:{default_image_tag()}" in target.read_text()


def test_init_compose_refuses_overwrite(tmp_path):
    target = tmp_path / "docker-compose.yaml"
    target.write_text("# user's own compose file\n")

    with pytest.raises(ComposeFileExists, match="already exists"):
        init_compose(target)

    assert target.read_text() == "# user's own compose file\n"


def test_init_compose_force_overwrites(tmp_path):
    target = tmp_path / "docker-compose.yaml"
    target.write_text("# stale\n")

    init_compose(target, image_tag="v1.0.0", force=True)

    assert "ghcr.io/kolodkin/aaiclick:v1.0.0" in target.read_text()


def test_cli_compose_init_writes_file(tmp_path):
    result = subprocess.run(
        [sys.executable, "-m", "aaiclick", "compose", "init", "--path", "stack.yaml", "--image-tag", "v1.2.3"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    assert "Wrote" in result.stdout
    assert "ghcr.io/kolodkin/aaiclick:v1.2.3" in (tmp_path / "stack.yaml").read_text()


def test_cli_compose_init_existing_file_exits_nonzero(tmp_path):
    (tmp_path / "docker-compose.yaml").write_text("# mine\n")
    result = subprocess.run(
        [sys.executable, "-m", "aaiclick", "compose", "init"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 1
    assert "already exists" in result.stderr
