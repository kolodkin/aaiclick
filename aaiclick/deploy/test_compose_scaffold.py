"""Tests for the docker-compose scaffold command."""

from __future__ import annotations

import subprocess
import sys
import xml.etree.ElementTree as ET

import pytest
import yaml

from .compose_scaffold import (
    IMAGE_TAG_TOKEN,
    ComposeFileExists,
    default_image_tag,
    init_compose,
)

EXPECTED_SERVICES = {"clickhouse", "postgres", "registry", "buildkitd", "migrate", "server", "worker", "background"}


def test_init_compose_writes_rendered_file(tmp_path):
    target = tmp_path / "docker-compose.yaml"
    written = init_compose(target, image_tag="v9.9.9")
    assert written.path == target.resolve()
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


def test_init_compose_refuses_to_overwrite_an_existing_env(tmp_path):
    (tmp_path / ".env").write_text("MINE=1\n")

    with pytest.raises(ComposeFileExists, match=".env already exists"):
        init_compose(tmp_path / "docker-compose.yaml")

    assert (tmp_path / ".env").read_text() == "MINE=1\n"
    assert not (tmp_path / "docker-compose.yaml").exists()


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


def test_init_compose_server_carries_auth_credentials(tmp_path):
    """Distributed URLs enforce auth, so the server needs a signing secret and
    a seed admin or the stack comes up healthy but cannot be logged into."""
    target = tmp_path / "docker-compose.yaml"
    init_compose(target, image_tag="v1.0.0")
    env = yaml.safe_load(target.read_text())["services"]["server"]["environment"]
    assert env["AAICLICK_JWT_SECRET"]
    assert env["AAICLICK_ADMIN_PASSWORD"]


def test_init_compose_builds_on_rootless_buildkitd(tmp_path):
    """The worker dispatches container tasks to the mounted host daemon, but
    builds run on a rootless buildkitd, never that daemon."""
    target = tmp_path / "docker-compose.yaml"
    init_compose(target, image_tag="v1.0.0")
    parsed = yaml.safe_load(target.read_text())
    buildkitd = parsed["services"]["buildkitd"]
    assert buildkitd["image"].startswith("moby/buildkit:v") and buildkitd["image"].endswith("-rootless")
    assert {"seccomp=unconfined", "apparmor=unconfined"} <= set(buildkitd["security_opt"])
    assert "docker.sock" not in str(buildkitd)
    worker_env = parsed["services"]["worker"]["environment"]
    assert worker_env["AAICLICK_RUNNER"] == "docker"
    assert worker_env["AAICLICK_BUILDKIT_ADDR"] == "tcp://buildkitd:1234"
    toml = parsed["configs"]["buildkitd_toml"]["content"]
    assert '[registry."host.docker.internal:5000"]' in toml and "http = true" in toml


def test_init_compose_ships_the_s3_profile_switched_off(tmp_path):
    """ClickHouse gets the users.d file that documents the S3 opt-in, but the
    scaffold has no bucket credentials, so the profile itself stays commented out."""
    target = tmp_path / "docker-compose.yaml"
    init_compose(target, image_tag="v1.0.0")
    parsed = yaml.safe_load(target.read_text())

    mounts = {c["target"]: c["source"] for c in parsed["services"]["clickhouse"]["configs"]}
    content = parsed["configs"][mounts["/etc/clickhouse-server/users.d/s3.xml"]]["content"]
    assert "s3_allow_server_credentials_in_user_queries" in content
    assert list(ET.fromstring(content)) == []


def test_init_compose_ships_github_app_credentials_switched_off(tmp_path):
    """The worker documents the GitHub App opt-in for private-repo builds, but
    the scaffold has no key, so the mount and both settings stay commented out."""
    target = tmp_path / "docker-compose.yaml"
    init_compose(target, image_tag="v1.0.0")
    text = target.read_text()
    assert "# - ./secrets/github-app.pem:/run/secrets/github-app.pem:ro" in text
    assert '# AAICLICK_GITHUB_APP_ID: "..."' in text
    assert "# AAICLICK_GITHUB_APP_PEM_FILE: /run/secrets/github-app.pem" in text
    worker = yaml.safe_load(text)["services"]["worker"]
    assert worker["volumes"] == ["/var/run/docker.sock:/var/run/docker.sock"]
    assert not any(key.startswith("AAICLICK_GITHUB_APP") for key in worker["environment"])
