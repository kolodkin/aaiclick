"""Tests for the helm chart scaffold command."""

from __future__ import annotations

import shutil
import subprocess
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

import pytest
import yaml

from .compose_scaffold import IMAGE_TAG_TOKEN
from .k8s_scaffold import HelmChartExists, init_helm
from .placeholders import PLACEHOLDER_MARKER, Scaffold

EXPECTED_TEMPLATES = {
    "_helpers.tpl",
    "server.yaml",
    "worker.yaml",
    "background.yaml",
    "rbac.yaml",
    "buildkitd.yaml",
    "clickhouse.yaml",
    "postgres.yaml",
}

needs_helm = pytest.mark.skipif(shutil.which("helm") is None, reason="needs the helm binary")


def _render(chart: Path, *sets: str) -> list[dict]:
    """``helm template`` the chart with in-chart databases; the rendered documents."""
    args = [arg for item in ("devDependencies.enabled=true", *sets) for arg in ("--set", item)]
    result = subprocess.run(["helm", "template", "x", str(chart), *args], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    return [d for d in yaml.safe_load_all(result.stdout) if d]


def _doc(docs: list[dict], kind: str, name: str) -> dict:
    (doc,) = [d for d in docs if d["kind"] == kind and d["metadata"]["name"] == name]
    return doc


def _env(deployment: dict) -> dict[str, str | None]:
    return {e["name"]: e.get("value") for e in deployment["spec"]["template"]["spec"]["containers"][0]["env"]}


@pytest.fixture(scope="module")
def default_render(tmp_path_factory) -> tuple[Scaffold, list[dict]]:
    """One scaffold rendered with default values, shared by the read-only render tests."""
    scaffold = init_helm(tmp_path_factory.mktemp("helm") / "chart", image_tag="v1.0.0")
    return scaffold, _render(scaffold.path)


def test_init_helm_writes_chart_tree(tmp_path):
    target = tmp_path / "aaiclick-chart"
    written = init_helm(target, image_tag="v9.9.9")
    assert written.path == target.resolve()
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


@needs_helm
def test_helm_server_carries_generated_auth_credentials(default_render):
    """Distributed URLs enforce auth, so the server needs a signing secret and
    a seed admin or the release comes up healthy but cannot be logged into."""
    scaffold, docs = default_render
    env = _env(_doc(docs, "Deployment", "aaiclick-server"))
    assert env["AAICLICK_JWT_SECRET"] and PLACEHOLDER_MARKER not in env["AAICLICK_JWT_SECRET"]
    assert env["AAICLICK_ADMIN_PASSWORD"] == scaffold.admin_password


@needs_helm
def test_helm_worker_builds_on_isolated_buildkitd(default_render):
    """The worker turns container tasks into Pods and builds on the chart's
    rootless buildkitd, which only the worker may reach."""
    _, docs = default_render
    worker = _doc(docs, "Deployment", "aaiclick-worker")
    env = _env(worker)
    assert env["AAICLICK_RUNNER"] == "kubernetes"

    service = _doc(docs, "Service", "aaiclick-buildkitd")
    port = service["spec"]["ports"][0]["port"]
    assert env["AAICLICK_BUILDKIT_ADDR"] == f"tcp://{service['metadata']['name']}:{port}"

    buildkitd = _doc(docs, "Deployment", "aaiclick-buildkitd")
    assert buildkitd["spec"]["template"]["spec"]["containers"][0]["image"].endswith("-rootless")

    (ingress,) = _doc(docs, "NetworkPolicy", "aaiclick-buildkitd")["spec"]["ingress"]
    (peer,) = ingress["from"]
    assert peer["podSelector"]["matchLabels"].items() <= worker["spec"]["template"]["metadata"]["labels"].items()
    assert ingress["ports"] == [{"port": port}]


@needs_helm
def test_helm_clickhouse_mounts_the_s3_profile_switched_off(default_render):
    """Same contract as the compose scaffold: the users.d file is mounted into
    ClickHouse as documentation, with the profile commented out."""
    _, docs = default_render
    content = _doc(docs, "ConfigMap", "aaiclick-clickhouse-users")["data"]["s3.xml"]
    assert "s3_allow_server_credentials_in_user_queries" in content
    assert list(ET.fromstring(content)) == []

    pod = _doc(docs, "Deployment", "aaiclick-clickhouse")["spec"]["template"]["spec"]
    (mount,) = pod["containers"][0]["volumeMounts"]
    assert mount["mountPath"] == "/etc/clickhouse-server/users.d/s3.xml"
    assert mount["subPath"] == "s3.xml"
    assert {"name": mount["name"], "configMap": {"name": "aaiclick-clickhouse-users"}} in pod["volumes"]


@needs_helm
def test_helm_worker_mounts_the_github_app_key_when_enabled(tmp_path):
    """The non-root worker gets both settings and a key it can read."""
    target = tmp_path / "aaiclick-chart"
    init_helm(target, image_tag="v1.0.0")

    docs = _render(target, "githubApp.appId=123", "githubApp.secretName=gh")

    worker = _doc(docs, "Deployment", "aaiclick-worker")
    pod = worker["spec"]["template"]["spec"]
    container = pod["containers"][0]
    env = _env(worker)
    assert env["AAICLICK_GITHUB_APP_ID"] == "123"
    assert env["AAICLICK_GITHUB_APP_PEM_FILE"] == "/run/secrets/github-app/github-app.pem"
    assert {"name": "github-app", "mountPath": "/run/secrets/github-app", "readOnly": True} in container["volumeMounts"]
    assert {"name": "github-app", "secret": {"secretName": "gh", "defaultMode": 0o440}} in pod["volumes"]
    assert pod["securityContext"] == {"fsGroup": 1000}
