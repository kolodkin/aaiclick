"""Tests for the helm chart scaffold command."""

from __future__ import annotations

import shutil
import subprocess
import sys
import xml.etree.ElementTree as ET

import pytest
import yaml

from .compose_scaffold import IMAGE_TAG_TOKEN
from .k8s_scaffold import HelmChartExists, init_helm

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


def test_init_helm_server_carries_auth_credentials(tmp_path):
    """Distributed URLs enforce auth, so the server needs a signing secret and
    a seed admin or the release comes up healthy but cannot be logged into."""
    target = tmp_path / "aaiclick-chart"
    init_helm(target, image_tag="v1.0.0")

    values = yaml.safe_load((target / "values.yaml").read_text())
    assert values["auth"]["jwtSecret"]
    assert values["auth"]["adminPassword"]

    server = (target / "templates" / "server.yaml").read_text()
    assert "AAICLICK_JWT_SECRET" in server
    assert ".Values.auth.jwtSecret" in server
    assert "AAICLICK_ADMIN_PASSWORD" in server
    assert ".Values.auth.adminPassword" in server


def test_init_helm_worker_runs_the_kubernetes_runner(tmp_path):
    """The chart's worker turns container tasks into Pods."""
    target = tmp_path / "aaiclick-chart"
    init_helm(target, image_tag="v1.0.0")
    worker = (target / "templates" / "worker.yaml").read_text()
    assert "- name: AAICLICK_RUNNER" in worker
    assert 'value: "kubernetes"' in worker


def test_init_helm_builds_on_isolated_buildkitd(tmp_path):
    """The worker builds on the chart's buildkitd, which only the worker may reach."""
    target = tmp_path / "chart"
    init_helm(target, image_tag="v1.0.0")
    values = yaml.safe_load((target / "values.yaml").read_text())
    assert values["buildkit"]["enabled"] is True
    assert values["buildkit"]["image"].endswith("-rootless")
    buildkitd = (target / "templates" / "buildkitd.yaml").read_text()
    assert "kind: NetworkPolicy" in buildkitd and "app: aaiclick-worker" in buildkitd
    assert "tcp://aaiclick-buildkitd:1234" in (target / "templates" / "worker.yaml").read_text()


def test_init_helm_ships_the_s3_profile_switched_off(tmp_path):
    """Same contract as the compose scaffold: the users.d file is mounted into
    ClickHouse as documentation, with the profile commented out."""
    target = tmp_path / "aaiclick-chart"
    init_helm(target, image_tag="v1.0.0")

    values = yaml.safe_load((target / "values.yaml").read_text())
    content = values["devDependencies"]["clickhouse"]["s3UsersXml"]
    assert "s3_allow_server_credentials_in_user_queries" in content
    assert list(ET.fromstring(content)) == []

    clickhouse = (target / "templates" / "clickhouse.yaml").read_text()
    assert "/etc/clickhouse-server/users.d/s3.xml" in clickhouse
    assert ".Values.devDependencies.clickhouse.s3UsersXml" in clickhouse


def test_init_helm_ships_github_app_credentials_switched_off(tmp_path):
    target = tmp_path / "aaiclick-chart"
    init_helm(target, image_tag="v1.0.0")
    values = yaml.safe_load((target / "values.yaml").read_text())
    assert values["githubApp"] == {"appId": "", "secretName": ""}


def _render_worker(chart, *sets: str) -> subprocess.CompletedProcess[str]:
    args = ["helm", "template", "x", str(chart), "--set", "devDependencies.enabled=true"]
    for item in sets:
        args += ["--set", item]
    return subprocess.run(args, capture_output=True, text=True)


def _worker_pod(rendered: str) -> dict:
    docs = [d for d in yaml.safe_load_all(rendered) if d]
    (worker,) = [d for d in docs if d["kind"] == "Deployment" and d["metadata"]["name"] == "aaiclick-worker"]
    return worker["spec"]["template"]["spec"]


needs_helm = pytest.mark.skipif(shutil.which("helm") is None, reason="needs the helm binary")


@needs_helm
def test_helm_worker_mounts_the_github_app_key_when_enabled(tmp_path):
    """The non-root worker gets both settings and a key it can read."""
    target = tmp_path / "aaiclick-chart"
    init_helm(target, image_tag="v1.0.0")

    result = _render_worker(target, "githubApp.appId=123", "githubApp.secretName=gh")

    assert result.returncode == 0, result.stderr
    pod = _worker_pod(result.stdout)
    container = pod["containers"][0]
    env = {e["name"]: e.get("value") for e in container["env"]}
    assert env["AAICLICK_GITHUB_APP_ID"] == "123"
    assert env["AAICLICK_GITHUB_APP_PEM_FILE"] == "/run/secrets/github-app/github-app.pem"
    assert {"name": "github-app", "mountPath": "/run/secrets/github-app", "readOnly": True} in container["volumeMounts"]
    assert {"name": "github-app", "secret": {"secretName": "gh", "defaultMode": 0o440}} in pod["volumes"]
    assert pod["securityContext"] == {"fsGroup": 1000}


@needs_helm
def test_helm_worker_is_unchanged_without_github_app(tmp_path):
    target = tmp_path / "aaiclick-chart"
    init_helm(target, image_tag="v1.0.0")

    result = _render_worker(target)

    assert result.returncode == 0, result.stderr
    pod = _worker_pod(result.stdout)
    assert "securityContext" not in pod and "volumes" not in pod
    assert not any(e["name"].startswith("AAICLICK_GITHUB_APP") for e in pod["containers"][0]["env"])


@needs_helm
@pytest.mark.parametrize(
    "only",
    [
        pytest.param("githubApp.appId=123", id="secret-missing"),
        pytest.param("githubApp.secretName=gh", id="app-id-missing"),
    ],
)
def test_helm_rejects_half_configured_github_app(tmp_path, only):
    target = tmp_path / "aaiclick-chart"
    init_helm(target, image_tag="v1.0.0")

    result = _render_worker(target, only)

    assert result.returncode != 0
    assert "set both appId and secretName, or neither" in result.stderr
