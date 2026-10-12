"""Tests for the Dockerfile scaffold command."""

from __future__ import annotations

from pathlib import Path

import pytest

from .docker_scaffold import (
    DOCKERFILE_TEMPLATE,
    MANIFEST_NONE,
    MANIFEST_POM,
    MANIFEST_PYPROJECT,
    MANIFEST_REQUIREMENTS,
    MANIFEST_UV_LOCK,
    AmbiguousManifests,
    DockerfileExists,
    detect_manifest,
    init_dockerfile,
)


def test_init_dockerfile_writes_file(tmp_path):
    target = tmp_path / "Dockerfile"
    written = init_dockerfile(target)
    assert written == target.resolve()
    assert target.read_text() == DOCKERFILE_TEMPLATE


def test_init_dockerfile_refuses_overwrite(tmp_path):
    target = tmp_path / "Dockerfile"
    target.write_text("# user's own Dockerfile\n")

    with pytest.raises(DockerfileExists, match="already exists"):
        init_dockerfile(target)

    # User's content untouched.
    assert target.read_text() == "# user's own Dockerfile\n"


def test_init_dockerfile_force_overwrites(tmp_path):
    target = tmp_path / "Dockerfile"
    target.write_text("# stale\n")

    init_dockerfile(target, force=True)

    assert target.read_text() == DOCKERFILE_TEMPLATE


def test_e2e_fixture_dockerfile_matches_template():
    """The e2e fixture Dockerfile must match the canonical template
    byte-for-byte. Drift is silent rot — the e2e would still pass while
    the scaffold (the user-facing artifact) diverged. Pinning them
    together turns drift into an immediate test failure.

    This test is unconditional — it doesn't require a docker daemon —
    so it runs on every PR alongside the regular orch suite."""
    repo_root = Path(__file__).resolve().parents[3]
    fixture = repo_root / "test_e2e" / "docker" / "fixtures" / "sample_job" / "Dockerfile"
    if not fixture.is_file():
        pytest.skip(f"e2e fixture not present at {fixture} (running from installed wheel?)")
    assert fixture.read_text() == DOCKERFILE_TEMPLATE, (
        "test_e2e/docker/fixtures/sample_job/Dockerfile has drifted from "
        "DOCKERFILE_TEMPLATE in docker_scaffold.py. Update both — they must "
        "stay byte-equal so the e2e exercises exactly what the user-facing "
        "`aaiclick docker init` command produces."
    )


_PROJECT = "[project]\nname = 'x'\n"


@pytest.mark.parametrize(
    "files, expected",
    [
        pytest.param({}, MANIFEST_NONE, id="empty"),
        pytest.param({"job.py": ""}, MANIFEST_NONE, id="code-only"),
        pytest.param({"requirements.txt": ""}, MANIFEST_REQUIREMENTS, id="requirements-empty-file"),
        pytest.param({"requirements.txt": "six\n"}, MANIFEST_REQUIREMENTS, id="requirements"),
        pytest.param({"pyproject.toml": _PROJECT, "uv.lock": ""}, MANIFEST_UV_LOCK, id="uv-lock"),
        pytest.param(
            {"pyproject.toml": _PROJECT, "uv.lock": "", "requirements.txt": ""},
            MANIFEST_UV_LOCK,
            id="uv-lock-beats-requirements",
        ),
        pytest.param(
            {"pyproject.toml": _PROJECT, "requirements.txt": ""},
            MANIFEST_REQUIREMENTS,
            id="requirements-beats-pyproject",
        ),
        pytest.param({"pyproject.toml": _PROJECT}, MANIFEST_PYPROJECT, id="pyproject"),
        pytest.param(
            {"pyproject.toml": "[tool.ruff]\nline-length = 120\n"}, MANIFEST_NONE, id="pyproject-tool-config-only"
        ),
        pytest.param(
            {"pyproject.toml": "[project.scripts]\nx = 'x:main'\n"}, MANIFEST_NONE, id="pyproject-subtable-only"
        ),
        pytest.param({"pom.xml": "<project/>"}, MANIFEST_POM, id="pom"),
        pytest.param(
            {"pom.xml": "<project/>", "pyproject.toml": "[tool.ruff]\n"},
            MANIFEST_POM,
            id="pom-with-tool-config-pyproject",
        ),
        pytest.param({"sub/requirements.txt": ""}, MANIFEST_NONE, id="subdirectory-ignored"),
    ],
)
def test_detect_manifest(tmp_path, files, expected):
    for name, content in files.items():
        path = tmp_path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)
    assert detect_manifest(tmp_path) == expected


def test_detect_manifest_requirements_directory_is_not_a_manifest(tmp_path):
    (tmp_path / "requirements.txt").mkdir()
    assert detect_manifest(tmp_path) == MANIFEST_NONE


@pytest.mark.parametrize("python_manifest", ["requirements.txt", "uv.lock", "pyproject.toml"])
def test_detect_manifest_pom_with_python_manifest_is_ambiguous(tmp_path, python_manifest):
    (tmp_path / "pom.xml").write_text("<project/>")
    (tmp_path / "pyproject.toml").write_text(_PROJECT)
    (tmp_path / python_manifest).write_text(_PROJECT if python_manifest == "pyproject.toml" else "")
    with pytest.raises(AmbiguousManifests, match="pom.xml alongside"):
        detect_manifest(tmp_path)
