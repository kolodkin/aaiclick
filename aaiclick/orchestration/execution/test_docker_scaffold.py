"""Tests for the Dockerfile scaffold command and the default build Dockerfiles.

The rendered Dockerfile text is the contract the build task writes into a
checkout, so the render tests assert on it directly."""

from __future__ import annotations

from pathlib import Path

import pytest

from .docker_scaffold import (
    _PYTHON_HEAD,
    _PYTHON_TAIL,
    DOCKERFILE_TEMPLATE,
    MANIFEST_NONE,
    MANIFEST_POM,
    MANIFEST_PYPROJECT,
    MANIFEST_REQUIREMENTS,
    MANIFEST_UV_LOCK,
    DockerfileExists,
    detect_manifest,
    init_dockerfile,
    render_default_dockerfile,
)

_REPO_ROOT = Path(__file__).resolve().parents[3]


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
    fixture = _REPO_ROOT / "test_e2e" / "fixtures" / "sample_job" / "Dockerfile"
    if not fixture.is_file():
        pytest.skip(f"e2e fixture not present at {fixture} (running from installed wheel?)")
    assert fixture.read_text() == DOCKERFILE_TEMPLATE, (
        "test_e2e/fixtures/sample_job/Dockerfile has drifted from "
        "DOCKERFILE_TEMPLATE in docker_scaffold.py. Update both — they must "
        "stay byte-equal so the e2e exercises exactly what the user-facing "
        "`aaiclick docker init` command produces."
    )


_PROJECT = "[project]\nname = 'x'\n"


@pytest.mark.parametrize(
    "files, expected",
    [
        pytest.param({}, MANIFEST_NONE, id="empty"),
        pytest.param({"requirements.txt": ""}, MANIFEST_REQUIREMENTS, id="requirements"),
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
            {"pyproject.toml": "[project]  # metadata\nname = 'x'\n"}, MANIFEST_PYPROJECT, id="pyproject-header-comment"
        ),
        pytest.param({"uv.lock": ""}, MANIFEST_NONE, id="uv-lock-without-pyproject"),
        pytest.param(
            {"uv.lock": "", "requirements.txt": ""}, MANIFEST_REQUIREMENTS, id="uv-lock-without-pyproject-falls-through"
        ),
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


def test_detect_manifest_pom_with_python_manifest_is_ambiguous(tmp_path):
    (tmp_path / "pom.xml").write_text("<project/>")
    (tmp_path / "requirements.txt").write_text("")
    with pytest.raises(ValueError, match="pom.xml alongside requirements.txt"):
        detect_manifest(tmp_path)


def test_render_default_dockerfile_none_is_the_thin_layer():
    text = render_default_dockerfile(MANIFEST_NONE)
    assert "\nRUN " not in text and "\nUSER " not in text
    assert text.endswith("FROM ${BASE_IMAGE}\nCOPY --chown=aaiclick:aaiclick . /src\nWORKDIR /src\n")


@pytest.mark.parametrize(
    "kind, requirements",
    [
        pytest.param(MANIFEST_REQUIREMENTS, "requirements.txt", id="requirements"),
        pytest.param(MANIFEST_UV_LOCK, "/tmp/exported.txt", id="uv-lock"),
        pytest.param(MANIFEST_PYPROJECT, "pyproject.toml", id="pyproject"),
    ],
)
def test_render_default_dockerfile_python_install_block(kind, requirements):
    """The thin layer wraps one root layer that mounts the checkout as cwd,
    installs the manifest with the base image's packages as overrides, removes
    uv, and restores the runtime user before the tree is copied."""
    text = render_default_dockerfile(kind)
    assert text.startswith(_PYTHON_HEAD)
    assert text.endswith(_PYTHON_TAIL)
    assert text.count("\nRUN ") == 1
    assert (
        text.index("\nUSER root\n")
        < text.index("RUN --mount=type=bind,target=/ctx")
        < text.index("cd /ctx")
        < text.index("pip install --target /tmp/uv ")
        < text.index("pip list --format=freeze > /tmp/base.txt")
        < text.index("/tmp/uv/bin/uv pip install --system ")
        < text.index(f"--overrides /tmp/base.txt -r {requirements}")
        < text.index("rm -rf /tmp/uv /tmp/base.txt")
        < text.index("\nUSER aaiclick\n")
    )
    assert ("uv export --frozen --no-dev --no-hashes --no-emit-project --no-editable" in text) == (
        kind == MANIFEST_UV_LOCK
    )


def test_render_default_dockerfile_jvm():
    text = render_default_dockerfile(MANIFEST_POM)
    assert text.index("FROM maven:") < text.index("COPY . /build") < text.index("FROM eclipse-temurin:")
    assert text.endswith('"io.github.kolodkin.aaiclick.task.AaiTaskShim"]\n')


_SECRET_MOUNTS = (
    "--mount=type=secret,id=pip_index_url,env=PIP_INDEX_URL",
    "--mount=type=secret,id=pip_extra_index_url,env=PIP_EXTRA_INDEX_URL",
)


@pytest.mark.parametrize(
    "text",
    [
        pytest.param(render_default_dockerfile(MANIFEST_REQUIREMENTS), id="requirements"),
        pytest.param(render_default_dockerfile(MANIFEST_UV_LOCK), id="uv-lock"),
        pytest.param(render_default_dockerfile(MANIFEST_PYPROJECT), id="pyproject"),
        pytest.param(DOCKERFILE_TEMPLATE, id="docker-init"),
    ],
)
def test_index_urls_come_from_secret_mounts(text):
    """Index URLs may embed a token, so they reach the install step as secrets,
    never as build-args a Dockerfile step could persist."""
    assert text.startswith("# syntax=docker/dockerfile:1\n")
    assert all(mount in text for mount in _SECRET_MOUNTS)
    assert "ARG PIP_INDEX_URL" not in text and "ARG PIP_EXTRA_INDEX_URL" not in text
