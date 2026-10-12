"""Tests for the Dockerfile scaffold command."""

from __future__ import annotations

from pathlib import Path

import pytest

from .docker_scaffold import (
    DEFAULT_BUILD_DOCKERFILE_TEMPLATE,
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
    render_default_dockerfile,
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


_PYTHON_TAIL = "COPY --chown=aaiclick:aaiclick . /src\nWORKDIR /src\n"
_PIP_INDEX = '--index-url "${PIP_INDEX_URL:-https://pypi.org/simple/}"'


def test_render_default_dockerfile_none_is_the_thin_layer():
    assert render_default_dockerfile(MANIFEST_NONE) == DEFAULT_BUILD_DOCKERFILE_TEMPLATE
    assert "USER root" not in DEFAULT_BUILD_DOCKERFILE_TEMPLATE
    assert DEFAULT_BUILD_DOCKERFILE_TEMPLATE.endswith(_PYTHON_TAIL)


@pytest.mark.parametrize(
    "kind, copy_line, resolve_fragment",
    [
        pytest.param(
            MANIFEST_REQUIREMENTS,
            "COPY requirements.txt /tmp/aaiclick-deps/",
            "/tmp/aaiclick-deps/requirements.txt -o /tmp/aaiclick-deps/final.txt",
            id="requirements",
        ),
        pytest.param(
            MANIFEST_UV_LOCK,
            "COPY pyproject.toml uv.lock /tmp/aaiclick-deps/",
            "uv export --frozen --no-dev --no-emit-workspace --no-emit-local --no-emit-package aaiclick --no-hashes",
            id="uv-lock",
        ),
        pytest.param(
            MANIFEST_PYPROJECT,
            "COPY pyproject.toml /tmp/aaiclick-deps/",
            "/tmp/aaiclick-deps/pyproject.toml -o /tmp/aaiclick-deps/final.txt",
            id="pyproject",
        ),
    ],
)
def test_render_default_dockerfile_python_install_block(kind, copy_line, resolve_fragment):
    text = render_default_dockerfile(kind)
    head = DEFAULT_BUILD_DOCKERFILE_TEMPLATE.removesuffix(_PYTHON_TAIL)
    assert text.startswith(head)
    assert copy_line in text
    assert resolve_fragment in text
    # Install runs as root; the runtime user is restored before the tree is copied.
    assert (
        text.index("\nUSER root\n") < text.index(copy_line) < text.index("\nUSER aaiclick\n") < text.index(_PYTHON_TAIL)
    )
    assert text.count(_PIP_INDEX) == text.count("pip install")
    assert text.endswith(_PYTHON_TAIL)


@pytest.mark.parametrize("kind", [MANIFEST_REQUIREMENTS, MANIFEST_UV_LOCK, MANIFEST_PYPROJECT])
def test_render_default_dockerfile_keeps_base_image_packages(kind):
    """The base image's packages (aaiclick and its dependencies) win over the
    user's pins: the installed set overrides the resolve and constrains pip."""
    text = render_default_dockerfile(kind)
    assert "pip list --format=freeze > /tmp/aaiclick-deps/base.txt" in text
    assert "uv pip compile --python python --override /tmp/aaiclick-deps/base.txt --no-emit-package aaiclick" in text
    assert '--default-index "${PIP_INDEX_URL:-https://pypi.org/simple/}"' in text
    assert "${PIP_TRUSTED_HOST:+--allow-insecure-host ${PIP_TRUSTED_HOST}}" in text
    assert "-c /tmp/aaiclick-deps/base.txt -r /tmp/aaiclick-deps/final.txt" in text


def test_render_default_dockerfile_jvm():
    text = render_default_dockerfile(MANIFEST_POM)
    assert text.startswith(
        "# Default image for a git build whose repo has a pom.xml and no Dockerfile.\n"
        "FROM maven:3.9-eclipse-temurin-21 AS build\n"
    )
    assert "mvn -B -q -DskipTests package dependency:copy-dependencies" in text
    assert "FROM eclipse-temurin:21-jre\n" in text
    assert text.endswith(
        'ENTRYPOINT ["java", "-cp", "/app/*:/app/libs/*", "io.github.kolodkin.aaiclick.task.AaiTaskShim"]\n'
    )
    assert "BASE_IMAGE" not in text
