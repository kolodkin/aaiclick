"""Tests for the Dockerfile scaffold command."""

from __future__ import annotations

from pathlib import Path

import pytest

from .docker_scaffold import (
    _PYTHON_TAIL,
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


@pytest.mark.parametrize(
    "python_files",
    [{"requirements.txt": ""}, {"pyproject.toml": _PROJECT, "uv.lock": ""}, {"pyproject.toml": _PROJECT}],
    ids=["requirements", "uv-lock", "pyproject"],
)
def test_detect_manifest_pom_with_python_manifest_is_ambiguous(tmp_path, python_files):
    (tmp_path / "pom.xml").write_text("<project/>")
    for name, content in python_files.items():
        (tmp_path / name).write_text(content)
    with pytest.raises(AmbiguousManifests, match="pom.xml alongside"):
        detect_manifest(tmp_path)


_THIN_LAYER = render_default_dockerfile(MANIFEST_NONE)


def test_render_default_dockerfile_none_is_the_thin_layer():
    assert "\nUSER " not in _THIN_LAYER
    assert "\nRUN " not in _THIN_LAYER
    assert _THIN_LAYER.endswith(_PYTHON_TAIL)


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
    """The thin layer wraps the install: tooling that depends only on the base
    image (its own cached layer), the manifest copy, resolve + install with the
    base image's packages winning, and the runtime user restored before the
    tree is copied."""
    text = render_default_dockerfile(kind)
    assert text.startswith(_THIN_LAYER.removesuffix(_PYTHON_TAIL))
    assert text.endswith(_PYTHON_TAIL)
    assert resolve_fragment in text
    tooling = "pip list --format=freeze > /tmp/aaiclick-deps/base.txt\n"
    assert text.index("\nUSER root\n") < text.index(tooling) < text.index(copy_line) < text.index("\nUSER aaiclick\n")
    assert (
        "uv pip compile --python python --no-cache --override /tmp/aaiclick-deps/base.txt --no-emit-package aaiclick"
        in text
    )
    assert "uv pip install --system --no-cache" in text
    assert "-c /tmp/aaiclick-deps/base.txt -r /tmp/aaiclick-deps/final.txt" in text
    # The worker's forwarded index args reach pip and uv alike.
    assert text.count('"${PIP_INDEX_URL:-https://pypi.org/simple/}"') == 3
    assert "${PIP_TRUSTED_HOST:+--allow-insecure-host ${PIP_TRUSTED_HOST}}" in text


def test_render_default_dockerfile_jvm():
    text = render_default_dockerfile(MANIFEST_POM)
    assert text.startswith("# Default image for a git build whose repo has a pom.xml and no Dockerfile.\nFROM maven:")
    assert text.endswith('"io.github.kolodkin.aaiclick.task.AaiTaskShim"]\n')
    assert "BASE_IMAGE" not in text
