"""Tests for the Dockerfile scaffold command."""

from __future__ import annotations

from pathlib import Path

import pytest

from .docker_scaffold import DOCKERFILE_TEMPLATE, DockerfileExists, init_dockerfile


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
