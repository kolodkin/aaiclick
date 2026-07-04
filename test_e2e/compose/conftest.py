"""Fixtures for the compose-stack e2e suite.

The suite assumes the scaffolded stack is ALREADY RUNNING — CI scaffolds
via ``python -m aaiclick compose init`` and ``docker compose up -d`` before
invoking pytest (see ``_compose-e2e-reusable.yaml``). Tests drive the stack
from outside: HTTP against the published server port, ``docker compose
exec`` for CLI round-trips."""

from __future__ import annotations

import os
from pathlib import Path

import pytest


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "compose_e2e: end-to-end tests against the scaffolded docker-compose stack",
    )


@pytest.fixture(scope="session")
def compose_dir() -> Path:
    raw = os.environ.get("AAICLICK_E2E_COMPOSE_DIR")
    if not raw:
        pytest.skip("AAICLICK_E2E_COMPOSE_DIR not set — compose stack not running")
    return Path(raw)


@pytest.fixture(scope="session")
def server_url(compose_dir: Path) -> str:
    return os.environ.get("AAICLICK_E2E_SERVER_URL", "http://localhost:5255")
