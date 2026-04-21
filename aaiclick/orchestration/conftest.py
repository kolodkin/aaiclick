"""Pytest fixtures for aaiclick.orchestration tests.

Session-scoped worker-isolation and chdb pin fixtures register globally
via the ``aaiclick.test_utils`` plugin (see ``aaiclick/conftest.py``).
This conftest imports the per-test orch-context fixtures and defines
orchestration-local helpers: the log-dir redirect and the polling-speed
monkeypatches.
"""

import pytest

from aaiclick.test_utils import (  # noqa: F401 — re-exported pytest fixtures
    orch_ctx,
    orch_ctx_no_ch,
    orch_module_ctx,
    orch_module_ctx_no_ch,
)


@pytest.fixture(autouse=True)
def _tmp_log_dir(tmp_path, monkeypatch):
    """Direct task logs to a temporary directory in all orchestration tests."""
    monkeypatch.setenv("AAICLICK_LOG_DIR", str(tmp_path))


@pytest.fixture
def fast_poll(monkeypatch):
    """Reduce polling and retry delays for worker-loop tests."""
    monkeypatch.setattr(
        "aaiclick.orchestration.execution.worker.POLL_INTERVAL",
        0.5,
    )
    monkeypatch.setattr(
        "aaiclick.orchestration.background.background_worker.RETRY_BASE_DELAY",
        0.01,
    )
    monkeypatch.setattr(
        "aaiclick.orchestration.execution.mp_worker.CHILD_POLL_INTERVAL",
        0.1,
    )
