"""Pytest fixtures for aaiclick.orchestration tests.

Shared fixtures (``ch_worker_setup``, ``sql_worker_setup``, ``orch_ctx``
family) register globally via the ``aaiclick.testing`` plugin (see
``aaiclick/conftest.py``). This conftest holds orchestration-local
helpers: the log-dir redirect and the polling-speed monkeypatches.
"""

import pytest


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
