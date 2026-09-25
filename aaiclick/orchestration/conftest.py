"""Pytest fixtures for aaiclick.orchestration tests.

Shared fixtures (``ch_worker_setup``, ``sql_worker_setup``, ``orch_ctx``
family) register globally via the ``aaiclick.testing`` plugin (see
``aaiclick/conftest.py``). This conftest holds orchestration-local
helpers: the polling-speed monkeypatches.
"""

import pytest


@pytest.fixture
def fast_poll(monkeypatch):
    """Reduce polling and retry delays for worker-loop tests."""
    monkeypatch.setattr(
        "aaiclick.orchestration.execution.execution_worker.POLL_INTERVAL",
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
