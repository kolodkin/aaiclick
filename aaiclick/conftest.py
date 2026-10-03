"""Root pytest configuration for aaiclick.

Shared fixtures and helpers live in ``aaiclick.testing``. The fixtures are
imported here so pytest registers them at this root-level conftest regardless
of rootdir detection — ``pytest_plugins`` in a nested conftest is ignored
unless that conftest happens to be at pytest's rootdir, which breaks when
tests are collected via ``--pyargs`` from a working directory outside the
repo (e.g. the release-pipeline smoke test).

Subpackage conftests (``data/``, ``orchestration/``, ``oplog/``)
may additionally import the per-test/per-module fixtures they need
(``orch_ctx``, ``orch_ctx_no_ch``, ``ctx``) from ``aaiclick.testing``.
"""

import importlib.util
from collections.abc import Iterator

import pytest

from aaiclick.testing import (  # noqa: F401 - re-exported as pytest fixtures
    gc_leak_check,
    orch_ctx,
    orch_ctx_no_ch,
    orch_module_ctx,
    orch_module_ctx_no_ch,
    worker_databases,
)

# ``server`` is an optional extra: its package imports fastapi at import
# time, so a full-suite run without the extra would error at collection.
# Skip the subtree when fastapi is absent — CI exercises it under a
# dedicated ``--extra`` matrix job.
collect_ignore = [] if importlib.util.find_spec("fastapi") else ["server"]


@pytest.fixture(autouse=True, scope="session")
def _worker_databases() -> Iterator[None]:
    """Give this xdist worker its own CH and SQL databases for the session."""
    with worker_databases():
        yield
