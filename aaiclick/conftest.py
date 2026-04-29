"""Root pytest configuration for aaiclick.

Shared fixtures and helpers live in ``aaiclick.testing``. The fixtures are
imported here so pytest registers them at this root-level conftest regardless
of rootdir detection — ``pytest_plugins`` in a nested conftest is ignored
unless that conftest happens to be at pytest's rootdir, which breaks when
tests are collected via ``--pyargs`` from a working directory outside the
repo (e.g. the release-pipeline smoke test).

Subpackage conftests (``data/``, ``orchestration/``, ``oplog/``, ``ai/``)
may additionally import the per-test/per-module fixtures they need
(``orch_ctx``, ``orch_ctx_no_ch``, ``ctx``) from ``aaiclick.testing``.
"""

from aaiclick.testing import (  # noqa: F401 - re-exported as pytest fixtures
    ch_worker_setup,
    orch_ctx,
    orch_ctx_no_ch,
    orch_module_ctx,
    orch_module_ctx_no_ch,
    sql_worker_setup,
)
