"""Root pytest configuration for aaiclick.

Shared fixtures live in ``aaiclick.testing`` and are registered as a
pytest plugin here so session-scoped autouse fixtures (``ch_worker_setup``,
``sql_worker_setup``, ``pin_chdb_session``) fire exactly once per session
regardless of how many subpackage conftests re-export them.

Subpackage conftests (``data/``, ``orchestration/``, ``oplog/``, ``ai/``)
import the per-test/per-module fixtures they need (``orch_ctx``,
``orch_ctx_no_ch``, ``ctx``) from ``aaiclick.testing``.
"""

pytest_plugins = ["aaiclick.testing"]
