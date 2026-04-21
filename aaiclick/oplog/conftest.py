"""Pytest fixtures for aaiclick.oplog tests.

Session-scoped worker-isolation and chdb pin fixtures register globally
via the ``aaiclick.testing`` plugin (see ``aaiclick/conftest.py``).
Oplog tests use the full orch context (chdb + SQL) via ``orch_ctx``.
"""

from aaiclick.testing import (  # noqa: F401 — re-exported pytest fixtures
    orch_ctx,
    orch_module_ctx,
)
