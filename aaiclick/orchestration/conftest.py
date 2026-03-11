"""
Orchestration test configuration.

Groups all orchestration tests onto a single xdist worker to prevent
PostgreSQL row-locking conflicts (SELECT FOR UPDATE SKIP LOCKED).
"""

import pytest

from aaiclick.orchestration.context import orch_context


def pytest_collection_modifyitems(items):
    try:
        import xdist  # noqa: F401
    except ImportError:
        return
    for item in items:
        if "/orchestration/" in str(item.fspath):
            item.add_marker(pytest.mark.xdist_group("orchestration"))


@pytest.fixture
async def orch_ctx():
    """
    Function-scoped orch context for orchestration tests.

    Cannot be session-scoped: SQLAlchemy async sessions don't safely
    share across tests (concurrent operation and event loop issues).
    """
    async with orch_context():
        yield
