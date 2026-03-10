"""
Orchestration test configuration.

Groups all orchestration tests onto a single xdist worker to prevent
PostgreSQL row-locking conflicts (SELECT FOR UPDATE SKIP LOCKED).
"""

import pytest


def pytest_collection_modifyitems(items):
    try:
        import xdist  # noqa: F401
    except ImportError:
        return
    for item in items:
        if "/orchestration/" in str(item.fspath):
            item.add_marker(pytest.mark.xdist_group("orchestration"))
