"""
Tests for Object and View __del__ guards.
"""

from unittest.mock import patch

from aaiclick import create_object_from_value
from aaiclick.data.data_context import data_context
from aaiclick.data.object import Object

# Object guard tests


def test_del_guard_unregistered_object():
    """Guard: Object created without factory should not error on __del__."""
    obj = Object()
    assert obj._registered is False
    obj.__del__()


@patch("aaiclick.data.object.object.sys.is_finalizing", return_value=True)
async def test_del_guard_interpreter_shutdown(mock_finalizing, ctx):
    """Guard: __del__ skips decref during interpreter shutdown."""
    obj = await create_object_from_value([1, 2, 3])

    obj.__del__()

    # decref was skipped, data should still be accessible
    result = await obj.data()
    assert result == [1, 2, 3]


async def test_del_guard_after_context_exit():
    """Guard: __del__ after context exit should not raise."""
    async with data_context():
        obj = await create_object_from_value([1, 2, 3])

    # Context exited, worker stopped
    obj.__del__()


# View guard tests


@patch("aaiclick.data.object.object.sys.is_finalizing", return_value=True)
async def test_view_del_guard_interpreter_shutdown(mock_finalizing, ctx):
    """Guard: View.__del__ skips decref during interpreter shutdown."""
    obj = await create_object_from_value([1, 2, 3])
    view = obj.view(limit=2)

    view.__del__()

    # decref was skipped, source data should still be accessible
    result = await obj.data()
    assert result == [1, 2, 3]


async def test_view_del_guard_after_context_exit():
    """Guard: View.__del__ after context exit should not raise."""
    async with data_context():
        obj = await create_object_from_value([1, 2, 3])
        view = obj.view(limit=2)

    # Context exited, worker stopped
    view.__del__()
