"""
Tests for LifecycleHandler ABC and LocalLifecycleHandler.
"""

from unittest.mock import AsyncMock, MagicMock

import pytest

from aaiclick.data.data_context import LifecycleHandler, LocalLifecycleHandler


def test_lifecycle_handler_is_abstract():
    """LifecycleHandler cannot be instantiated directly."""
    with pytest.raises(TypeError):
        LifecycleHandler()  # type: ignore[abstract]


def test_local_lifecycle_delegates_incref():
    """LocalLifecycleHandler.incref delegates to AsyncTableWorker."""
    handler = LocalLifecycleHandler(MagicMock())
    handler._worker = MagicMock()

    handler.incref("table_123")

    handler._worker.incref.assert_called_once_with("table_123")


def test_local_lifecycle_delegates_decref():
    """LocalLifecycleHandler.decref delegates to AsyncTableWorker."""
    handler = LocalLifecycleHandler(MagicMock())
    handler._worker = MagicMock()

    handler.decref("table_456")

    handler._worker.decref.assert_called_once_with("table_456")


async def test_local_lifecycle_start_delegates(ctx):
    """LocalLifecycleHandler.start delegates to AsyncTableWorker.start."""
    handler = LocalLifecycleHandler(MagicMock())
    mock_worker = AsyncMock()
    handler._worker = mock_worker

    await handler.start()

    mock_worker.start.assert_called_once()


async def test_local_lifecycle_stop_delegates(ctx):
    """LocalLifecycleHandler.stop delegates to AsyncTableWorker.stop."""
    handler = LocalLifecycleHandler(MagicMock())
    mock_worker = AsyncMock()
    handler._worker = mock_worker

    await handler.stop()

    mock_worker.stop.assert_called_once()


# Note: ``test_data_context_always_creates_local_lifecycle`` lives in
# ``aaiclick/data_extra_tests/test_lifecycle.py`` because it requires a real
# ``async with data_context():`` block — the ``ctx`` fixture wraps
# orch_context and gives a ``TaskLifecycleHandler``, not the
# ``LocalLifecycleHandler`` this assertion targets.


def test_track_table_records_default_flags():
    handler = LocalLifecycleHandler(MagicMock())
    handler.track_table("t_123")
    tracked = list(handler.iter_tracked_tables())
    assert len(tracked) == 1
    assert tracked[0].name == "t_123"
    assert tracked[0].pinned is False
    assert tracked[0].owned is False


def test_track_table_with_owned_flag():
    handler = LocalLifecycleHandler(MagicMock())
    handler.track_table("t_owned", owned=True)
    tracked = list(handler.iter_tracked_tables())
    assert tracked[0].owned is True


def test_track_table_owned_flag_upgraded_not_downgraded():
    handler = LocalLifecycleHandler(MagicMock())
    handler.track_table("t_x", owned=True)
    handler.track_table("t_x")
    tracked = list(handler.iter_tracked_tables())
    assert tracked[0].owned is True


def test_mark_pinned_after_track():
    handler = LocalLifecycleHandler(MagicMock())
    handler.track_table("t_999")
    handler.mark_pinned("t_999")
    tracked = list(handler.iter_tracked_tables())
    assert tracked[0].pinned is True


def test_mark_pinned_unknown_table_is_silent():
    """Pin can be set by the serializer for tables not registered in this
    handler — be tolerant."""
    handler = LocalLifecycleHandler(MagicMock())
    handler.mark_pinned("t_unknown")
    assert list(handler.iter_tracked_tables()) == []


def test_incref_auto_tracks_table():
    handler = LocalLifecycleHandler(MagicMock())
    handler._worker = MagicMock()
    handler.incref("t_auto")
    tracked = list(handler.iter_tracked_tables())
    assert len(tracked) == 1
    assert tracked[0].name == "t_auto"


def test_lifecycle_handler_base_track_methods_are_noops():
    """Base class defaults: track/mark_pinned no-op, iter_tracked_tables empty."""

    class StubHandler(LifecycleHandler):
        async def start(self):
            pass

        async def stop(self):
            pass

        def incref(self, table_name: str) -> None:
            pass

        def decref(self, table_name: str) -> None:
            pass

    handler = StubHandler()
    handler.track_table("anything")
    handler.mark_pinned("anything")
    assert list(handler.iter_tracked_tables()) == []
