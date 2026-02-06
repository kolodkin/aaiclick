"""
Tests for Object and View __del__ guards.
"""

import weakref
from unittest.mock import MagicMock, patch

from aaiclick.data.object import Object, View


def test_del_unregistered_object():
    """Object created but never registered should not error on __del__."""
    obj = Object()
    assert obj._context_ref is None
    # Should not raise
    obj.__del__()


def test_del_after_context_gc():
    """Object survives context being garbage collected."""
    obj = Object()
    ctx = MagicMock()
    obj._context_ref = weakref.ref(ctx)

    # Context is GC'd
    del ctx

    # Now obj._context_ref() returns None - should not raise
    obj.__del__()


@patch("aaiclick.data.object.sys.is_finalizing", return_value=True)
def test_del_during_interpreter_shutdown(mock_finalizing):
    """Object.__del__ returns early during interpreter shutdown."""
    obj = Object()
    ctx = MagicMock()
    obj._context_ref = weakref.ref(ctx)

    # Call __del__ - should return early due to is_finalizing
    obj.__del__()

    # decref should NOT have been called
    ctx.decref.assert_not_called()


def test_del_calls_decref_when_context_exists():
    """Object.__del__ calls decref when context is valid."""
    obj = Object()
    ctx = MagicMock()
    # Need to keep a reference to ctx so weakref doesn't return None
    obj._context_ref = weakref.ref(ctx)
    obj._saved_ctx = ctx  # Keep reference alive

    obj.__del__()

    ctx.decref.assert_called_once_with(obj._table_name)


def test_del_worker_none_is_noop():
    """decref is no-op when worker is None (context exited)."""
    obj = Object()
    ctx = MagicMock()
    ctx._worker = None  # Simulate context exit
    obj._context_ref = weakref.ref(ctx)
    obj._saved_ctx = ctx

    # Should not raise even though worker is None
    obj.__del__()

    # decref was called, but it's a no-op internally
    ctx.decref.assert_called_once()


# View tests


def test_view_del_unregistered():
    """View created without registration should not error on __del__."""
    source = Object()
    source._context_ref = None
    view = View(source)

    assert view._context_ref is None
    # Should not raise
    view.__del__()


def test_view_del_after_context_gc():
    """View survives context being garbage collected."""
    source = Object()
    ctx = MagicMock()
    source._context_ref = weakref.ref(ctx)
    source._saved_ctx = ctx

    view = View(source)

    # Context is GC'd
    del ctx
    del source._saved_ctx

    # Now view._context_ref() returns None - should not raise
    view.__del__()


@patch("aaiclick.data.object.sys.is_finalizing", return_value=True)
def test_view_del_during_interpreter_shutdown(mock_finalizing):
    """View.__del__ returns early during interpreter shutdown."""
    source = Object()
    ctx = MagicMock()
    source._context_ref = weakref.ref(ctx)
    source._saved_ctx = ctx

    view = View(source)
    view._saved_ctx = ctx  # Keep reference alive

    # Reset mock to only count View's __del__ call
    ctx.decref.reset_mock()

    view.__del__()

    # decref should NOT have been called by __del__
    ctx.decref.assert_not_called()


def test_view_del_calls_decref_on_source_table():
    """View.__del__ decrefs the source's table."""
    source = Object()
    ctx = MagicMock()
    source._context_ref = weakref.ref(ctx)
    source._saved_ctx = ctx

    view = View(source)
    view._saved_ctx = ctx

    # Reset to only count View's __del__
    ctx.decref.reset_mock()

    view.__del__()

    # Should decref the source's table
    ctx.decref.assert_called_once_with(source.table)
