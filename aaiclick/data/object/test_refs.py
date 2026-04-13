"""
Tests for the non-trivial logic in ``aaiclick.data.object.refs``:
the custom ``_to_wire`` serializers for ``ObjectRef`` / ``ViewRef`` and
the ``is_upstream_ref`` / ``is_persistent_object_ref`` predicates.

Plain model shapes (``UpstreamRef``, ``GroupResultsRef``, ``CallableRef``)
are not re-tested here — their serialization is default Pydantic
behavior, covered upstream.
"""

from __future__ import annotations

from aaiclick.data.object.refs import (
    ObjectRef,
    UpstreamRef,
    ViewRef,
    is_persistent_object_ref,
    is_upstream_ref,
)


def test_object_ref_ephemeral_omits_persistent():
    """Ephemeral Object refs have no ``persistent`` key — the wire
    format stays minimal for the common case."""
    assert ObjectRef(table="t_123").to_dict() == {
        "object_type": "object",
        "table": "t_123",
    }


def test_object_ref_persistent_emits_flag():
    assert ObjectRef(table="p_kev", persistent=True).to_dict() == {
        "object_type": "object",
        "table": "p_kev",
        "persistent": True,
    }


def test_object_ref_persistent_false_still_omits():
    """``persistent=False`` serializes identically to ``persistent=None``
    — the flag only appears on the wire when truthy."""
    assert ObjectRef(table="t_1", persistent=False).to_dict() == {
        "object_type": "object",
        "table": "t_1",
    }


def test_view_ref_with_modifiers():
    """Full round-trip of ``ViewRef`` — pins every field the hand-written
    ``_to_wire`` serializer emits, so dropping one breaks this test."""
    wire = ViewRef(
        table="t_1",
        where="x > 0",
        limit=10,
        offset=5,
        order_by="aai_id",
        selected_fields=["a", "b"],
        renamed_columns={"a": "alpha"},
        persistent=True,
    ).to_dict()
    assert wire == {
        "object_type": "view",
        "table": "t_1",
        "where": "x > 0",
        "limit": 10,
        "offset": 5,
        "order_by": "aai_id",
        "selected_fields": ["a", "b"],
        "renamed_columns": {"a": "alpha"},
        "persistent": True,
    }


def test_is_upstream_ref_predicate():
    assert is_upstream_ref(UpstreamRef(task_id=1).to_dict()) is True
    assert is_upstream_ref({"ref_type": "group_results", "group_id": 1}) is False
    assert is_upstream_ref({"native_value": 1}) is False
    assert is_upstream_ref(None) is False
    assert is_upstream_ref("str") is False


def test_is_persistent_object_ref_predicate():
    assert (
        is_persistent_object_ref(
            ObjectRef(table="p_x", persistent=True).to_dict()
        )
        is True
    )
    assert is_persistent_object_ref(ObjectRef(table="t_x").to_dict()) is False
    assert is_persistent_object_ref(ViewRef(table="t_x").to_dict()) is False
