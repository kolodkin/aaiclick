"""
Tests for the non-trivial logic in ``aaiclick.data.object.refs``:
the custom ``_to_wire`` serializers for ``ObjectRef`` / ``ViewRef`` and
the ``is_upstream_ref`` / ``is_persistent_object_ref`` predicates.

Plain model shapes (``UpstreamRef``, ``GroupResultsRef``, ``CallableRef``)
are not re-tested here — their serialization is default Pydantic
behavior, covered upstream.
"""

from __future__ import annotations

import pytest

from aaiclick.data.object.refs import (
    ObjectRef,
    UpstreamRef,
    ViewRef,
    is_persistent_object_ref,
    is_upstream_ref,
)


@pytest.mark.parametrize(
    "ref, expected",
    [
        # Ephemeral refs omit ``persistent`` to keep the wire format minimal.
        pytest.param(
            ObjectRef(table="t_123"),
            {"object_type": "object", "table": "t_123"},
            id="ephemeral-omits-persistent",
        ),
        pytest.param(
            ObjectRef(table="p_kev", persistent=True),
            {"object_type": "object", "table": "p_kev", "persistent": True},
            id="persistent-emits-flag",
        ),
        # ``persistent=False`` serializes like ``None`` — the flag appears only when truthy.
        pytest.param(
            ObjectRef(table="t_1", persistent=False),
            {"object_type": "object", "table": "t_1"},
            id="persistent-false-still-omits",
        ),
    ],
)
def test_object_ref_to_dict(ref, expected):
    """Pure function: the hand-written wire dict is the contract."""
    assert ref.to_dict() == expected


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
    assert is_persistent_object_ref(ObjectRef(table="p_x", persistent=True).to_dict()) is True
    assert is_persistent_object_ref(ObjectRef(table="t_x").to_dict()) is False
    assert is_persistent_object_ref(ViewRef(table="t_x").to_dict()) is False
