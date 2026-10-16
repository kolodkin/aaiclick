"""
Tests for the non-trivial logic in ``aaiclick.data.object.refs``:
the custom ``_to_wire`` serializers for ``ObjectRef`` / ``ViewRef``, the
``_aaiclick_type`` alias round trip, ``ref_kind`` classification and the
``is_upstream_ref`` / ``is_persistent_object_ref`` predicates.
"""

from __future__ import annotations

import pytest

from aaiclick.data.models import ViewSchema
from aaiclick.data.object.refs import (
    ObjectRef,
    UpstreamRef,
    ViewRef,
    is_persistent_object_ref,
    is_upstream_ref,
    native_ref,
    ref_kind,
)


@pytest.mark.parametrize(
    "ref, expected",
    [
        # Ephemeral refs omit ``persistent`` to keep the wire format minimal.
        pytest.param(
            ObjectRef(table="t_123"),
            {"_aaiclick_type": "object", "table": "t_123"},
            id="ephemeral-omits-persistent",
        ),
        pytest.param(
            ObjectRef(table="p_kev", persistent=True),
            {"_aaiclick_type": "object", "table": "p_kev", "persistent": True},
            id="persistent-emits-flag",
        ),
        # ``persistent=False`` serializes like ``None`` — the flag appears only when truthy.
        pytest.param(
            ObjectRef(table="t_1", persistent=False),
            {"_aaiclick_type": "object", "table": "t_1"},
            id="persistent-false-still-omits",
        ),
    ],
)
def test_object_ref_to_dict(ref, expected):
    """Pure function: the hand-written wire dict is the contract."""
    assert ref.to_dict() == expected


def test_models_round_trip_through_alias():
    wire = {"_aaiclick_type": "upstream", "task_id": 7}
    assert UpstreamRef.model_validate(wire).to_dict() == wire
    assert UpstreamRef(task_id="w1").to_dict() == {"_aaiclick_type": "upstream", "task_id": "w1"}


@pytest.mark.parametrize(
    "value, expected",
    [
        pytest.param(UpstreamRef(task_id=1).to_dict(), "upstream", id="upstream"),
        pytest.param(native_ref(3), "native", id="native"),
        pytest.param(ObjectRef(table="t").to_dict(), "object", id="object"),
        pytest.param({"_aaiclick_type": "task", "id": "w1"}, "task", id="task"),
        pytest.param({"id": "w1", "value": 3}, None, id="untagged-dict-is-plain"),
        pytest.param([1, 2], None, id="non-dict"),
    ],
)
def test_ref_kind(value, expected):
    assert ref_kind(value) == expected


def test_ref_kind_unknown_type_raises():
    with pytest.raises(ValueError, match="_aaiclick_type"):
        ref_kind({"_aaiclick_type": "banana"})


def test_is_upstream_ref_predicate():
    assert is_upstream_ref(UpstreamRef(task_id=1).to_dict()) is True
    assert is_upstream_ref({"_aaiclick_type": "group_results", "group_id": 1}) is False
    assert is_upstream_ref(native_ref(1)) is False
    assert is_upstream_ref(None) is False
    assert is_upstream_ref("str") is False


def test_is_persistent_object_ref_predicate():
    assert is_persistent_object_ref(ObjectRef(table="p_x", persistent=True).to_dict()) is True
    assert is_persistent_object_ref(ObjectRef(table="t_x").to_dict()) is False
    assert (
        is_persistent_object_ref(ViewRef(table="t_x", view_schema=ViewSchema(fieldtype="a", columns={})).to_dict())
        is False
    )
