"""
Tests for ``aaiclick.data.object.refs`` — the Pydantic models + helpers
that define the serialized Task kwarg/result shape.

These tests pin down the on-wire format so that any regression (e.g.
a model field added without a serializer update) is caught immediately.
The format is consumed by ``orchestration.execution.runner`` when
deserializing task kwargs/results, so its stability matters.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from aaiclick.data.object.refs import (
    CallableRef,
    GroupResultsRef,
    ObjectRef,
    UpstreamRef,
    ViewRef,
    callable_ref,
    group_results_ref,
    is_persistent_object_ref,
    is_upstream_ref,
    native_value_ref,
    upstream_ref,
)


def test_upstream_ref_wire_shape():
    assert UpstreamRef(task_id=42).to_dict() == {
        "ref_type": "upstream",
        "task_id": 42,
    }


def test_upstream_ref_rejects_non_int_task_id():
    with pytest.raises(ValidationError):
        UpstreamRef(task_id="not_an_int")  # type: ignore[arg-type]


def test_group_results_ref_wire_shape():
    assert GroupResultsRef(group_id=99).to_dict() == {
        "ref_type": "group_results",
        "group_id": 99,
    }


def test_callable_ref_wire_shape():
    assert CallableRef(entrypoint="mod.fn").to_dict() == {
        "ref_type": "callable",
        "entrypoint": "mod.fn",
    }


def test_object_ref_ephemeral_omits_persistent():
    """Ephemeral Object refs have no ``persistent`` key — that keeps
    the wire format minimal for the common case."""
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


def test_object_ref_emits_job_id_when_set():
    assert ObjectRef(table="p_x", persistent=True, job_id=5).to_dict() == {
        "object_type": "object",
        "table": "p_x",
        "persistent": True,
        "job_id": 5,
    }


def test_view_ref_always_emits_modifier_fields():
    """View modifier fields (where / limit / offset / ...) are always
    present on the wire — the runner's ``View(...)`` reconstruction
    reads them unconditionally via ``.get()``."""
    wire = ViewRef(table="t_1").to_dict()
    assert wire == {
        "object_type": "view",
        "table": "t_1",
        "where": None,
        "limit": None,
        "offset": None,
        "order_by": None,
        "selected_fields": None,
        "renamed_columns": None,
    }


def test_view_ref_with_modifiers():
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


def test_helpers_match_model_output():
    """The ``upstream_ref`` / ``group_results_ref`` / ``callable_ref``
    helpers are thin wrappers — assert they match the model's output."""
    assert upstream_ref(7) == UpstreamRef(task_id=7).to_dict()
    assert group_results_ref(8) == GroupResultsRef(group_id=8).to_dict()
    assert callable_ref("mod.f") == CallableRef(entrypoint="mod.f").to_dict()


def test_native_value_ref_is_transparent():
    assert native_value_ref(42) == {"native_value": 42}
    assert native_value_ref([1, 2, 3]) == {"native_value": [1, 2, 3]}


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


def test_refs_are_frozen():
    """Refs are immutable value objects — mutation should raise."""
    ref = UpstreamRef(task_id=1)
    with pytest.raises(ValidationError):
        ref.task_id = 2  # type: ignore[misc]
