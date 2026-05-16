"""Tests for the LazyOperator class and named operator results.

Spec: docs/lazy_operator.md
"""

import pytest

from aaiclick import create_object_from_value
from aaiclick.data.models import (
    AAI_ID_COLUMN,
    FIELDTYPE_ARRAY,
    FIELDTYPE_SCALAR,
    ColumnInfo,
    Schema,
)
from aaiclick.data.object import LazyOperator, operators
from aaiclick.data.object.operators import (
    _peek_schema,
    _preview_operator_schema,
)


BINARY_OPERATORS = ["+", "-", "*", "/", "//", "%", "**",
                    "==", "!=", "<", "<=", ">", ">=",
                    "&", "|", "^"]


async def test_apply_operator_db_with_name_uses_temp_named_scope(ctx):
    """name='foo' (no scope) → temp_named table prefix t_foo_<id>."""
    obj_a = await create_object_from_value([1, 2, 3], aai_id=True)
    obj_b = await create_object_from_value([10, 20, 30], aai_id=True)
    result = await operators._apply_operator_db(
        obj_a._get_query_info(),
        obj_b._get_query_info(),
        "+",
        obj_a.ch_client,
        name="foo",
    )
    assert result.table.startswith("t_foo_")
    assert await result.data() == [11, 22, 33]


async def test_apply_operator_db_with_name_and_scope_job(ctx):
    """name='bar', scope='job' → j_<job_id>_bar table."""
    obj_a = await create_object_from_value([1, 2, 3], aai_id=True)
    obj_b = await create_object_from_value([10, 20, 30], aai_id=True)
    result = await operators._apply_operator_db(
        obj_a._get_query_info(),
        obj_b._get_query_info(),
        "+",
        obj_a.ch_client,
        name="bar",
        scope="job",
    )
    assert result.table.startswith("j_")
    assert result.table.endswith("_bar")
    assert result.persistent is True
    assert await result.data() == [11, 22, 33]


@pytest.mark.parametrize("operator", BINARY_OPERATORS)
async def test_preview_matches_materialized_schema_array_array(ctx, operator):
    """Pre-materialize schema preview must match the schema of the materialized result."""
    obj_a = await create_object_from_value([1, 2, 3], aai_id=True)
    obj_b = await create_object_from_value([4, 5, 6], aai_id=True)

    preview = _preview_operator_schema(obj_a.schema, obj_b.schema, operator)
    materialized = await operators._apply_operator_db(
        obj_a._get_query_info(),
        obj_b._get_query_info(),
        operator,
        obj_a.ch_client,
    )

    # Compare the bits that the preview is responsible for: fieldtype + columns.
    # Table name and engine are set by create_object and are not part of preview.
    assert preview.fieldtype == materialized.schema.fieldtype
    assert set(preview.columns.keys()) == set(materialized.schema.columns.keys())
    for col_name in preview.columns:
        assert preview.columns[col_name].type == materialized.schema.columns[col_name].type
        assert preview.columns[col_name].nullable == materialized.schema.columns[col_name].nullable


@pytest.mark.parametrize("operator", BINARY_OPERATORS)
async def test_preview_matches_materialized_schema_array_scalar(ctx, operator):
    """Scalar broadcast must produce a matching preview schema."""
    obj_a = await create_object_from_value([1, 2, 3], aai_id=True)
    obj_b = await create_object_from_value(7)

    preview = _preview_operator_schema(obj_a.schema, obj_b.schema, operator)
    materialized = await operators._apply_operator_db(
        obj_a._get_query_info(),
        obj_b._get_query_info(),
        operator,
        obj_a.ch_client,
    )

    assert preview.fieldtype == materialized.schema.fieldtype
    for col_name in preview.columns:
        assert preview.columns[col_name].type == materialized.schema.columns[col_name].type


def test_peek_schema_python_int():
    schema = _peek_schema(7)
    assert schema.fieldtype == FIELDTYPE_SCALAR
    assert schema.columns["value"].type == "Int64"


def test_peek_schema_python_float():
    schema = _peek_schema(3.14)
    assert schema.fieldtype == FIELDTYPE_SCALAR
    assert schema.columns["value"].type == "Float64"


async def test_peek_schema_existing_object_returns_its_schema(ctx):
    """For an Object, _peek_schema just returns .schema unchanged."""
    obj = await create_object_from_value([1, 2, 3], aai_id=True)
    assert _peek_schema(obj) is obj.schema


# -----------------------------------------------------------------------------
# LazyOperator class
# -----------------------------------------------------------------------------


async def test_lazy_operator_holds_lhs_rhs_operator(ctx):
    obj_a = await create_object_from_value([1, 2, 3], aai_id=True)
    obj_b = await create_object_from_value([4, 5, 6], aai_id=True)
    schema_preview = _preview_operator_schema(obj_a.schema, obj_b.schema, "+")
    lazy = LazyOperator(lhs=obj_a, rhs=obj_b, operator="+", schema_preview=schema_preview)

    assert lazy.lhs is obj_a
    assert lazy.rhs is obj_b
    assert lazy.operator == "+"
    assert lazy.schema.fieldtype == FIELDTYPE_ARRAY


async def test_lazy_operator_table_raises_before_materialize(ctx):
    obj_a = await create_object_from_value([1, 2, 3], aai_id=True)
    obj_b = await create_object_from_value([4, 5, 6], aai_id=True)
    preview = _preview_operator_schema(obj_a.schema, obj_b.schema, "+")
    lazy = LazyOperator(lhs=obj_a, rhs=obj_b, operator="+", schema_preview=preview)

    with pytest.raises(RuntimeError, match="no table yet"):
        lazy.table


async def test_as_returns_new_lazy_with_name(ctx):
    obj_a = await create_object_from_value([1, 2, 3], aai_id=True)
    obj_b = await create_object_from_value([4, 5, 6], aai_id=True)
    preview = _preview_operator_schema(obj_a.schema, obj_b.schema, "+")
    lazy = LazyOperator(lhs=obj_a, rhs=obj_b, operator="+", schema_preview=preview)

    named = lazy.as_("daily_total")

    assert named is not lazy
    assert named._name == "daily_total"
    assert named._scope == "temp_named"
    # Receiver unchanged.
    assert lazy._name is None
    assert lazy._scope is None


async def test_as_with_explicit_scope(ctx):
    obj_a = await create_object_from_value([1, 2, 3], aai_id=True)
    obj_b = await create_object_from_value([4, 5, 6], aai_id=True)
    preview = _preview_operator_schema(obj_a.schema, obj_b.schema, "+")
    lazy = LazyOperator(lhs=obj_a, rhs=obj_b, operator="+", schema_preview=preview)

    job_scoped = lazy.as_("yearly", scope="job")
    assert job_scoped._name == "yearly"
    assert job_scoped._scope == "job"
