"""Tests for the LazyOperator class and named operator results.

See ``docs/user_guide/object.md`` — "Lazy Operator Results".
"""

import pytest

from aaiclick import create_object_from_value
from aaiclick.data.models import (
    FIELDTYPE_ARRAY,
    FIELDTYPE_SCALAR,
)
from aaiclick.data.object import LazyOperator, operators
from aaiclick.data.object.schema_compute import _preview_operator_schema
from aaiclick.tenancy import DEFAULT_TENANT_ID

BINARY_OPERATORS = ["+", "-", "*", "/", "//", "%", "**", "==", "!=", "<", "<=", ">", ">=", "&", "|", "^"]


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
        _ = lazy.table


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


async def test_await_unnamed_lazy_materializes_to_temp(ctx):
    obj_a = await create_object_from_value([1, 2, 3], aai_id=True)
    obj_b = await create_object_from_value([4, 5, 6], aai_id=True)
    preview = _preview_operator_schema(obj_a.schema, obj_b.schema, "+")
    lazy = LazyOperator(lhs=obj_a, rhs=obj_b, operator="+", schema_preview=preview)

    result = await lazy
    assert result.table.startswith("t_")
    assert result.scope == "temp"
    assert await result.data() == [5, 7, 9]


async def test_await_with_as_temp_named(ctx):
    obj_a = await create_object_from_value([1, 2, 3], aai_id=True)
    obj_b = await create_object_from_value([4, 5, 6], aai_id=True)
    preview = _preview_operator_schema(obj_a.schema, obj_b.schema, "+")
    lazy = LazyOperator(lhs=obj_a, rhs=obj_b, operator="+", schema_preview=preview).as_("daily_total")

    result = await lazy
    assert result.table.startswith("t_daily_total_")
    assert result.scope == "temp_named"
    assert await result.data() == [5, 7, 9]


async def test_await_with_scope_job(ctx):
    obj_a = await create_object_from_value([1, 2, 3], aai_id=True)
    obj_b = await create_object_from_value([4, 5, 6], aai_id=True)
    preview = _preview_operator_schema(obj_a.schema, obj_b.schema, "+")
    lazy = LazyOperator(lhs=obj_a, rhs=obj_b, operator="+", schema_preview=preview).as_("yearly", scope="job")

    result = await lazy
    assert result.table.startswith("j_")
    assert result.table.endswith("_yearly")
    assert result.persistent is True
    assert await result.data() == [5, 7, 9]


async def test_re_await_is_idempotent(ctx):
    """Awaiting the same LazyOperator twice returns the same Object — no second table."""
    obj_a = await create_object_from_value([1, 2, 3], aai_id=True)
    obj_b = await create_object_from_value([4, 5, 6], aai_id=True)
    preview = _preview_operator_schema(obj_a.schema, obj_b.schema, "+")
    lazy = LazyOperator(lhs=obj_a, rhs=obj_b, operator="+", schema_preview=preview)

    first = await lazy
    second = await lazy
    assert first is second


async def test_chain_two_lazies_writes_two_tables(ctx):
    """`(a + b) + c` materializes inner then outer — two separate tables."""
    obj_a = await create_object_from_value([1, 2, 3], aai_id=True)
    obj_b = await create_object_from_value([10, 20, 30], aai_id=True)
    obj_c = await create_object_from_value([100, 200, 300], aai_id=True)

    inner_preview = _preview_operator_schema(obj_a.schema, obj_b.schema, "+")
    inner = LazyOperator(lhs=obj_a, rhs=obj_b, operator="+", schema_preview=inner_preview)

    outer_preview = _preview_operator_schema(inner.schema, obj_c.schema, "+")
    outer = LazyOperator(lhs=inner, rhs=obj_c, operator="+", schema_preview=outer_preview).as_("grand_total")

    result = await outer
    assert result.table.startswith("t_grand_total_")
    assert await result.data() == [111, 222, 333]
    # Inner was materialized too.
    assert inner._materialized is not None
    assert inner._materialized.table != result.table


async def test_lazy_never_awaited_creates_no_table(ctx):
    """Building a LazyOperator without awaiting writes nothing to ClickHouse.

    Asserts the LazyOperator's internal contract directly rather than counting
    ``system.tables`` — under parallel-xdist execution on a shared distributed
    backend, that count is racy against other workers.
    """
    obj_a = await create_object_from_value([1, 2, 3], aai_id=True)
    obj_b = await create_object_from_value([4, 5, 6], aai_id=True)
    preview = _preview_operator_schema(obj_a.schema, obj_b.schema, "+")

    lazy = LazyOperator(lhs=obj_a, rhs=obj_b, operator="+", schema_preview=preview)

    # No materialized table, no lifecycle registration, no incref — all marks of
    # zero DB activity.
    assert lazy._materialized is None
    assert lazy._registered is False
    assert lazy._owns_lifecycle_ref is False
    # Reading .table raises (rather than silently returning a name that doesn't exist).
    with pytest.raises(RuntimeError, match="no table yet"):
        _ = lazy.table


async def test_data_auto_materializes(ctx):
    """Calling .data() on an unawaited lazy materializes and returns rows."""
    obj_a = await create_object_from_value([1, 2, 3], aai_id=True)
    obj_b = await create_object_from_value([4, 5, 6], aai_id=True)
    preview = _preview_operator_schema(obj_a.schema, obj_b.schema, "+")
    lazy = LazyOperator(lhs=obj_a, rhs=obj_b, operator="+", schema_preview=preview)

    rows = await lazy.data()
    assert rows == [5, 7, 9]
    assert lazy._materialized is not None
    # Second call reuses the materialized Object.
    cached_table = lazy._materialized.table
    rows_again = await lazy.data()
    assert rows_again == [5, 7, 9]
    assert lazy._materialized.table == cached_table


async def test_result_auto_materializes(ctx):
    obj_a = await create_object_from_value([1, 2, 3], aai_id=True)
    obj_b = await create_object_from_value([4, 5, 6], aai_id=True)
    preview = _preview_operator_schema(obj_a.schema, obj_b.schema, "+")
    lazy = LazyOperator(lhs=obj_a, rhs=obj_b, operator="+", schema_preview=preview)

    result = await lazy.result()
    assert result is not None
    assert lazy._materialized is not None


# -----------------------------------------------------------------------------
# Public-syntax tests (dunders → LazyOperator)
# -----------------------------------------------------------------------------


async def test_add_returns_lazy_operator(ctx):
    obj_a = await create_object_from_value([1, 2, 3], aai_id=True)
    obj_b = await create_object_from_value([4, 5, 6], aai_id=True)
    lazy = obj_a + obj_b
    assert isinstance(lazy, LazyOperator)
    assert await lazy.data() == [5, 7, 9]


async def test_public_as_named_temp(ctx):
    obj_a = await create_object_from_value([1, 2, 3], aai_id=True)
    obj_b = await create_object_from_value([4, 5, 6], aai_id=True)
    result = await (obj_a + obj_b).as_("daily_total")
    assert result.table.startswith("t_daily_total_")
    assert await result.data() == [5, 7, 9]


async def test_public_as_scope_global(ctx):
    from aaiclick import delete_persistent_object

    # Pre-clean: a previous failed run could have left p_yearly_avg behind.
    await delete_persistent_object("yearly_avg", scope="global")
    try:
        obj_a = await create_object_from_value([1, 2, 3], aai_id=True)
        obj_b = await create_object_from_value([4, 5, 6], aai_id=True)
        result = await (obj_a + obj_b).as_("yearly_avg", scope="global")
        assert result.table == f"p_{DEFAULT_TENANT_ID}_yearly_avg"
        assert result.persistent is True
        assert await result.data() == [5, 7, 9]
    finally:
        await delete_persistent_object("yearly_avg", scope="global")


async def test_reverse_op_with_naming(ctx):
    """`(2 + a).as_('foo')` goes through __radd__; result is named."""
    obj_a = await create_object_from_value([1, 2, 3], aai_id=True)
    result = await (2 + obj_a).as_("rfoo")
    assert result.table.startswith("t_rfoo_")
    assert await result.data() == [3, 4, 5]


async def test_chain_via_public_operator(ctx):
    """(a + b) + c via public syntax → 2 tables, outer named."""
    obj_a = await create_object_from_value([1, 2, 3], aai_id=True)
    obj_b = await create_object_from_value([10, 20, 30], aai_id=True)
    obj_c = await create_object_from_value([100, 200, 300], aai_id=True)

    chain = (obj_a + obj_b) + obj_c
    assert isinstance(chain, LazyOperator)
    assert isinstance(chain.lhs, LazyOperator)
    assert chain.rhs is obj_c

    result = await chain.as_("total")
    assert result.table.startswith("t_total_")
    assert await result.data() == [111, 222, 333]


async def test_comparison_returns_lazy(ctx):
    obj_a = await create_object_from_value([1, 2, 3], aai_id=True)
    obj_b = await create_object_from_value([2, 2, 2], aai_id=True)
    lazy = obj_a < obj_b
    assert isinstance(lazy, LazyOperator)
    assert await lazy.data() == [True, False, False]


async def test_bitwise_returns_lazy(ctx):
    obj_a = await create_object_from_value([5, 6, 7], aai_id=True)
    obj_b = await create_object_from_value([3, 3, 3], aai_id=True)
    lazy = obj_a & obj_b
    assert isinstance(lazy, LazyOperator)
    assert await lazy.data() == [1, 2, 3]


async def test_lazy_operator_is_public_api():
    """LazyOperator is importable from the top-level package."""
    import aaiclick
    from aaiclick.data.object import LazyOperator as InternalLazy

    assert hasattr(aaiclick, "LazyOperator")
    assert aaiclick.LazyOperator is InternalLazy


# -----------------------------------------------------------------------------
# Phase 2: aggregations and unary transforms return LazyOperator
# -----------------------------------------------------------------------------

SIMPLE_AGG_METHODS = ["min", "max", "sum", "mean", "std", "var", "count"]
UNARY_NUMERIC_METHODS = ["abs", "log2", "sqrt"]
UNARY_STRING_METHODS = ["lower", "upper", "length", "trim"]


@pytest.mark.parametrize("method", SIMPLE_AGG_METHODS)
async def test_aggregation_returns_lazy_operator(ctx, method):
    """Calling an aggregation method on an Object returns a LazyOperator
    (no DB hit until await)."""
    obj = await create_object_from_value([1, 2, 3, 4, 5])
    lazy = getattr(obj, method)()
    assert isinstance(lazy, LazyOperator)
    assert lazy._materialized is None
    assert lazy.operator == method
    assert lazy.rhs is None


async def test_aggregation_as_named_temp(ctx):
    """obj.sum().as_('foo') materializes into t_foo_<snowflake>."""
    obj = await create_object_from_value([10, 20, 30, 40])
    result = await obj.sum().as_("daily_total")
    assert result.table.startswith("t_daily_total_")
    assert result.scope == "temp_named"
    assert await result.data() == 100


async def test_aggregation_as_scope_job(ctx):
    """obj.mean().as_('avg', scope='job') uses j_<job_id>_avg."""
    obj = await create_object_from_value([10, 20, 30, 40])
    result = await obj.mean().as_("avg", scope="job")
    assert result.table.startswith("j_")
    assert result.table.endswith("_avg")
    assert result.persistent is True
    assert await result.data() == 25.0


async def test_aggregation_data_auto_materializes(ctx):
    """The user's request: ``await obj.sum().data()`` works directly — no
    double-await needed (LazyOperator.data() materializes then reads)."""
    obj = await create_object_from_value([1, 2, 3, 4, 5])
    assert await obj.sum().data() == 15


async def test_double_await_pattern_still_works(ctx):
    """Legacy ``await (await obj.sum()).data()`` still works because
    LazyOperator is awaitable and resolves to a materialized Object."""
    obj = await create_object_from_value([1, 2, 3, 4, 5])
    materialized = await obj.sum()
    assert materialized.table.startswith("t_")
    assert await materialized.data() == 15


async def test_aggregation_on_lazy_chain(ctx):
    """(a + b).sum() — the fluent pattern. LazyOperator+LazyOperator =
    chained plan; awaiting materializes both."""
    obj_a = await create_object_from_value([1, 2, 3], aai_id=True)
    obj_b = await create_object_from_value([10, 20, 30], aai_id=True)
    chain = (obj_a + obj_b).sum()
    assert isinstance(chain, LazyOperator)
    assert chain.operator == "sum"
    assert isinstance(chain.lhs, LazyOperator)
    assert chain.lhs.operator == "+"
    assert await chain.data() == 66


async def test_aggregation_on_lazy_chain_named(ctx):
    """(a + b).sum().as_('total', scope='job') materializes both — the inner
    + into an unnamed temp, the outer sum into j_<job_id>_total."""
    obj_a = await create_object_from_value([1, 2, 3], aai_id=True)
    obj_b = await create_object_from_value([10, 20, 30], aai_id=True)
    result = await (obj_a + obj_b).sum().as_("total", scope="job")
    assert result.table.startswith("j_")
    assert result.table.endswith("_total")
    assert await result.data() == 66


@pytest.mark.parametrize("method", UNARY_NUMERIC_METHODS)
async def test_unary_numeric_returns_lazy_operator(ctx, method):
    obj = await create_object_from_value([1.0, 4.0, 9.0])
    lazy = getattr(obj, method)()
    assert isinstance(lazy, LazyOperator)
    assert lazy.operator == method
    assert lazy.rhs is None


@pytest.mark.parametrize("method", UNARY_STRING_METHODS)
async def test_unary_string_returns_lazy_operator(ctx, method):
    obj = await create_object_from_value(["  hello  ", "World", "foo"])
    lazy = getattr(obj, method)()
    assert isinstance(lazy, LazyOperator)
    assert lazy.operator == method
    assert lazy.rhs is None


async def test_unary_transform_as_named(ctx):
    """obj.lower().as_('lowered') names the result table."""
    obj = await create_object_from_value(["HELLO", "WORLD"])
    result = await obj.lower().as_("lowered")
    assert result.table.startswith("t_lowered_")
    assert await result.data() == ["hello", "world"]


async def test_unary_transform_data_auto_materializes(ctx):
    obj = await create_object_from_value(["HELLO", "WORLD"])
    assert await obj.upper().data() == ["HELLO", "WORLD"]


async def test_count_if_str_returns_lazy(ctx):
    obj = await create_object_from_value([1, 2, 3, 4, 5])
    lazy = obj.count_if("value > 3")
    assert isinstance(lazy, LazyOperator)
    assert lazy.operator == "count_if"
    assert lazy.params == {"condition": "value > 3"}
    assert await lazy.data() == 2


async def test_count_if_dict_returns_lazy_dict(ctx):
    obj = await create_object_from_value([1, 2, 3, 4, 5])
    lazy = obj.count_if({"small": "value <= 2", "large": "value >= 4"})
    assert isinstance(lazy, LazyOperator)
    # Schema preview should already reflect the dict columns.
    assert set(lazy.schema.columns) == {"small", "large"}
    rows = await lazy.data()
    assert rows == {"small": 2, "large": 2}


async def test_count_if_as_named(ctx):
    obj = await create_object_from_value([1, 2, 3, 4, 5])
    result = await obj.count_if("value > 3").as_("big_count")
    assert result.table.startswith("t_big_count_")
    assert await result.data() == 2


async def test_quantile_returns_lazy(ctx):
    obj = await create_object_from_value([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
    lazy = obj.quantile(0.5)
    assert isinstance(lazy, LazyOperator)
    assert lazy.operator == "quantile"
    assert lazy.params == {"q": 0.5}
    assert await lazy.data() == 5.5


async def test_quantile_as_named(ctx):
    obj = await create_object_from_value([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
    result = await obj.quantile(0.25).as_("q1")
    assert result.table.startswith("t_q1_")


async def test_unique_returns_lazy(ctx):
    obj = await create_object_from_value([1, 2, 2, 3, 3, 3, 4])
    lazy = obj.unique()
    assert isinstance(lazy, LazyOperator)
    assert lazy.operator == "unique"
    assert lazy.schema.fieldtype == FIELDTYPE_ARRAY
    assert sorted(await lazy.data()) == [1, 2, 3, 4]


async def test_unique_as_named(ctx):
    obj = await create_object_from_value([1, 2, 2, 3, 3, 3, 4])
    result = await obj.unique().as_("distinct_vals")
    assert result.table.startswith("t_distinct_vals_")
    assert sorted(await result.data()) == [1, 2, 3, 4]


async def test_nunique_returns_lazy(ctx):
    obj = await create_object_from_value([1, 2, 2, 3, 3, 3, 4])
    lazy = obj.nunique()
    assert isinstance(lazy, LazyOperator)
    assert lazy.operator == "nunique"
    assert lazy.schema.fieldtype == FIELDTYPE_SCALAR
    assert await lazy.data() == 4


async def test_nunique_as_named(ctx):
    obj = await create_object_from_value([1, 2, 2, 3, 3, 3, 4])
    result = await obj.nunique().as_("n_distinct")
    assert result.table.startswith("t_n_distinct_")
    assert await result.data() == 4


async def test_chained_unary_then_aggregation(ctx):
    """obj.abs().sum() builds two stacked LazyOperators."""
    obj = await create_object_from_value([-1.0, -2.0, 3.0, -4.0])
    chain = obj.abs().sum()
    assert isinstance(chain, LazyOperator)
    assert chain.operator == "sum"
    assert isinstance(chain.lhs, LazyOperator)
    assert chain.lhs.operator == "abs"
    assert await chain.data() == 10.0


async def assert_preview_matches_materialized(lazy, label=""):
    """The core LazyOperator contract: the schema computed at plan time is the
    schema the materialized result actually has."""
    preview = lazy.schema
    materialized = await lazy
    assert preview.fieldtype == materialized.schema.fieldtype, label
    assert set(preview.columns) == set(materialized.schema.columns), label
    for col in preview.columns:
        assert preview.columns[col].type == materialized.schema.columns[col].type, label


async def test_aggregation_preview_matches_materialized(ctx):
    """Pre-materialize schema preview must match the schema of the materialized result."""
    obj = await create_object_from_value([1, 2, 3, 4, 5])
    for method in SIMPLE_AGG_METHODS:
        await assert_preview_matches_materialized(getattr(obj, method)(), method)


async def test_aggregation_on_explode_view_matches_preview(ctx):
    """sum() over an exploded array column keeps the element type.

    The materializer reads the post-explode type from ``QueryInfo`` — the same
    input the preview used — rather than the pre-explode ``Array(...)`` type
    ``system.columns`` reports for the base table.
    """
    obj = await create_object_from_value([{"vals": [1, 2]}, {"vals": [3, 4]}])
    lazy = obj.explode("vals")["vals"].sum()
    result = await lazy
    assert lazy.schema.columns["value"].type == "Int64"
    assert result.schema.columns["value"].type == "Int64"
    assert await result.data() == 10


async def test_unary_preview_matches_materialized(ctx):
    obj = await create_object_from_value([1.0, 4.0, 9.0])
    for method in UNARY_NUMERIC_METHODS:
        await assert_preview_matches_materialized(getattr(obj, method)(), method)


# -----------------------------------------------------------------------------
# Phase 3: string/regex, null-check, isin, coalesce and array_map return LazyOperator
# -----------------------------------------------------------------------------

# (method, args) — one call each, since the methods differ in arity.
STRING_OP_CALLS = [
    pytest.param("match", ("^a",), id="match"),
    pytest.param("like", ("a%",), id="like"),
    pytest.param("ilike", ("A%",), id="ilike"),
    pytest.param("extract", ("(a.)",), id="extract"),
    pytest.param("replace", ("a", "z"), id="replace"),
]


@pytest.mark.parametrize("method, args", STRING_OP_CALLS)
async def test_string_op_returns_lazy_operator(ctx, method, args):
    """String/regex methods plan synchronously — no await, no DB round-trip."""
    obj = await create_object_from_value(["apple", "banana"])
    lazy = getattr(obj, method)(*args)
    assert isinstance(lazy, LazyOperator)
    assert lazy.operator == method
    assert lazy.rhs is None
    assert lazy._materialized is None


@pytest.mark.parametrize("method, args", STRING_OP_CALLS)
async def test_string_op_preview_matches_materialized(ctx, method, args):
    obj = await create_object_from_value(["apple", "banana"])
    await assert_preview_matches_materialized(getattr(obj, method)(*args), method)


async def test_string_op_as_named(ctx):
    """obj.match(p).as_('flags') names the result table."""
    obj = await create_object_from_value(["apple", "banana", "avocado"])
    result = await obj.match("^a").as_("flags")
    assert result.table.startswith("t_flags_")
    assert await result.data() == [1, 0, 1]


async def test_replace_carries_replacement_through_params(ctx):
    obj = await create_object_from_value(["hello world", "foo bar"])
    lazy = obj.replace(" ", "_")
    assert lazy.params == {"pattern": " ", "replacement": "_"}
    assert await lazy.data() == ["hello_world", "foo_bar"]


async def test_chained_unary_then_string_op(ctx):
    """obj.upper().like('A%') stacks a string op on a unary transform."""
    obj = await create_object_from_value(["apple", "banana"])
    chain = obj.upper().like("A%")
    assert isinstance(chain.lhs, LazyOperator)
    assert chain.lhs.operator == "upper"
    assert await chain.data() == [1, 0]


async def test_chained_string_ops(ctx):
    """extract() then match() — each stage stays lazy until the outer await."""
    obj = await create_object_from_value(["id:123", "id:abc"])
    chain = obj.extract("id:(.*)").match("^\\d+$")
    assert isinstance(chain.lhs, LazyOperator)
    assert chain.lhs._materialized is None
    assert await chain.data() == [1, 0]


@pytest.mark.parametrize("method", ["is_null", "is_not_null"])
async def test_null_check_returns_lazy_operator(ctx, method):
    obj = await create_object_from_value([1, 2, 3])
    lazy = getattr(obj, method)()
    assert isinstance(lazy, LazyOperator)
    assert lazy.operator == method
    assert lazy.rhs is None


async def test_null_check_as_named(ctx):
    obj = await create_object_from_value([1, 2, 3])
    result = await obj.is_not_null().as_("present")
    assert result.table.startswith("t_present_")
    assert await result.data() == [1, 1, 1]


async def test_isin_returns_lazy_operator(ctx):
    obj = await create_object_from_value(["a", "b", "c"])
    allowed = await create_object_from_value(["a", "c"])
    lazy = obj.isin(allowed)
    assert isinstance(lazy, LazyOperator)
    assert lazy.operator == "isin"
    assert lazy.rhs is allowed


async def test_isin_python_list_inlined_at_materialize(ctx):
    """A Python list rides along as the rhs and is inlined as SQL literals on await."""
    obj = await create_object_from_value(["a", "b", "c"])
    lazy = obj.isin(["a", "c"])
    assert lazy.rhs == ["a", "c"]
    assert await lazy.data() == [1, 0, 1]


async def test_isin_as_named(ctx):
    obj = await create_object_from_value(["a", "b", "c"])
    result = await obj.isin(["a", "c"]).as_("allowed")
    assert result.table.startswith("t_allowed_")
    assert await result.data() == [1, 0, 1]


async def test_coalesce_returns_lazy_operator(ctx):
    obj = await create_object_from_value([1, 2, 3])
    lazy = obj.coalesce(0)
    assert isinstance(lazy, LazyOperator)
    assert lazy.operator == "coalesce"
    assert lazy.rhs == 0


async def test_coalesce_as_named(ctx):
    obj = await create_object_from_value([1, 2, 3])
    result = await obj.coalesce(0).as_("filled")
    assert result.table.startswith("t_filled_")
    assert await result.data() == [1, 2, 3]


async def test_array_map_returns_lazy_operator(ctx):
    a = await create_object_from_value([1, 2, 3], aai_id=True)
    b = await create_object_from_value([10, 20, 30], aai_id=True)
    lazy = a.array_map(b, "+")
    assert isinstance(lazy, LazyOperator)
    assert lazy.operator == "array_map"
    assert lazy.params == {"operator": "+"}
    assert await lazy.data() == [11, 22, 33]


async def test_array_map_as_named(ctx):
    a = await create_object_from_value([1, 2, 3], aai_id=True)
    result = await a.array_map(10, "*").as_("scaled")
    assert result.table.startswith("t_scaled_")
    assert sorted(await result.data()) == [10, 20, 30]
