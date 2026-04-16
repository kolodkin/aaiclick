"""
Tests for ``aaiclick.oplog.lineage_forest`` — unit coverage of the pure
collapse / render helpers, plus a DB-backed test that runs a real
STRATEGY-mode pipeline and asserts ``build_forest`` returns the right
tree shape end-to-end.
"""

from __future__ import annotations

from aaiclick.data.data_context import create_object_from_value
from aaiclick.data.data_context.ch_client import create_ch_client
from aaiclick.oplog.lineage import lineage_context
from aaiclick.oplog.lineage_forest import (
    LineageNode,
    Route,
    _enumerate_paths,
    _extract_value,
    _format_values,
    _render_signature,
    build_and_render,
    build_forest,
    collapse_to_routes,
    render_routes,
)
from aaiclick.orchestration.orch_context import task_scope

# ---------------------------------------------------------------------
# Pure unit tests — no DB
# ---------------------------------------------------------------------


def _leaf(table: str, aai_id: int, value, role: str | None = None) -> LineageNode:
    return LineageNode(
        table=table,
        aai_id=aai_id,
        operation="source",
        role=role,
        values={"value": value},
    )


def test_enumerate_paths_single_leaf():
    leaf = _leaf("p_x", 1, 10)
    paths = _enumerate_paths(leaf)
    assert paths == [[leaf]]


def test_enumerate_paths_branching_tree():
    """A node with two children produces two leaf-to-root paths."""
    left = _leaf("p_left", 1, 10, role="left")
    right = _leaf("p_right", 2, 20, role="right")
    root = LineageNode(
        table="result",
        aai_id=99,
        operation="+",
        role=None,
        values={"value": 30},
        children={"left": left, "right": right},
    )

    paths = _enumerate_paths(root)

    assert len(paths) == 2
    # Each path is leaf-first with the root at the end.
    assert [p[0].table for p in paths] == ["p_left", "p_right"]
    assert all(p[-1] is root for p in paths)


def test_collapse_merges_structurally_identical_trees():
    """Two trees with the same (table, op, role) chain collapse to one route."""
    def _tree(root_id: int, leaf_id: int, leaf_val: float) -> LineageNode:
        leaf = _leaf("p_prices", leaf_id, leaf_val, role="left")
        return LineageNode(
            table="mul",
            aai_id=root_id,
            operation="*",
            role=None,
            values={"value": leaf_val * 2},
            children={"left": leaf},
        )

    forest = [_tree(100, 1, 10.0), _tree(101, 2, 20.0)]
    routes = collapse_to_routes(forest)

    assert len(routes) == 1
    route = routes[0]
    assert route.match_count == 2
    assert route.leaf_table == "p_prices"
    assert route.leaf_values == [10.0, 20.0]
    assert route.root_table == "mul"
    assert route.root_values == [20.0, 40.0]
    assert route.signature == (
        ("p_prices", "source", "left"),
        ("mul", "*", None),
    )


def test_collapse_splits_distinct_routes():
    """Each distinct role path at the root becomes its own route."""
    tree = LineageNode(
        table="result",
        aai_id=7,
        operation="+",
        role=None,
        values={"value": 30},
        children={
            "left": _leaf("p_a", 1, 10, role="left"),
            "right": _leaf("p_b", 2, 20, role="right"),
        },
    )

    routes = collapse_to_routes([tree])

    assert len(routes) == 2
    signatures = {r.signature for r in routes}
    assert (
        ("p_a", "source", "left"),
        ("result", "+", None),
    ) in signatures
    assert (
        ("p_b", "source", "right"),
        ("result", "+", None),
    ) in signatures


def test_extract_value_returns_first_column():
    node = LineageNode(
        table="t", aai_id=1, operation="source", role=None, values={"value": 42}
    )
    assert _extract_value(node) == 42


def test_extract_value_handles_missing_values():
    node = LineageNode(
        table="t", aai_id=1, operation="source", role=None, values=None
    )
    assert _extract_value(node) is None


def test_format_values_collapses_uniform_list():
    assert _format_values([5.0, 5.0, 5.0]) == "[5.0] (×3)"


def test_format_values_preserves_heterogeneous_list():
    assert _format_values([10.0, 20.0, 30.0]) == "[10.0, 20.0, 30.0]"


def test_format_values_handles_empty():
    assert _format_values([]) == "[]"


def test_render_signature_chains_hops():
    signature = (
        ("p_prices", "source", "left"),
        ("mul", "*", None),
        ("add_result", "+", "left"),
    )
    rendered = _render_signature(signature)
    # leaf table → * as `left` → mul → + as None (mul's role into +) → add_result
    assert "`p_prices`" in rendered
    assert "as `left`" in rendered
    assert "`*`" in rendered
    assert "`+`" in rendered
    assert "`add_result`" in rendered
    # The leaf's role annotates the first arrow, so `left` appears exactly once
    assert rendered.count("as `left`") == 1


def test_render_routes_empty():
    assert render_routes([]) == ""


def test_render_routes_includes_match_counts_and_exemplars():
    route = Route(
        signature=(
            ("p_prices", "source", "left"),
            ("mul", "*", None),
        ),
        match_count=2,
        leaf_table="p_prices",
        leaf_values=[10.0, 20.0],
        root_table="mul",
        root_values=[20.0, 40.0],
        exemplars=[
            [("p_prices", 1, 10.0), ("mul", 100, 20.0)],
            [("p_prices", 2, 20.0), ("mul", 101, 40.0)],
        ],
    )

    rendered = render_routes([route])

    assert "Unique routes: 1" in rendered
    assert "Total matched paths: 2" in rendered
    assert "matched: 2 rows" in rendered
    assert "p_prices" in rendered
    assert "#1=10.0" in rendered
    assert "#2=20.0" in rendered


def test_render_routes_truncates_when_above_exemplar_cap():
    """When match_count exceeds the exemplar cap the render notes hidden paths."""
    route = Route(
        signature=(("p_x", "source", None), ("t_y", "op", None)),
        match_count=12,
        leaf_table="p_x",
        leaf_values=[1, 2, 3, 4, 5],
        root_table="t_y",
        root_values=[10, 20, 30, 40, 50],
        exemplars=[
            [("p_x", 1, 1), ("t_y", 100, 10)],
            [("p_x", 2, 2), ("t_y", 101, 20)],
            [("p_x", 3, 3), ("t_y", 102, 30)],
            [("p_x", 4, 4), ("t_y", 103, 40)],
            [("p_x", 5, 5), ("t_y", 104, 50)],
        ],
    )

    rendered = render_routes([route])

    assert "exemplar paths (5 of 12):" in rendered
    assert "…and 7 more path(s)" in rendered


# ---------------------------------------------------------------------
# DB-backed: real STRATEGY-mode pipeline → real forest
# ---------------------------------------------------------------------


async def test_build_and_render_strategy_populated(orch_ctx):
    """A STRATEGY-mode pipeline yields a populated forest that collapses
    to one route per input role and renders them in the markdown output."""
    left_table = "p_forest_left"
    right_table = "p_forest_right"

    async with task_scope(
        task_id=1,
        job_id=1,
        run_id=100,
        sampling_strategy={left_table: "value >= 20"},
    ):
        a = await create_object_from_value([10, 20, 30], name="forest_left")
        b = await create_object_from_value([1, 2, 3], name="forest_right")
        result = await (a + b)
        result_table = result.table

    try:
        async with lineage_context():
            forest = await build_forest(result_table, job_id=1)
            rendered = await build_and_render(result_table, job_id=1)

        # Strategy matched rows where left.value >= 20 → aai_ids for 20 and 30.
        assert len(forest) == 2

        root_ops = {tree.operation for tree in forest}
        assert root_ops == {"+"}

        for tree in forest:
            assert set(tree.children.keys()) == {"left", "right"}
            for child in tree.children.values():
                # Each child is a persistent create_from_value leaf
                assert child.children == {}
                assert child.operation in ("source", "create_from_value")

        # Route collapse: 2 distinct signatures (left path, right path)
        routes = collapse_to_routes(forest)
        assert len(routes) == 2
        assert {r.leaf_table for r in routes} == {left_table, right_table}

        # Render output surfaces real aai_ids and the two routes
        assert "Unique routes: 2" in rendered
        assert left_table in rendered
        assert right_table in rendered
    finally:
        ch = await create_ch_client()
        await ch.command(f"DROP TABLE IF EXISTS {left_table}")
        await ch.command(f"DROP TABLE IF EXISTS {right_table}")


async def test_build_forest_none_mode_returns_empty(orch_ctx):
    """A job run under NONE mode has empty result_aai_ids, so the forest is empty."""
    async with task_scope(task_id=1, job_id=2, run_id=100):
        a = await create_object_from_value([1, 2, 3])
        b = await create_object_from_value([4, 5, 6])
        result = await a.concat(b)
        result_table = result.table

    async with lineage_context():
        forest = await build_forest(result_table, job_id=2)
        rendered = await build_and_render(result_table, job_id=2)

    assert forest == []
    assert rendered == ""
