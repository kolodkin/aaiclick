"""Tests for pure graph flattening (``aaiclick.orchestration.graph``)."""

from __future__ import annotations

from .graph import (
    DependencyRow,
    GraphEdge,
    build_graph_edges,
    drop_cycle_edges,
    expand_dependencies,
    group_member_tasks,
)
from .models import DEPENDENCY_GROUP, DEPENDENCY_TASK


def _task_dep(previous_id: int, next_id: int) -> DependencyRow:
    return DependencyRow(previous_id, DEPENDENCY_TASK, next_id, DEPENDENCY_TASK)


def test_task_to_task_dependencies_pass_through():
    edges = expand_dependencies([_task_dep(1, 2)], {}, {})

    assert edges == [GraphEdge(1, 2)]


def test_group_member_tasks_includes_nested_children():
    members = group_member_tasks(10, {10: {1}, 11: {2, 3}}, {10: {11}})

    assert members == {1, 2, 3}


def test_group_as_previous_expands_to_sink_tasks():
    """``G >> B`` means B waits for all of G, so only G's sinks gain an edge."""
    dependencies = [
        _task_dep(1, 2),
        DependencyRow(10, DEPENDENCY_GROUP, 3, DEPENDENCY_TASK),
    ]

    edges = expand_dependencies(dependencies, {10: {1, 2}}, {})

    assert GraphEdge(2, 3) in edges
    assert GraphEdge(1, 3) not in edges


def test_group_as_next_expands_to_source_tasks():
    """``A >> G`` means all of G waits for A, so only G's sources gain an edge."""
    dependencies = [
        _task_dep(1, 2),
        DependencyRow(3, DEPENDENCY_TASK, 10, DEPENDENCY_GROUP),
    ]

    edges = expand_dependencies(dependencies, {10: {1, 2}}, {})

    assert GraphEdge(3, 1) in edges
    assert GraphEdge(3, 2) not in edges


def test_group_to_group_connects_sinks_to_sources():
    dependencies = [
        _task_dep(1, 2),
        _task_dep(3, 4),
        DependencyRow(10, DEPENDENCY_GROUP, 11, DEPENDENCY_GROUP),
    ]

    edges = expand_dependencies(dependencies, {10: {1, 2}, 11: {3, 4}}, {})

    assert GraphEdge(2, 3) in edges
    assert GraphEdge(2, 4) not in edges
    assert GraphEdge(1, 3) not in edges


def test_empty_group_contributes_no_edges():
    dependencies = [DependencyRow(10, DEPENDENCY_GROUP, 3, DEPENDENCY_TASK)]

    edges = expand_dependencies(dependencies, {10: set()}, {})

    assert edges == []


def test_expansion_deduplicates_edges():
    dependencies = [_task_dep(1, 2), _task_dep(1, 2)]

    edges = expand_dependencies(dependencies, {}, {})

    assert edges == [GraphEdge(1, 2)]


def test_drop_cycle_edges_removes_back_edge_and_counts_it():
    kept, dropped = drop_cycle_edges([GraphEdge(1, 2), GraphEdge(2, 3), GraphEdge(3, 1)])

    assert dropped == 1
    assert len(kept) == 2


def test_drop_cycle_edges_keeps_acyclic_graph_intact():
    edges = [GraphEdge(1, 2), GraphEdge(1, 3), GraphEdge(2, 4), GraphEdge(3, 4)]

    kept, dropped = drop_cycle_edges(edges)

    assert dropped == 0
    assert kept == sorted(edges)


def test_drop_cycle_edges_handles_deep_chain_without_recursion_error():
    """A linear chain longer than Python's recursion limit must not blow the stack."""
    edges = [GraphEdge(i, i + 1) for i in range(1500)]

    kept, dropped = drop_cycle_edges(edges)

    assert dropped == 0
    assert len(kept) == 1500


def test_build_graph_edges_expands_then_drops_cycles():
    dependencies = [_task_dep(1, 2), _task_dep(2, 1)]

    edges, dropped = build_graph_edges(dependencies, {}, {})

    assert dropped == 1
    assert len(edges) == 1
