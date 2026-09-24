"""Tests for pure graph flattening (``aaiclick.orchestration.graph``)."""

from __future__ import annotations

import pytest

from .graph import (
    DependencyRow,
    GraphEdge,
    build_graph_edges,
    drop_cycle_edges,
    group_member_tasks,
    rollup_status,
)
from .models import (
    DEPENDENCY_GROUP,
    DEPENDENCY_TASK,
    TASK_CANCELLED,
    TASK_CLAIMED,
    TASK_COMPLETED,
    TASK_FAILED,
    TASK_PENDING,
    TASK_PENDING_CANCELLED_CLEANUP,
    TASK_PENDING_FAILURE_CLEANUP,
    TASK_RUNNING,
    TASK_UPSTREAM_FAILED,
)


def _task_dep(previous_id: int, next_id: int) -> DependencyRow:
    return DependencyRow(previous_id, DEPENDENCY_TASK, next_id, DEPENDENCY_TASK)


@pytest.mark.parametrize(
    "dependencies, group_tasks, expected",
    [
        pytest.param([_task_dep(1, 2)], {}, [GraphEdge(1, 2)], id="task-to-task-passes-through"),
        # The scheduler matches ``tasks.group_id`` only, so a child group's tasks do not wait.
        pytest.param(
            [DependencyRow(3, DEPENDENCY_TASK, 10, DEPENDENCY_GROUP)],
            {10: {1}, 11: {2}},
            [GraphEdge(3, 1)],
            id="group-edge-skips-nested-group-tasks",
        ),
        pytest.param([DependencyRow(10, DEPENDENCY_GROUP, 3, DEPENDENCY_TASK)], {10: set()}, [], id="empty-group"),
        pytest.param([_task_dep(1, 2), _task_dep(1, 2)], {}, [GraphEdge(1, 2)], id="duplicates-collapse"),
    ],
)
def test_build_graph_edges_layout(dependencies, group_tasks, expected):
    assert build_graph_edges(dependencies, group_tasks).layout == expected


def test_group_member_tasks_includes_nested_children():
    members = group_member_tasks(10, {10: {1}, 11: {2, 3}}, {10: {11}})

    assert members == {1, 2, 3}


@pytest.mark.parametrize(
    "group_dep, expected",
    [
        pytest.param(
            DependencyRow(10, DEPENDENCY_GROUP, 3, DEPENDENCY_TASK),
            {GraphEdge(1, 3), GraphEdge(2, 3)},
            id="group-to-task-from-every-member",
        ),
        pytest.param(
            DependencyRow(3, DEPENDENCY_TASK, 10, DEPENDENCY_GROUP),
            {GraphEdge(3, 1), GraphEdge(3, 2)},
            id="task-to-group-into-every-member",
        ),
        pytest.param(
            DependencyRow(10, DEPENDENCY_GROUP, 11, DEPENDENCY_GROUP),
            {GraphEdge(1, 4), GraphEdge(1, 5), GraphEdge(2, 4), GraphEdge(2, 5)},
            id="group-to-group-every-pair",
        ),
    ],
)
def test_group_edges_expand_to_every_member_like_the_scheduler(group_dep, expected):
    """Intra-group ordering does not narrow a group edge: the scheduler makes every member wait."""
    dependencies = [_task_dep(1, 2), _task_dep(4, 5), group_dep]

    edges = build_graph_edges(dependencies, {10: {1, 2}, 11: {4, 5}}).layout

    assert set(edges) == {GraphEdge(1, 2), GraphEdge(4, 5)} | expected


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

    graph = build_graph_edges(dependencies, {})

    assert graph.dropped == 1
    assert len(graph.layout) == 1
    assert graph.drawn == graph.layout


def test_build_graph_edges_draws_group_edge_once_and_lays_it_out_per_member():
    dependencies = [_task_dep(1, 2), DependencyRow(3, DEPENDENCY_TASK, 10, DEPENDENCY_GROUP)]

    graph = build_graph_edges(dependencies, {10: {1, 2}})

    assert graph.drawn == [GraphEdge(1, 2), GraphEdge(3, 10)]
    assert graph.layout == [GraphEdge(1, 2), GraphEdge(3, 1), GraphEdge(3, 2)]


def test_build_graph_edges_skips_group_edge_with_nothing_to_anchor():
    """An empty group, or one whose every member edge is a dropped back-edge, draws nothing."""
    dependencies = [
        DependencyRow(3, DEPENDENCY_TASK, 10, DEPENDENCY_GROUP),
        _task_dep(4, 5),
        DependencyRow(5, DEPENDENCY_TASK, 11, DEPENDENCY_GROUP),
    ]

    graph = build_graph_edges(dependencies, {10: set(), 11: {4}})

    assert graph.dropped == 1
    assert graph.drawn == [GraphEdge(4, 5)]


@pytest.mark.parametrize(
    "statuses, expected",
    [
        pytest.param([TASK_FAILED, TASK_RUNNING], TASK_RUNNING, id="running-beats-failed"),
        pytest.param([TASK_PENDING, TASK_CLAIMED], TASK_RUNNING, id="claimed-counts-as-running"),
        pytest.param([TASK_COMPLETED, TASK_FAILED, TASK_UPSTREAM_FAILED], TASK_FAILED, id="failed-beats-upstream"),
        pytest.param([TASK_COMPLETED, TASK_UPSTREAM_FAILED, TASK_CANCELLED], TASK_UPSTREAM_FAILED, id="upstream"),
        pytest.param([TASK_COMPLETED, TASK_CANCELLED], TASK_CANCELLED, id="cancelled"),
        pytest.param([TASK_COMPLETED, TASK_COMPLETED], TASK_COMPLETED, id="all-completed"),
        pytest.param([TASK_COMPLETED, TASK_PENDING], TASK_PENDING, id="partial-progress-is-pending"),
        pytest.param([TASK_COMPLETED, TASK_PENDING_FAILURE_CLEANUP], TASK_PENDING, id="failure-cleanup-is-pending"),
        pytest.param([TASK_COMPLETED, TASK_PENDING_CANCELLED_CLEANUP], TASK_PENDING, id="cancelled-cleanup-is-pending"),
        pytest.param([], TASK_PENDING, id="empty"),
    ],
)
def test_rollup_status(statuses, expected):
    assert rollup_status(statuses) == expected
