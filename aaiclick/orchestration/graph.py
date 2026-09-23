"""Pure graph flattening for the job graph view.

Imports no SQLModel: callers pass plain ids, so these functions stay
unit-testable without a database and the ``view_models`` → ``orchestration``
import boundary stays one-directional.

A dependency touching a ``Group`` is drawn as one edge to or from the group's
container node — it means every member waits, which is what the scheduler
enforces (``dependency_graph.py``). The layout engine cannot attach an edge to a
container, so each such edge is also expanded onto the group's direct member
tasks for positioning only. Groups reach the client as container nodes with a
status rolled up from every task beneath them.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping, Sequence
from typing import NamedTuple

from .models import (
    DEPENDENCY_GROUP,
    TASK_CANCELLED,
    TASK_CLAIMED,
    TASK_COMPLETED,
    TASK_FAILED,
    TASK_PENDING,
    TASK_RUNNING,
    TASK_UPSTREAM_FAILED,
    DependencyType,
    TaskStatus,
)

_WHITE, _GREY, _BLACK = 0, 1, 2


class DependencyRow(NamedTuple):
    """A ``Dependency`` row reduced to plain ids and kinds."""

    previous_id: int
    previous_type: DependencyType
    next_id: int
    next_type: DependencyType


class GraphEdge(NamedTuple):
    """A resolved task-to-task edge."""

    source_id: int
    target_id: int


def group_member_tasks(
    group_id: int,
    group_members: Mapping[int, set[int]],
    group_children: Mapping[int, set[int]],
) -> set[int]:
    """Task ids belonging to ``group_id``, descending through nested groups."""
    seen: set[int] = set()
    tasks: set[int] = set()
    stack = [group_id]
    while stack:
        current = stack.pop()
        if current in seen:
            continue
        seen.add(current)
        tasks |= group_members.get(current, set())
        stack.extend(group_children.get(current, set()))
    return tasks


def rollup_status(statuses: Iterable[TaskStatus]) -> TaskStatus:
    """Summarise member task statuses into one status for a group container.

    Activity outranks outcome — a group with a failure and a task still running
    reads as running, then fails once it settles — and among outcomes the worse
    wins. A cleanup state and a mix of finished and unstarted members both
    read as pending: neither is running, neither is done.
    """
    seen = set(statuses)
    if seen & {TASK_RUNNING, TASK_CLAIMED}:
        return TASK_RUNNING
    for outcome in (TASK_FAILED, TASK_UPSTREAM_FAILED, TASK_CANCELLED):
        if outcome in seen:
            return outcome
    if seen and seen <= {TASK_COMPLETED}:
        return TASK_COMPLETED
    return TASK_PENDING


def _member_edges(dep: DependencyRow, group_members: Mapping[int, set[int]]) -> set[GraphEdge]:
    """``dep`` rewritten onto task ids, a group standing for its direct members."""
    heads = group_members.get(dep.previous_id, set()) if dep.previous_type == DEPENDENCY_GROUP else {dep.previous_id}
    tails = group_members.get(dep.next_id, set()) if dep.next_type == DEPENDENCY_GROUP else {dep.next_id}
    return {GraphEdge(head, tail) for head in heads for tail in tails if head != tail}


def expand_dependencies(
    dependencies: Sequence[DependencyRow],
    group_members: Mapping[int, set[int]],
) -> list[GraphEdge]:
    """Rewrite group-touching dependencies onto the group's direct member tasks.

    ``A >> G`` becomes A → every member of G, ``G >> B`` every member → B, and
    ``G >> H`` every member of G → every member of H. Tasks in a nested group
    are not members of the parent, matching the scheduler.
    """
    edges: set[GraphEdge] = set()
    for dep in dependencies:
        edges |= _member_edges(dep, group_members)
    return sorted(edges)


def drop_cycle_edges(edges: Sequence[GraphEdge]) -> tuple[list[GraphEdge], int]:
    """Remove back-edges so the result is a DAG.

    Dependencies should already be acyclic, but a corrupt row must not reach
    dagre, which does not terminate cleanly on cycles. Iterative DFS — a linear
    chain longer than Python's recursion limit is a legitimate job shape.
    """
    ordered = sorted(edges)
    adjacency: dict[int, list[int]] = {}
    for edge in ordered:
        adjacency.setdefault(edge.source_id, []).append(edge.target_id)

    color: dict[int, int] = {}
    dropped: set[GraphEdge] = set()
    nodes = sorted({e.source_id for e in ordered} | {e.target_id for e in ordered})

    for root in nodes:
        if color.get(root, _WHITE) != _WHITE:
            continue
        color[root] = _GREY
        stack: list[tuple[int, Iterator[int]]] = [(root, iter(adjacency.get(root, ())))]
        while stack:
            node, remaining = stack[-1]
            nxt = next(remaining, None)
            if nxt is None:
                color[node] = _BLACK
                stack.pop()
            elif color.get(nxt, _WHITE) == _GREY:
                dropped.add(GraphEdge(node, nxt))
            elif color.get(nxt, _WHITE) == _WHITE:
                color[nxt] = _GREY
                stack.append((nxt, iter(adjacency.get(nxt, ()))))

    return [e for e in ordered if e not in dropped], len(dropped)


class GraphEdges(NamedTuple):
    """Edges to draw, and the task-level edges to lay them out with."""

    drawn: list[GraphEdge]
    """One per dependency; an endpoint may be a group id."""
    layout: list[GraphEdge]
    """Every dependency expanded onto member tasks, cycle-free."""
    dropped: int


def build_graph_edges(
    dependencies: Sequence[DependencyRow],
    group_members: Mapping[int, set[int]],
) -> GraphEdges:
    """Expand group dependencies for layout and drop any cycles.

    A dependency is drawn only while layout keeps at least one of its member
    edges — an empty group or a cycle-dropped edge has nothing to anchor it.
    """
    layout, dropped = drop_cycle_edges(expand_dependencies(dependencies, group_members))
    kept = set(layout)
    drawn = {GraphEdge(d.previous_id, d.next_id) for d in dependencies if _member_edges(d, group_members) & kept}
    return GraphEdges(sorted(drawn), layout, dropped)
