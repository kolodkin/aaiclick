"""Pure graph flattening for the job graph view.

Imports no SQLModel: callers pass plain ids, so these functions stay
unit-testable without a database and the ``view_models`` → ``orchestration``
import boundary stays one-directional.

v1 renders tasks only, so a dependency touching a ``Group`` is rewritten onto
member tasks. Expanding to *every* member would be quadratic and would
misrepresent ordering — a task deep inside a group would appear to depend
directly on an upstream node it never individually waits on.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from typing import NamedTuple

from .models import DEPENDENCY_GROUP, DEPENDENCY_TASK, DependencyType

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


def _inner_edges(members: set[int], task_edges: Sequence[GraphEdge]) -> list[GraphEdge]:
    return [e for e in task_edges if e.source_id in members and e.target_id in members]


def _sources(members: set[int], inner: Sequence[GraphEdge]) -> set[int]:
    """Members with no predecessor inside the group."""
    return members - {e.target_id for e in inner}


def _sinks(members: set[int], inner: Sequence[GraphEdge]) -> set[int]:
    """Members with no successor inside the group."""
    return members - {e.source_id for e in inner}


def expand_dependencies(
    dependencies: Sequence[DependencyRow],
    group_members: Mapping[int, set[int]],
    group_children: Mapping[int, set[int]],
) -> list[GraphEdge]:
    """Rewrite group-touching dependencies onto member tasks.

    ``G >> B`` becomes G's sinks → B; ``A >> G`` becomes A → G's sources;
    ``G >> H`` becomes G's sinks → H's sources.
    """
    task_edges = [
        GraphEdge(d.previous_id, d.next_id)
        for d in dependencies
        if d.previous_type == DEPENDENCY_TASK and d.next_type == DEPENDENCY_TASK
    ]
    edges: set[GraphEdge] = set(task_edges)

    for dep in dependencies:
        if dep.previous_type == DEPENDENCY_TASK and dep.next_type == DEPENDENCY_TASK:
            continue

        if dep.previous_type == DEPENDENCY_GROUP:
            members = group_member_tasks(dep.previous_id, group_members, group_children)
            heads = _sinks(members, _inner_edges(members, task_edges))
        else:
            heads = {dep.previous_id}

        if dep.next_type == DEPENDENCY_GROUP:
            members = group_member_tasks(dep.next_id, group_members, group_children)
            tails = _sources(members, _inner_edges(members, task_edges))
        else:
            tails = {dep.next_id}

        for head in heads:
            for tail in tails:
                if head != tail:
                    edges.add(GraphEdge(head, tail))

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
            descended = False
            for nxt in remaining:
                state = color.get(nxt, _WHITE)
                if state == _GREY:
                    dropped.add(GraphEdge(node, nxt))
                elif state == _WHITE:
                    color[nxt] = _GREY
                    stack.append((nxt, iter(adjacency.get(nxt, ()))))
                    descended = True
                    break
            if not descended:
                color[node] = _BLACK
                stack.pop()

    return [e for e in ordered if e not in dropped], len(dropped)


def build_graph_edges(
    dependencies: Sequence[DependencyRow],
    group_members: Mapping[int, set[int]],
    group_children: Mapping[int, set[int]],
) -> tuple[list[GraphEdge], int]:
    """Expand group dependencies, then drop any cycles. Returns (edges, dropped)."""
    return drop_cycle_edges(expand_dependencies(dependencies, group_members, group_children))
