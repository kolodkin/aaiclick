# Job Graph View Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Render a job's task dependency DAG as an interactive graph inside `@job <name>`, alongside the existing tasks table.

**Architecture:** The server resolves graph semantics — it expands `Group` dependencies onto member tasks and drops cycles — and returns plain task nodes plus task-to-task edges from `GET /jobs/{ref}/graph`. The client owns geometry only: dagre computes coordinates inside a single isolated module, and React Flow renders. Layout is memoised on a structural key so 2 s polling re-colours nodes without repositioning them.

**Tech Stack:** Python 3 / FastAPI / SQLModel / pydantic on the server; React 19 + TypeScript + Vite 6 + TanStack Query on the client; `@xyflow/react` 12 (MIT) for rendering and `@dagrejs/dagre` 3 (MIT) for layout.

**Spec:** `docs/designs/job_graph_view.md`

## Global Constraints

- **v1 renders tasks only.** Groups are honoured semantically but never drawn. Group containers are deferred to `docs/designs/future.md`.
- **All imports at the top of the file.** No imports inside functions, methods, or test functions.
- **Prefer `Literal` over enums** for closed string sets, with module-level constants for the values.
- **Prefer `NamedTuple` over plain tuples** in function signatures; use named attribute access, not positional unpacking.
- **No `__all__` in `__init__.py`.**
- **Never use `Any`** to avoid proper typing.
- **Tests live alongside the module** they test (`aaiclick/orchestration/test_graph.py` tests `aaiclick/orchestration/graph.py`). Flat module-level `def test_*` functions — no test classes.
- **Never use `@pytest.mark.asyncio`** — `pytest-asyncio` auto-detects async test functions.
- **`filterwarnings = ["error"]`** — any unhandled warning fails the test run.
- **Do not weaken unrelated failing tests.** If tests outside this work break, fix the implementation.
- **No history comments** (`# Removed: ...`). Version control tracks history.
- **Positions are never computed server-side.** Coordinates stay in `src/lib/graphLayout.ts`.
- **`src/api/schema.ts` is generated** — never hand-edit. Run `npm run gen-types` after view models change; CI fails on drift.

!!! warning "There is no JavaScript test runner in this project"
    Vitest is deferred (`docs/designs/future.md`). Frontend tasks are verified by `npm run check` (the `tsc --noEmit` CI gate) and the Playwright e2e suite. Do not invent a `vitest` or `jest` command — it will not exist. All genuinely testable logic (group expansion, cycle dropping) is deliberately placed server-side in Python where it can be unit-tested.

---

## File Structure

**Backend**

| File | Responsibility |
|-------------------------------------------------|-----------------------------------------------------|
| `aaiclick/orchestration/graph.py` (create)       | Pure flattening: group expansion, cycle dropping. Imports no SQLModel. |
| `aaiclick/orchestration/test_graph.py` (create)  | Unit tests for the above                            |
| `aaiclick/orchestration/view_models.py` (modify) | `GraphNodeView`, `GraphEdgeView`, `JobGraphView`, adapter |
| `aaiclick/internal_api/jobs.py` (modify)         | `get_job_graph(ref)` — loads rows, delegates        |
| `aaiclick/internal_api/test_jobs_graph.py` (create) | Integration tests over a real job                |
| `aaiclick/server/routers/jobs.py` (modify)       | `GET /jobs/{ref}/graph` thin pass-through           |

**Frontend**

| File | Responsibility |
|--------------------------------------------|--------------------------------------------------|
| `package.json` (modify)                    | Add `@xyflow/react`, `@dagrejs/dagre`             |
| `src/lib/graphLayout.ts` (create)          | **Only** dagre import; layout + structural key    |
| `src/api/types.ts` (modify)                | Re-export `JobGraphView` etc.                     |
| `src/api/hooks.ts` (modify)                | `useJobGraph(ref)`                                |
| `src/components/graph/TaskNode.tsx` (create) | Custom React Flow node                          |
| `src/components/graph/JobGraph.tsx` (create) | ReactFlow wrapper: data → layout → render       |
| `src/prompt.ts` (modify)                   | `view: "table" \| "graph"` on the `job` route     |
| `src/App.tsx` (modify)                     | Pass `view` through to `JobDetail`                |
| `src/views/JobDetail.tsx` (modify)         | Table/Graph toggle                                |
| `src/styles/globals.css` (modify)          | Graph node styling                                |
| `test_e2e/web/test_smoke.py` (modify)      | Graph view smoke coverage                         |

---

### Task 1: Graph flattening core

Pure functions over plain ids. No database, no SQLModel, no fixtures — every behaviour here is exhaustively testable in isolation. This is the only genuinely subtle logic in the feature.

**Files:**
- Create: `aaiclick/orchestration/graph.py`
- Test: `aaiclick/orchestration/test_graph.py`

**Interfaces:**
- Consumes: `DependencyType`, `DEPENDENCY_TASK`, `DEPENDENCY_GROUP` from `aaiclick/orchestration/models.py`
- Produces:
  - `class DependencyRow(NamedTuple)` — `previous_id: int`, `previous_type: DependencyType`, `next_id: int`, `next_type: DependencyType`
  - `class GraphEdge(NamedTuple)` — `source_id: int`, `target_id: int`
  - `group_member_tasks(group_id: int, group_members: Mapping[int, set[int]], group_children: Mapping[int, set[int]]) -> set[int]`
  - `expand_dependencies(dependencies: Sequence[DependencyRow], group_members: Mapping[int, set[int]], group_children: Mapping[int, set[int]]) -> list[GraphEdge]`
  - `drop_cycle_edges(edges: Sequence[GraphEdge]) -> tuple[list[GraphEdge], int]`
  - `build_graph_edges(dependencies: Sequence[DependencyRow], group_members: Mapping[int, set[int]], group_children: Mapping[int, set[int]]) -> tuple[list[GraphEdge], int]`

- [ ] **Step 1: Write the failing tests**

Create `aaiclick/orchestration/test_graph.py`:

```python
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest aaiclick/orchestration/test_graph.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'aaiclick.orchestration.graph'`

- [ ] **Step 3: Write the implementation**

Create `aaiclick/orchestration/graph.py`:

```python
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


def _sources(members: set[int], inner: Sequence[GraphEdge]) -> set[int]:
    """Members with no predecessor inside the group."""
    return members - {e.target_id for e in inner}


def _sinks(members: set[int], inner: Sequence[GraphEdge]) -> set[int]:
    """Members with no successor inside the group."""
    return members - {e.source_id for e in inner}


def _inner_edges(members: set[int], task_edges: Sequence[GraphEdge]) -> list[GraphEdge]:
    return [e for e in task_edges if e.source_id in members and e.target_id in members]


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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest aaiclick/orchestration/test_graph.py -v`
Expected: PASS — 11 passed

- [ ] **Step 5: Run linters**

Run: `uv run ruff check aaiclick/orchestration/graph.py aaiclick/orchestration/test_graph.py && uv run ruff format --check aaiclick/orchestration/graph.py aaiclick/orchestration/test_graph.py && uv run pyright aaiclick/orchestration/graph.py`
Expected: no errors

- [ ] **Step 6: Commit**

```bash
git add aaiclick/orchestration/graph.py aaiclick/orchestration/test_graph.py
git commit -m "Add pure graph flattening for the job graph view"
```

---

### Task 2: Graph view models and adapter

**Files:**
- Modify: `aaiclick/orchestration/view_models.py`
- Test: `aaiclick/orchestration/test_view_models.py`

**Interfaces:**
- Consumes: `DependencyRow`, `GraphEdge`, `build_graph_edges` from Task 1
- Produces:
  - `GRAPH_NODE_TASK = "task"`, `GRAPH_NODE_GROUP = "group"`, `GraphNodeKind = Literal["task", "group"]`
  - `class GraphNodeView(BaseModel)`, `class GraphEdgeView(BaseModel)`, `class JobGraphView(BaseModel)`
  - `build_job_graph_view(job: Job, tasks: list[Task], groups: list[Group], dependencies: list[Dependency]) -> JobGraphView`

- [ ] **Step 1: Write the failing test**

Append to `aaiclick/orchestration/test_view_models.py`:

```python
def test_build_job_graph_view_expands_group_dependency_onto_sink_task():
    """A group→task dependency must reach the graph as a task→task edge."""
    job = Job(id=1, name="graph_job")
    tasks = [
        Task(id=101, job_id=1, group_id=200, entrypoint="m.a", name="a"),
        Task(id=102, job_id=1, group_id=200, entrypoint="m.b", name="b"),
        Task(id=103, job_id=1, entrypoint="m.c", name="c"),
    ]
    groups = [Group(id=200, job_id=1, name="g")]
    dependencies = [
        Dependency(previous_id=101, previous_type=DEPENDENCY_TASK, next_id=102, next_type=DEPENDENCY_TASK),
        Dependency(previous_id=200, previous_type=DEPENDENCY_GROUP, next_id=103, next_type=DEPENDENCY_TASK),
    ]

    view = build_job_graph_view(job, tasks, groups, dependencies)

    assert {n.id for n in view.nodes} == {101, 102, 103}
    assert all(n.kind == GRAPH_NODE_TASK for n in view.nodes)
    edges = {(e.source_id, e.target_id) for e in view.edges}
    assert (102, 103) in edges
    assert (101, 103) not in edges
    assert view.dropped_cycle_edges == 0


def test_build_job_graph_view_carries_parent_group_id_for_future_containers():
    job = Job(id=1, name="graph_job")
    tasks = [Task(id=101, job_id=1, group_id=200, entrypoint="m.a", name="a")]
    groups = [Group(id=200, job_id=1, name="g")]

    view = build_job_graph_view(job, tasks, groups, [])

    assert view.nodes[0].parent_group_id == 200
```

The two tests need these names. Merge them into `test_view_models.py`'s
existing `from .models import (...)` and `from .view_models import (...)`
statements rather than adding new ones — `ruff` flags duplicate imports:

- From `.models`: `DEPENDENCY_GROUP`, `DEPENDENCY_TASK`, `Dependency`, `Group`, `Job`, `Task`
- From `.view_models`: `GRAPH_NODE_TASK`, `build_job_graph_view`

Read the file's current import block first and add only the names not already
present.

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest aaiclick/orchestration/test_view_models.py -k graph -v`
Expected: FAIL — `ImportError: cannot import name 'build_job_graph_view'`

- [ ] **Step 3: Write the implementation**

Add to `aaiclick/orchestration/view_models.py`. Extend the existing `from .models import (...)` block with `Dependency` and `Group`, add `from typing import Literal` to the typing import, and add `from .graph import DependencyRow, build_graph_edges`:

```python
GRAPH_NODE_TASK = "task"
GRAPH_NODE_GROUP = "group"
GraphNodeKind = Literal["task", "group"]


class GraphNodeView(BaseModel):
    """A node in the job graph. v1 emits only ``"task"`` nodes."""

    id: SnowflakeId
    kind: GraphNodeKind
    name: str
    parent_group_id: SnowflakeId | None = None
    status: TaskStatus
    entrypoint: str
    attempt: int
    started_at: datetime | None = None
    completed_at: datetime | None = None
    error: str | None = None
    is_image_build: bool = False


class GraphEdgeView(BaseModel):
    """A resolved task-to-task edge."""

    source_id: SnowflakeId
    target_id: SnowflakeId


class JobGraphView(BaseModel):
    """Job dependency graph served by ``GET /jobs/{ref}/graph``."""

    job_id: SnowflakeId
    nodes: list[GraphNodeView] = Field(default_factory=list)
    edges: list[GraphEdgeView] = Field(default_factory=list)
    dropped_cycle_edges: int = 0


def task_to_graph_node(task: Task) -> GraphNodeView:
    return GraphNodeView(
        id=task.id,
        kind=GRAPH_NODE_TASK,
        name=task.name,
        parent_group_id=task.group_id,
        status=task.status,
        entrypoint=task.entrypoint,
        attempt=task.attempt,
        started_at=task.started_at,
        completed_at=task.completed_at,
        error=task.error,
        is_image_build=task.is_image_build,
    )


def build_job_graph_view(
    job: Job,
    tasks: list[Task],
    groups: list[Group],
    dependencies: list[Dependency],
) -> JobGraphView:
    """Resolve the job's DAG into task nodes and task-to-task edges."""
    group_members: dict[int, set[int]] = {g.id: set() for g in groups}
    for task in tasks:
        if task.group_id is not None:
            group_members.setdefault(task.group_id, set()).add(task.id)

    group_children: dict[int, set[int]] = {g.id: set() for g in groups}
    for group in groups:
        if group.parent_group_id is not None:
            group_children.setdefault(group.parent_group_id, set()).add(group.id)

    rows = [
        DependencyRow(d.previous_id, d.previous_type, d.next_id, d.next_type)
        for d in dependencies
    ]
    edges, dropped = build_graph_edges(rows, group_members, group_children)

    known = {t.id for t in tasks}
    return JobGraphView(
        job_id=job.id,
        nodes=[task_to_graph_node(t) for t in tasks],
        edges=[
            GraphEdgeView(source_id=e.source_id, target_id=e.target_id)
            for e in edges
            if e.source_id in known and e.target_id in known
        ],
        dropped_cycle_edges=dropped,
    )
```

The `known` filter drops edges pointing at tasks outside this job — a dependency row can reference a task removed by a retention sweep, and React Flow throws on an edge whose endpoint is missing.

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest aaiclick/orchestration/test_view_models.py -v`
Expected: PASS — including the two new graph tests

- [ ] **Step 5: Run linters**

Run: `uv run ruff check aaiclick/orchestration/ && uv run pyright aaiclick/orchestration/view_models.py`
Expected: no errors

- [ ] **Step 6: Commit**

```bash
git add aaiclick/orchestration/view_models.py aaiclick/orchestration/test_view_models.py
git commit -m "Add job graph view models and adapter"
```

---

### Task 3: Internal API and REST route

**Files:**
- Modify: `aaiclick/internal_api/jobs.py`
- Modify: `aaiclick/server/routers/jobs.py`
- Test: `aaiclick/internal_api/test_jobs_graph.py` (create)

**Interfaces:**
- Consumes: `JobGraphView`, `build_job_graph_view` from Task 2
- Produces: `jobs.get_job_graph(ref: RefId) -> JobGraphView`; route `GET /api/v0/jobs/{ref}/graph`

- [ ] **Step 1: Write the failing test**

Create `aaiclick/internal_api/test_jobs_graph.py`:

```python
"""Tests for ``aaiclick.internal_api.jobs.get_job_graph``."""

from __future__ import annotations

import pytest

from aaiclick.orchestration.factories import create_job, create_task
from aaiclick.orchestration.orch_context import commit_tasks
from aaiclick.orchestration.view_models import GRAPH_NODE_TASK, JobGraphView

from . import errors, jobs

_SAMPLE_TASK = "aaiclick.orchestration.fixtures.sample_tasks.simple_task"


async def test_get_job_graph_returns_nodes_and_edges(orch_ctx):
    job = await create_job("graph_pipeline", _SAMPLE_TASK)
    raw = create_task(_SAMPLE_TASK)
    transform = create_task(_SAMPLE_TASK)
    report = create_task(_SAMPLE_TASK)
    raw >> transform >> report
    await commit_tasks(report, job_id=job.id)

    graph = await jobs.get_job_graph(job.id)

    assert isinstance(graph, JobGraphView)
    assert graph.job_id == job.id
    node_ids = {n.id for n in graph.nodes}
    assert {raw.id, transform.id, report.id} <= node_ids
    edges = {(e.source_id, e.target_id) for e in graph.edges}
    assert (raw.id, transform.id) in edges
    assert (transform.id, report.id) in edges
    assert all(n.kind == GRAPH_NODE_TASK for n in graph.nodes)


async def test_get_job_graph_resolves_by_name(orch_ctx):
    job = await create_job("graph_by_name", _SAMPLE_TASK)
    only = create_task(_SAMPLE_TASK)
    await commit_tasks(only, job_id=job.id)

    graph = await jobs.get_job_graph("graph_by_name")

    assert graph.job_id == job.id


async def test_get_job_graph_missing_job_raises_not_found(orch_ctx):
    with pytest.raises(errors.NotFound):
        await jobs.get_job_graph("no_such_job_at_all")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest aaiclick/internal_api/test_jobs_graph.py -v`
Expected: FAIL — `AttributeError: module 'aaiclick.internal_api.jobs' has no attribute 'get_job_graph'`

- [ ] **Step 3: Implement `get_job_graph`**

In `aaiclick/internal_api/jobs.py`, extend the existing `aaiclick.orchestration.models` import with `Dependency` and `Group`, extend the `view_models` import with `JobGraphView` and `build_job_graph_view`, then add:

```python
async def get_job_graph(ref: RefId) -> JobGraphView:
    """Return the job's dependency graph as task nodes and task-to-task edges.

    Group dependencies are expanded onto member tasks server-side — the client
    receives no ``Group`` or ``Dependency`` rows.
    """
    async with get_sql_session() as session:
        job = await _resolve_job(ref, session)
        if job is None:
            raise NotFound(f"Job not found: {ref}")
        tasks = list(
            (await session.execute(select(Task).where(Task.job_id == job.id).order_by(col(Task.created_at))))
            .scalars()
            .all()
        )
        groups = list(
            (await session.execute(select(Group).where(Group.job_id == job.id))).scalars().all()
        )
        task_ids = [t.id for t in tasks]
        group_ids = [g.id for g in groups]
        dependencies = list(
            (
                await session.execute(
                    select(Dependency).where(
                        col(Dependency.next_id).in_(task_ids + group_ids)
                        | col(Dependency.previous_id).in_(task_ids + group_ids)
                    )
                )
            )
            .scalars()
            .all()
        )
    return build_job_graph_view(job, tasks, groups, dependencies)
```

!!! warning "`Dependency` has no `job_id` column"
    Dependencies are scoped by their endpoint ids, not by job. Filtering on `job_id` will not compile — select on `previous_id` / `next_id` membership as shown. Guard the empty case: if `task_ids + group_ids` is empty, SQLAlchemy emits an `IN ()` that some backends reject, so return early when `tasks` and `groups` are both empty.

Add that guard directly before the dependency query:

```python
        if not task_ids and not group_ids:
            return JobGraphView(job_id=job.id)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest aaiclick/internal_api/test_jobs_graph.py -v`
Expected: PASS — 3 passed

- [ ] **Step 5: Add the REST route**

In `aaiclick/server/routers/jobs.py`, extend the `view_models` import with `JobGraphView` and add after `job_stats`:

```python
@router.get(
    "/{ref}/graph",
    response_model=JobGraphView,
    responses=problem_responses(404),
    dependencies=[Depends(orch_scope)],
)
async def job_graph(ref: RefId) -> JobGraphView:
    return await jobs_api.get_job_graph(ref)
```

- [ ] **Step 6: Verify the route is registered**

Run: `uv run --extra server python -m aaiclick.server.dump_openapi | python -c "import json,sys; print('/api/v0/jobs/{ref}/graph' in json.load(sys.stdin)['paths'])"`
Expected: `True`

- [ ] **Step 7: Run linters and the full backend suite**

Run: `uv run ruff check aaiclick/ && uv run pyright aaiclick/internal_api/jobs.py aaiclick/server/routers/jobs.py && uv run pytest aaiclick/internal_api aaiclick/orchestration -q`
Expected: no lint errors, all tests pass

- [ ] **Step 8: Commit**

```bash
git add aaiclick/internal_api/jobs.py aaiclick/internal_api/test_jobs_graph.py aaiclick/server/routers/jobs.py
git commit -m "Add GET /jobs/{ref}/graph endpoint"
```

---

### Task 4: Frontend dependencies and the isolated layout module

`graphLayout.ts` is the only file in the codebase permitted to import dagre. Confining the engine here is what makes it swappable later.

**Files:**
- Modify: `package.json`
- Create: `src/lib/graphLayout.ts`
- Modify: `src/api/types.ts`
- Modify: `src/api/hooks.ts`

**Interfaces:**
- Consumes: `JobGraphView` from the regenerated `schema.ts` (Task 3 must be committed first)
- Produces:
  - `NODE_SIZE: { width: number; height: number }`
  - `structuralKey(nodes: LayoutNode[], edges: LayoutEdge[]): string`
  - `layout(nodes: LayoutNode[], edges: LayoutEdge[]): Map<string, { x: number; y: number }>`
  - `useJobGraph(ref: string)` returning `JobGraphView`

- [ ] **Step 1: Install the dependencies**

Run: `npm install @xyflow/react@^12 @dagrejs/dagre@^3`
Expected: both added to `dependencies` in `package.json`

- [ ] **Step 2: Regenerate the API types**

Run: `npm run gen-types`
Expected: `src/api/schema.ts` gains `JobGraphView`, `GraphNodeView`, `GraphEdgeView`

- [ ] **Step 3: Add the type re-exports**

In `src/api/types.ts`, add below the existing view re-exports:

```ts
export type JobGraphView = S["JobGraphView"];
export type GraphNodeView = S["GraphNodeView"];
export type GraphEdgeView = S["GraphEdgeView"];
```

- [ ] **Step 4: Write the layout module**

Create `src/lib/graphLayout.ts`:

```ts
// The ONLY module permitted to import a layout engine. Everything else works
// through `layout()` and `structuralKey()`, so replacing dagre is a one-file
// change — see docs/designs/job_graph_view.md.
import dagre from "@dagrejs/dagre";

export interface LayoutNode {
  id: string;
}

export interface LayoutEdge {
  source: string;
  target: string;
}

export const NODE_SIZE = { width: 220, height: 76 };

// Identifies the graph's *shape*. Status changes leave this untouched, so a
// 2 s poll re-colours nodes in place instead of repositioning them mid-read.
// Only structural change — dynamic tasks registering at runtime — re-lays out.
export function structuralKey(nodes: LayoutNode[], edges: LayoutEdge[]): string {
  const n = nodes
    .map((x) => x.id)
    .sort()
    .join(",");
  const e = edges
    .map((x) => `${x.source}>${x.target}`)
    .sort()
    .join(",");
  return `${n}|${e}`;
}

export function layout(nodes: LayoutNode[], edges: LayoutEdge[]): Map<string, { x: number; y: number }> {
  const g = new dagre.graphlib.Graph();
  g.setGraph({ rankdir: "LR", nodesep: 24, ranksep: 72 });
  g.setDefaultEdgeLabel(() => ({}));

  for (const node of nodes) {
    g.setNode(node.id, { width: NODE_SIZE.width, height: NODE_SIZE.height });
  }
  for (const edge of edges) {
    g.setEdge(edge.source, edge.target);
  }

  dagre.layout(g);

  const positions = new Map<string, { x: number; y: number }>();
  for (const node of nodes) {
    const placed = g.node(node.id);
    // dagre returns node centres; React Flow positions by top-left corner.
    positions.set(node.id, {
      x: placed.x - NODE_SIZE.width / 2,
      y: placed.y - NODE_SIZE.height / 2,
    });
  }
  return positions;
}
```

- [ ] **Step 5: Add the query hook**

In `src/api/hooks.ts`, add `JobGraphView` to the type import block and append:

```ts
export function useJobGraph(ref: string) {
  return useQuery({
    queryKey: ["job-graph", ref],
    queryFn: () => fetchJSON<JobGraphView>(`/jobs/${encodeURIComponent(ref)}/graph`),
    enabled: ref.length > 0,
  });
}
```

- [ ] **Step 6: Type-check**

Run: `npm run check`
Expected: no errors

- [ ] **Step 7: Commit**

```bash
git add package.json package-lock.json src/api/schema.ts src/api/types.ts src/api/hooks.ts src/lib/graphLayout.ts
git commit -m "Add React Flow and dagre with an isolated layout module"
```

---

### Task 5: Graph components

**Files:**
- Create: `src/components/graph/TaskNode.tsx`
- Create: `src/components/graph/JobGraph.tsx`
- Modify: `src/styles/globals.css`

**Interfaces:**
- Consumes: `layout`, `structuralKey`, `useJobGraph` from Task 4; `StatusBadge` from `src/components/StatusBadge.tsx`; `durationBetween` from `src/lib/format.ts`. Node dimensions live in `NODE_SIZE` (Task 4) and are mirrored by the `.gnode` CSS rule in Step 3 — if you change one, change both, or dagre will reserve the wrong space.
- Produces: `JobGraph({ refId, onPrompt }: { refId: string; onPrompt: (v: string) => void })`

- [ ] **Step 1: Write the node component**

Create `src/components/graph/TaskNode.tsx`:

```tsx
import { Handle, Position, type NodeProps, type Node } from "@xyflow/react";
import type { GraphNodeView } from "../../api/types";
import { StatusBadge } from "../StatusBadge";
import { durationBetween } from "../../lib/format";

export type TaskNodeData = { view: GraphNodeView };
export type TaskNodeType = Node<TaskNodeData, "task">;

function shortEntrypoint(entrypoint: string): string {
  if (entrypoint.includes(":")) return entrypoint.split(":").pop() ?? entrypoint;
  if (entrypoint.includes(".")) return entrypoint.split(".").pop() ?? entrypoint;
  return entrypoint;
}

export function TaskNode({ data }: NodeProps<TaskNodeType>) {
  const { view } = data;
  return (
    <div className={`gnode gnode-${view.status}${view.is_image_build ? " gnode-build" : ""}`}>
      <Handle type="target" position={Position.Left} />
      <div className="gnode-head">
        <span className="mono gnode-name">{view.name}</span>
        <StatusBadge status={view.status} reason={view.error} />
      </div>
      <div className="gnode-meta">
        <span className="mono">{shortEntrypoint(view.entrypoint)}</span>
        <span>{durationBetween(view.started_at, view.completed_at)}</span>
        {view.attempt > 1 && <span>attempt {view.attempt}</span>}
      </div>
      <Handle type="source" position={Position.Right} />
    </div>
  );
}
```

- [ ] **Step 2: Write the graph wrapper**

Create `src/components/graph/JobGraph.tsx`:

```tsx
import { useMemo } from "react";
import { Background, Controls, ReactFlow, type Edge } from "@xyflow/react";
import "@xyflow/react/dist/style.css";
import { useJobGraph } from "../../api/hooks";
import { layout, structuralKey } from "../../lib/graphLayout";
import { TaskNode, type TaskNodeType } from "./TaskNode";

const NODE_TYPES = { task: TaskNode };
const CROWDED_NODE_COUNT = 300;

export function JobGraph({ refId, onPrompt }: { refId: string; onPrompt: (v: string) => void }) {
  const { data, isLoading, isError } = useJobGraph(refId);

  const rawNodes = data?.nodes ?? [];
  const rawEdges = data?.edges ?? [];

  const layoutNodes = useMemo(() => rawNodes.map((n) => ({ id: String(n.id) })), [rawNodes]);
  const layoutEdges = useMemo(
    () => rawEdges.map((e) => ({ source: String(e.source_id), target: String(e.target_id) })),
    [rawEdges],
  );

  const key = useMemo(() => structuralKey(layoutNodes, layoutEdges), [layoutNodes, layoutEdges]);

  // Deliberately keyed on structure alone. Including the node data here would
  // re-run dagre on every 2 s poll and shuffle nodes while an operator reads
  // them — see docs/designs/job_graph_view.md.
  // eslint-disable-next-line react-hooks/exhaustive-deps
  const positions = useMemo(() => layout(layoutNodes, layoutEdges), [key]);

  const nodes: TaskNodeType[] = useMemo(
    () =>
      rawNodes.map((view) => ({
        id: String(view.id),
        type: "task" as const,
        position: positions.get(String(view.id)) ?? { x: 0, y: 0 },
        data: { view },
      })),
    [rawNodes, positions],
  );

  // Image-build tasks gate the rest of the graph, so their outgoing edges are
  // styled distinctly — the purpose `TaskView.is_image_build` was added for.
  const buildTaskIds = useMemo(
    () => new Set(rawNodes.filter((n) => n.is_image_build).map((n) => String(n.id))),
    [rawNodes],
  );

  const edges: Edge[] = useMemo(
    () =>
      rawEdges.map((e) => ({
        id: `${e.source_id}-${e.target_id}`,
        source: String(e.source_id),
        target: String(e.target_id),
        className: buildTaskIds.has(String(e.source_id)) ? "gedge-build" : undefined,
        animated: false,
      })),
    [rawEdges, buildTaskIds],
  );

  if (isLoading) return <p className="sub">loading graph…</p>;
  if (isError) return <p className="err">Could not load the job graph.</p>;
  if (rawNodes.length === 0) return <p className="sub">No tasks yet.</p>;

  return (
    <>
      {data && data.dropped_cycle_edges > 0 && (
        <div className="err">
          {data.dropped_cycle_edges} circular dependency edge(s) hidden to keep the graph renderable.
        </div>
      )}
      {rawNodes.length > CROWDED_NODE_COUNT && (
        <div className="sub">
          {rawNodes.length} tasks — the table view may be easier to scan.
        </div>
      )}
      <div className="graph-canvas" data-testid="job-graph">
        <ReactFlow
          nodes={nodes}
          edges={edges}
          nodeTypes={NODE_TYPES}
          onNodeClick={(_event, node) => onPrompt(`@task ${node.id}`)}
          onlyRenderVisibleElements
          fitView
          proOptions={{ hideAttribution: false }}
        >
          <Background />
          <Controls />
        </ReactFlow>
      </div>
    </>
  );
}
```

!!! warning "`fitView` is a mount-time prop, not a per-poll action"
    Passing `fitView` as a prop fits once when the flow mounts. Do not call `fitView()` from an effect that depends on node data — that re-frames the viewport on every poll and fights the operator's panning.

- [ ] **Step 3: Add the styles**

Append to `src/styles/globals.css`:

```css
/* ---- job graph ---- */
.graph-canvas {
  height: 70vh;
  min-height: 420px;
  border: 1px solid var(--border);
  border-radius: 10px;
  overflow: hidden;
  background: rgba(10, 14, 26, .35);
}
.gnode {
  width: 220px;
  height: 76px;
  padding: 8px 10px;
  border: 1px solid var(--border);
  border-left: 3px solid var(--gray);
  border-radius: 8px;
  background: var(--panel-strong);
  backdrop-filter: var(--glass-blur);
  box-shadow: var(--hi);
  cursor: pointer;
  overflow: hidden;
}
.gnode-head { display: flex; align-items: center; gap: 6px; justify-content: space-between; }
.gnode-name { font-size: 12px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.gnode-meta { display: flex; gap: 8px; margin-top: 6px; font-size: 11px; color: var(--muted); }
.gnode-PENDING   { border-left-color: var(--gray); }
.gnode-CLAIMED   { border-left-color: var(--purple); }
.gnode-RUNNING   { border-left-color: var(--blue); }
.gnode-COMPLETED { border-left-color: var(--green); }
.gnode-FAILED,
.gnode-UPSTREAM_FAILED { border-left-color: var(--red); }
.gnode-CANCELLED { border-left-color: var(--yellow); }
.gnode-build { border-style: dashed; }
.react-flow__edge.gedge-build .react-flow__edge-path {
  stroke-dasharray: 4 3;
  opacity: .65;
}
```

- [ ] **Step 4: Type-check**

Run: `npm run check`
Expected: no errors

- [ ] **Step 5: Commit**

```bash
git add src/components/graph src/styles/globals.css
git commit -m "Add job graph components"
```

---

### Task 6: Routing and the table/graph toggle

**Files:**
- Modify: `src/prompt.ts`
- Modify: `src/App.tsx`
- Modify: `src/views/JobDetail.tsx`

**Interfaces:**
- Consumes: `JobGraph` from Task 5
- Produces: `Route` variant `{ kind: "job"; name: string; view: JobView }` where `export type JobView = "table" | "graph"`

!!! warning "`JobView` already means something else in this codebase"
    `src/api/types.ts` exports `JobView` as the server model. Name the route type `JobViewMode` to avoid a collision.

- [ ] **Step 1: Extend the prompt parser**

In `src/prompt.ts`, replace the `job` route variant and its parse branch:

```ts
export type JobViewMode = "table" | "graph";
```

In the `Route` union, replace `| { kind: "job"; name: string }` with:

```ts
  | { kind: "job"; name: string; view: JobViewMode }
```

Replace the `@job ` branch in `parsePrompt`:

```ts
  if (p.startsWith("@job ")) {
    const rest = p.slice(5).trim();
    if (rest.endsWith(" graph")) {
      return { kind: "job", name: rest.slice(0, -6).trim(), view: "graph" };
    }
    return { kind: "job", name: rest, view: "table" };
  }
```

- [ ] **Step 2: Pass the mode through the router**

In `src/App.tsx`, replace the `case "job":` branch:

```tsx
    case "job":
      return <JobDetail name={route.name} view={route.view} onPrompt={onPrompt} />;
```

- [ ] **Step 3: Add the toggle to JobDetail**

In `src/views/JobDetail.tsx`, add imports:

```tsx
import { JobGraph } from "../components/graph/JobGraph";
import type { JobViewMode } from "../prompt";
```

Change the signature:

```tsx
export function JobDetail({
  name,
  view,
  onPrompt,
}: {
  name: string;
  view: JobViewMode;
  onPrompt: (v: string) => void;
}) {
```

Replace the final `<TasksTable ... />` line with:

```tsx
      <div className="chips">
        <span
          className={`chip${view === "table" ? " chip-active" : ""}`}
          onClick={() => onPrompt(`@job ${job.name}`)}
        >
          Table
        </span>
        <span
          className={`chip${view === "graph" ? " chip-active" : ""}`}
          onClick={() => onPrompt(`@job ${job.name} graph`)}
        >
          Graph
        </span>
      </div>
      {view === "graph" ? (
        <JobGraph refId={job.name} onPrompt={onPrompt} />
      ) : (
        <TasksTable tasks={job.tasks ?? []} onPrompt={onPrompt} />
      )}
```

- [ ] **Step 4: Add the active-chip style**

Append to `src/styles/globals.css`:

```css
.chip-active { border-color: var(--accent); color: var(--accent); }
```

- [ ] **Step 5: Type-check and build**

Run: `npm run check && npm run build`
Expected: no errors; build output written to `aaiclick/server/static/`

- [ ] **Step 6: Commit**

```bash
git add src/prompt.ts src/App.tsx src/views/JobDetail.tsx src/styles/globals.css
git commit -m "Add table/graph toggle to the job detail view"
```

---

### Task 7: End-to-end coverage and documentation references

**Files:**
- Modify: `test_e2e/web/test_smoke.py`
- Modify: `docs/designs/job_graph_view.md`
- Modify: `docs/designs/ui.md`
- Modify: `docs/designs/frontend.md`

**Interfaces:**
- Consumes: everything from Tasks 1–6

- [ ] **Step 1: Write the e2e test**

Append to `test_e2e/web/test_smoke.py`, matching the file's existing fixture usage (`base_url`, `page`):

```python
def test_job_graph_view_renders(page, base_url):
    """The graph toggle reaches a rendered React Flow canvas."""
    page.goto(f"{base_url}/?p=%40jobs")
    page.wait_for_selector("table")

    first_job = page.locator("table tbody tr td:first-child").first
    if first_job.count() == 0:
        pytest.skip("no jobs seeded in this environment")
    job_name = first_job.inner_text().strip()

    page.goto(f"{base_url}/?p=%40job+{job_name}+graph")
    page.wait_for_selector("[data-testid='job-graph']", timeout=10_000)

    assert page.locator("[data-testid='job-graph']").is_visible()
```

- [ ] **Step 2: Run the e2e suite**

Run: `npm run build && uv run pytest test_e2e/web/test_smoke.py -v -p no:cov`
Expected: PASS (or a clean SKIP if Playwright is absent)

- [ ] **Step 3: Add implementation references to the spec**

In `docs/designs/job_graph_view.md`, add under the `# Backend` heading:

```markdown
**Implementation**: `aaiclick/orchestration/graph.py` — see `build_graph_edges`;
`aaiclick/orchestration/view_models.py` — see `build_job_graph_view`;
`aaiclick/internal_api/jobs.py` — see `get_job_graph`;
`aaiclick/server/routers/jobs.py` — see `job_graph`.
```

And under `# Frontend`:

```markdown
**Implementation**: `src/lib/graphLayout.ts` — see `layout` and `structuralKey`;
`src/components/graph/JobGraph.tsx` — see `JobGraph`;
`src/components/graph/TaskNode.tsx` — see `TaskNode`.
```

Reference names, never line numbers — line numbers go stale.

- [ ] **Step 4: Update the UI and frontend specs**

In `docs/designs/ui.md`, under `## Job Detail (@job <name>)`, add after the tasks table description:

```markdown
A Table/Graph toggle switches the body between the tasks table and the
dependency graph. The prompt carries the mode — `@job <name> graph` — so the
view stays shareable as a URL.

**Implementation**: `src/views/JobDetail.tsx` — see `JobDetail`;
`docs/designs/job_graph_view.md` for the graph design.
```

In `docs/designs/frontend.md`, add to the tech-stack table:

```markdown
| Graph        | React Flow 12 + dagre 3      | Layered DAG view of job tasks (MIT)              |
```

And add to the endpoints table:

```markdown
| `useJobGraph`     | `GET /api/v0/jobs/{ref}/graph`   | `aaiclick/server/routers/jobs.py`        |
```

- [ ] **Step 5: Run the shortify pass on edited docs**

Use the `devpowers:shortify` skill on `docs/designs/job_graph_view.md`, `docs/designs/ui.md`, and `docs/designs/frontend.md`.

- [ ] **Step 6: Full verification**

Run: `uv run pytest aaiclick -q && uv run ruff check aaiclick/ && uv run pyright && npm run check && npm run build`
Expected: all green

- [ ] **Step 7: Commit and push**

```bash
git add test_e2e/web/test_smoke.py docs/designs/
git commit -m "Add job graph e2e coverage and documentation references"
git push -u origin claude/ui-graph-job-tasks-k6f6aj
```

- [ ] **Step 8: Verify CI**

Use the `devpowers:check-pr` skill to confirm GitHub Actions workflows pass. Fix any failures before considering the plan complete.

---

## Deferred — do not implement

Tracked in `docs/designs/future.md`:

- **Group containers** — React Flow subflows with rolled-up group status. The endpoint already carries `parent_group_id` and reserves `GRAPH_NODE_GROUP`.
- **Layout engine swap** — if nested-cluster quality disappoints, replace `graphLayout.ts` internals with Graphviz WASM (`@hpcc-js/wasm-graphviz`, Apache-2.0). Not elkjs: dual EPL-2.0 / GPL-3.0-or-later conflicts with the MIT wheel.
- **Vitest** — a JS test runner would let `structuralKey` and `layout` be unit-tested directly.
