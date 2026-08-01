Job Graph View
---

Interactive dependency graph for a job's tasks, rendered as an alternate view
inside `@job <name>`. Complements the existing tasks table: the table answers
"what is the status of each task", the graph answers "what is blocking what".

UX conventions live in `docs/designs/ui.md`; framework and build details in
`docs/designs/frontend.md`.

# Scope

v1 renders **tasks only**. Groups exist in the data model and are honoured
semantically (their dependencies are expanded onto member tasks), but they are
not drawn as containers. Group containers are deferred — see
`docs/designs/future.md`.

# Current state

The orchestration model already describes a full DAG, but none of it reaches
the API — `JobDetail` carries `tasks` as a flat list with no edges, and neither
`Group` nor `Dependency` is referenced anywhere in `aaiclick/internal_api/` or
`aaiclick/server/routers/`. A new endpoint is required.

| Concept       | Where it lives — `aaiclick/orchestration/models.py` | Exposed today  |
|---------------|------------------------------------------------------|----------------|
| Task nodes    | `Task`                                               | yes, flat list |
| Group nodes   | `Group`                                              | no             |
| Edges         | `Dependency`                                         | no             |
| Group nesting | `Group.parent_group_id`                              | no             |

`Dependency` is polymorphic: `previous_type` and `next_type` are each `task` or
`group`, so all four endpoint combinations occur.

# Responsibility boundary

The split between server and client is the load-bearing decision in this
design.

| Layer  | Owns                                            | Rationale                          |
|--------|-------------------------------------------------|------------------------------------|
| Server | Graph semantics — what depends on what          | Scheduler business, one source of truth |
| Client | Geometry — node coordinates, viewport, pixels   | Depends on font metrics and CSS    |

The server returns nodes and edges **fully resolved**. The client never sees a
`Group` row or a raw `Dependency` row.

Group expansion is not a formatting concern — it interprets what `G >> B`
means, and that meaning already exists in Python. Deriving it in the browser
would mean shipping the raw `groups` and `dependencies` tables to the client
and reimplementing dependency resolution in TypeScript. Two implementations of
the same semantics drift, and the browser copy would be the one that lies about
what is blocking what — the question this view exists to answer.

!!! warning "Positions are never computed server-side"
    Node coordinates depend on font metrics and CSS box sizing. Returning x/y
    from the API would bake client rendering details into the API contract and
    force a round-trip for anything that reflows. Layout stays in
    `src/lib/graphLayout.ts`.

# Backend

**Implementation**: `aaiclick/orchestration/graph.py` — see `build_graph_edges`;
`aaiclick/orchestration/view_models.py` — see `build_job_graph_view`;
`aaiclick/internal_api/jobs.py` — see `get_job_graph`;
`aaiclick/server/routers/jobs.py` — see `job_graph`.

## Endpoint

```
GET /api/v0/jobs/{ref}/graph → JobGraphView
```

Accepts the same `ref` (id or name) as `GET /api/v0/jobs/{ref}`.

## View models

`GraphNodeView`, `GraphEdgeView`, and `JobGraphView` in
`aaiclick/orchestration/view_models.py`.

Two decisions the field list does not explain:

- `parent_group_id` is populated in v1 even though groups are not drawn — it
  carries the hierarchy the v2 container work needs, so the endpoint does not
  change shape later.
- `GraphNodeKind` is `Literal["task", "group"]` with module-level
  `GRAPH_NODE_TASK` / `GRAPH_NODE_GROUP` constants, per the project's `Literal`
  convention. v1 emits only `"task"`.

## Group expansion

Pure functions live in a new module `aaiclick/orchestration/graph.py`, which
imports no SQLModel — keeping them out of `view_models.py` preserves the
one-directional import boundary restored when the `view_models` ↔
`orchestration` cycle was untangled.

A dependency touching a group is rewritten onto member tasks:

| Dependency | Meaning                        | Expands to                          |
|------------|--------------------------------|-------------------------------------|
| `G >> B`   | B waits for **all** of G       | G's sink tasks → B                  |
| `A >> G`   | **All** of G waits for A       | A → G's source tasks                |
| `G >> H`   | H waits for all of G           | G's sinks → H's sources             |

- **Source task** — a member with no predecessor inside the group.
- **Sink task** — a member with no successor inside the group.

Nested groups recurse through `parent_group_id`; an empty group contributes no
edges and is skipped.

Expanding to *every* member instead of sources/sinks would be quadratic in edge
count and would misrepresent ordering — a task deep inside a group would appear
to depend directly on an upstream node it never waits on individually.

## Internal API

`aaiclick/internal_api/jobs.py` gains `get_job_graph(ref)`, following the
existing `_resolve_job` / `_load_job_and_tasks` pattern: load the job's tasks,
groups, and dependencies, then delegate to `graph.py` for flattening and
`view_models.py` for the adapter. The router in
`aaiclick/server/routers/jobs.py` stays a thin pass-through, as the other job
routes do.

# Frontend

**Implementation**: `src/lib/graphLayout.ts` — see `layout` and `structuralKey`;
`src/components/graph/JobGraph.tsx` — see `JobGraph`;
`src/components/graph/TaskNode.tsx` — see `TaskNode`;
`src/views/JobDetail.tsx` — see `JobDetail` for the table/graph toggle.

## Technology

React Flow renders; dagre computes layout. Both MIT, matching the project
license.

| Package           | Version | License | Role                          |
|-------------------|---------|---------|-------------------------------|
| `@xyflow/react`   | 12.x    | MIT     | Renderer, viewport, subflows  |
| `@dagrejs/dagre`  | 3.x     | MIT     | Layered DAG layout            |

React Flow renders nodes as real React components, so `StatusBadge` and the
status variables in `src/styles/globals.css` are reused directly rather than
reimplemented in a canvas stylesheet. Its subflow support (`parentId` +
`extent`) is what v2 group containers will use. It ships no layout algorithm by
design, so a layout engine is required regardless of renderer.

??? info "Why not Cytoscape.js, d3, elkjs, or hand-rolled SVG"
    **Cytoscape.js** scales better (canvas, thousands of nodes) and has
    first-class compound nodes, but it is imperative and draws to canvas — node
    styling moves into its own selector language, discarding the existing
    components and theme.

    **d3** has no DAG layout. `d3-hierarchy` handles trees, where every node has
    exactly one parent, but the model supports fan-in (`[A, B] >> C`, see
    `_DependencyOps.__rrshift__`), so tasks have several. `d3-force` is
    non-deterministic and would drift on every poll. d3 is a rendering toolkit
    here, not a layout answer — which is why `d3-dag` exists separately.

    **elkjs** has the best nested-hierarchy layout and the largest install base,
    but is dual-licensed EPL-2.0 OR GPL-3.0-or-later, ships ~8 MB unpacked, and
    is async enough to want a Web Worker. Licensing is the blocker for an MIT
    wheel.

    **d3-dag** is MIT and smallest, with optimal crossing minimisation, but has
    no compound/cluster support — disqualifying once group containers land.

    **Hand-rolled SVG + dagre** has the best long-term maintenance profile (no
    framework-coupled dependency), but costs pan/zoom, edge routing, arrowheads,
    viewport fitting, and selection — work React Flow already does.

    The v2 escape hatch, if dagre's nested-cluster quality disappoints, is
    Graphviz WASM (`@hpcc-js/wasm-graphviz`, Apache-2.0) — real
    `subgraph cluster_*` support without elkjs's licensing.

## Layout isolation

`src/lib/graphLayout.ts` is the **only** module that imports dagre:

```ts
export function layout(
  nodes: GraphNode[],
  edges: GraphEdge[],
): Map<string, { x: number; y: number }>;
```

The dagre surface used is small — `setGraph({ rankdir })`, `setNode`,
`setEdge`, `layout`, and reading `x`/`y`. Confining it to one file makes the
engine swappable without touching components, which matters more than the engine
choice itself: replacing the layout engine is a one-file change, replacing the
renderer is a rewrite.

## Layout stability under polling

Re-running layout on every 2 s poll would reposition nodes while an operator is
reading them. Layout is therefore memoised on a **structural key** — sorted node
ids plus sorted edge pairs:

- A status change does not alter the key, so no re-layout runs; nodes re-colour
  in place.
- Layout runs only when structure changes, which happens when dynamic tasks
  register mid-run (see `aaiclick/orchestration/examples/orchestration_dynamic.py`).
- The viewport is preserved across re-layouts. `fitView` runs on first load and
  from an explicit control, never on poll.

## Files

| File                                  | Role                                              |
|---------------------------------------|---------------------------------------------------|
| `src/lib/graphLayout.ts`              | dagre wrapper + structural memo key                |
| `src/components/graph/JobGraph.tsx`   | ReactFlow wrapper: data → layout → render          |
| `src/components/graph/TaskNode.tsx`   | Custom node; reuses `StatusBadge`                  |
| `src/api/hooks.ts`                    | `useJobGraph(ref)`, 2 s `refetchInterval`          |
| `src/prompt.ts`                       | `view: "table" \| "graph"` on the `job` route      |
| `src/views/JobDetail.tsx`             | Table/Graph toggle                                 |

## Node rendering

Each node shows name (monospace), status badge, short entrypoint, duration,
and attempt when greater than 1. Border colour derives from the status
variable, matching the table's badges. Clicking a node sets the prompt to
`@task <id>`, reusing the existing `onPrompt` navigation.

Nodes with `is_image_build` are styled distinctly, along with their outgoing
edges — the purpose `TaskView.is_image_build` was added for.

## Routing

The URL remains the only application state. `parsePrompt` gains an optional
suffix on the job route:

```
@job <name>          → { kind: "job", name, view: "table" }
@job <name> graph    → { kind: "job", name, view: "graph" }
```

The table stays the default, so existing links and the e2e smoke test are
unaffected, and the graph view stays shareable as a URL.

# Error handling

| Condition              | Behaviour                                                  |
|------------------------|------------------------------------------------------------|
| Job not found          | Existing `JobDetail` not-found branch                      |
| Job with no tasks      | Placeholder message, not an empty canvas                   |
| Cycle in dependencies  | Back-edges dropped, count surfaced, warning shown          |
| Graph over ~300 nodes  | `onlyRenderVisibleElements`; banner suggesting table view   |

!!! warning "Cycles must be dropped before layout"
    Dependencies should form a DAG, but a corrupt row must not hang the UI —
    dagre does not terminate cleanly on cycles. Detection happens server-side
    in `graph.py`, which reports `dropped_cycle_edges` so the condition is
    visible rather than silent.

# Testing

| Layer                | Coverage                                                        |
|----------------------|-----------------------------------------------------------------|
| Python unit          | `graph.py` flattening — nested groups, fan-in/fan-out, group→group, empty groups, cycles |
| Python API           | `GET /jobs/{ref}/graph` — shape, ref resolution, missing job     |
| TypeScript           | `npm run check` (existing CI gate)                               |
| End-to-end           | `test_e2e/web/test_smoke.py` — load `@job <name> graph`, assert nodes render |

The flattening functions are pure and take plain ids, so they test
exhaustively without fixtures or a database.

!!! warning "Regenerate API types after the view models land"
    `npm run gen-types` must be re-run once `JobGraphView` exists, and
    `src/api/types.ts` needs one re-export line. CI fails on drift.

# Deferred

Group containers, rendered as React Flow subflows with a rolled-up group
status, are tracked in `docs/designs/future.md`. The endpoint shape already
accommodates them: `GraphNodeView.kind` gains `"group"` and
`parent_group_id` is already populated.
