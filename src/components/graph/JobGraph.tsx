import { useMemo, useState } from "react";
import { Background, Controls, MarkerType, ReactFlow, type Edge } from "@xyflow/react";
import "@xyflow/react/dist/style.css";
import { useJobGraph } from "../../api/hooks";
import type { GraphNodeView } from "../../api/types";
import { Chips } from "../Chips";
import { edgeKey, layout, structuralKey } from "../../lib/graphLayout";
import { GroupNode, type GroupNodeType } from "./GroupNode";
import { RoutedEdge } from "./RoutedEdge";
import { TaskNode, type BuildGate, type TaskNodeType } from "./TaskNode";

const NODE_TYPES = { task: TaskNode, group: GroupNode };
const EDGE_TYPES = { routed: RoutedEdge };
const CROWDED_NODE_COUNT = 300;

type GraphNode = TaskNodeType | GroupNodeType;

/**
 * Orders nodes so every container precedes its members (React Flow resolves
 * `parentId` against nodes already seen) and returns each node's parent. A
 * `parent_group_id` naming a group the server did not emit — empty, or its
 * row is gone — counts as no parent.
 */
function nestByGroup(views: GraphNodeView[]): { ordered: GraphNodeView[]; parentOf: Map<string, string> } {
  const groups = new Map(views.filter((v) => v.kind === "group").map((v) => [String(v.id), v]));
  const parentOf = new Map<string, string>();
  for (const view of views) {
    const parent = view.parent_group_id == null ? undefined : String(view.parent_group_id);
    if (parent !== undefined && groups.has(parent)) parentOf.set(String(view.id), parent);
  }
  const depth = (id: string): number => {
    let d = 0;
    for (let p = parentOf.get(id); p !== undefined; p = parentOf.get(p)) d++;
    return d;
  };
  const ordered = [...views].sort((a, b) => {
    if (a.kind !== b.kind) return a.kind === "group" ? -1 : 1;
    return a.kind === "group" ? depth(String(a.id)) - depth(String(b.id)) : 0;
  });
  return { ordered, parentOf };
}

export function JobGraph({ refId, onPrompt }: { refId: string; onPrompt: (v: string) => void }) {
  const { data, isLoading, isError } = useJobGraph(refId);
  const [showBuildEdges, setShowBuildEdges] = useState(false);

  const rawNodes = useMemo(() => data?.nodes ?? [], [data]);
  const allEdges = useMemo(() => data?.edges ?? [], [data]);
  const taskCount = useMemo(() => rawNodes.filter((n) => n.kind === "task").length, [rawNodes]);

  // The server classifies edges (`kind`, `attaches_build`) — which edges a
  // build gates, and which one keeps it attached to the pipeline, is graph
  // semantics, not geometry. These three partitions therefore depend only on
  // `allEdges`, which react-query keeps referentially stable across a
  // status-only poll, so the edge layer stops re-rendering every 2 s.
  const pipelineEdges = useMemo(() => allEdges.filter((e) => e.kind !== "build"), [allEdges]);
  const rootBuildEdges = useMemo(
    () => allEdges.filter((e) => e.kind === "build" && e.attaches_build),
    [allEdges],
  );
  const extraBuildEdges = useMemo(
    () => allEdges.filter((e) => e.kind === "build" && !e.attaches_build),
    [allEdges],
  );

  // Carries the build's own status, so this one legitimately tracks node data.
  const buildGates = useMemo(() => {
    const builds = new Map(rawNodes.filter((n) => n.is_image_build).map((n) => [String(n.id), n]));
    const gates = new Map<string, BuildGate>();
    for (const edge of allEdges) {
      const build = builds.get(String(edge.source_id));
      if (build) {
        gates.set(String(edge.target_id), { id: String(build.id), name: build.name, status: build.status });
      }
    }
    return gates;
  }, [allEdges, rawNodes]);

  const { ordered, parentOf } = useMemo(() => nestByGroup(rawNodes), [rawNodes]);
  const layoutNodes = useMemo(
    () => ordered.map((n) => ({ id: String(n.id), parent: parentOf.get(String(n.id)) })),
    [ordered, parentOf],
  );
  // Layout always sees *every* edge, including the collapsed build ones. dagre
  // then reserves space and computes waypoints for them, so revealing them
  // routes around nodes rather than through — and because the input never
  // changes, toggling moves nothing.
  const layoutEdges = useMemo(
    () => allEdges.map((e) => ({ source: String(e.source_id), target: String(e.target_id) })),
    [allEdges],
  );

  const key = useMemo(() => structuralKey(layoutNodes, layoutEdges), [layoutNodes, layoutEdges]);

  // Deliberately keyed on structure alone. Including the node data here would
  // re-run dagre on every 2 s poll and shuffle nodes while an operator reads
  // them. Status changes must re-colour in place, never reposition.
  // eslint-disable-next-line react-hooks/exhaustive-deps
  const { positions, sizes, edgePoints } = useMemo(() => layout(layoutNodes, layoutEdges), [key]);

  const nodes: GraphNode[] = useMemo(
    () =>
      ordered.map((view) => {
        const id = String(view.id);
        const parentId = parentOf.get(id);
        const absolute = positions.get(id) ?? { x: 0, y: 0 };
        // The layout is absolute; React Flow places a child relative to its
        // parent's top-left corner.
        const origin = (parentId && positions.get(parentId)) || { x: 0, y: 0 };
        const position = { x: absolute.x - origin.x, y: absolute.y - origin.y };
        const nesting = parentId ? { parentId, extent: "parent" as const } : {};
        if (view.kind === "group") {
          const size = sizes.get(id);
          return {
            id,
            type: "group" as const,
            position,
            ...nesting,
            style: size && { width: size.width, height: size.height },
            selectable: false,
            data: { view },
          };
        }
        return {
          id,
          type: "task" as const,
          position,
          ...nesting,
          data: { view, buildGate: buildGates.get(id), onPrompt },
        };
      }),
    [ordered, parentOf, positions, sizes, buildGates, onPrompt],
  );

  const edges: Edge[] = useMemo(() => {
    // Only the toggled-on extras are dashed. The root edge is always drawn and
    // is part of the graph's backbone, so it reads as a normal edge.
    const groups: [typeof pipelineEdges, boolean][] = [
      [pipelineEdges, false],
      [rootBuildEdges, false],
      [showBuildEdges ? extraBuildEdges : [], true],
    ];
    return groups.flatMap(([group, dashed]) =>
      group.map((e) => {
        const key = edgeKey(String(e.source_id), String(e.target_id));
        return {
          id: key,
          source: String(e.source_id),
          target: String(e.target_id),
          type: "routed" as const,
          data: { points: edgePoints.get(key) },
          className: dashed ? "gedge-build" : undefined,
          // Direction is the whole point of a dependency graph, and React Flow
          // draws no arrowhead by default.
          markerEnd: { type: MarkerType.ArrowClosed, width: 18, height: 18 },
        };
      }),
    );
  }, [pipelineEdges, rootBuildEdges, extraBuildEdges, showBuildEdges, edgePoints]);

  if (isLoading) return <p className="sub">loading graph…</p>;
  if (isError) return <p className="err">Could not load the job graph.</p>;
  if (taskCount === 0) return <p className="sub">No tasks yet.</p>;

  return (
    <>
      {data && data.dropped_cycle_edges > 0 && (
        <div className="err">
          {data.dropped_cycle_edges} circular dependency edge(s) hidden to keep the graph renderable.
        </div>
      )}
      {taskCount > CROWDED_NODE_COUNT && (
        <div className="sub">{taskCount} tasks — the table view may be easier to scan.</div>
      )}
      {extraBuildEdges.length > 0 && (
        <Chips
          chips={[
            {
              label: `${showBuildEdges ? "Hide" : "Show"} build dependencies (${extraBuildEdges.length})`,
              onClick: () => setShowBuildEdges((v) => !v),
              testId: "build-edges-toggle",
              active: showBuildEdges,
            },
          ]}
        />
      )}
      <div className="graph-canvas" data-testid="job-graph">
        <ReactFlow
          nodes={nodes}
          edges={edges}
          nodeTypes={NODE_TYPES}
          edgeTypes={EDGE_TYPES}
          onNodeClick={(_event, node) => {
            if (node.type === "task") onPrompt(`@task ${node.id}`);
          }}
          onlyRenderVisibleElements
          fitView
          // Cap zoom at 1:1 — fitView's default maxZoom of 2 blows a
          // single-node graph up to twice its designed size.
          fitViewOptions={{ maxZoom: 1, padding: 0.2 }}
        >
          <Background />
          <Controls />
        </ReactFlow>
      </div>
    </>
  );
}
