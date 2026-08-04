import { useMemo, useState } from "react";
import { Background, Controls, MarkerType, ReactFlow, type Edge } from "@xyflow/react";
import "@xyflow/react/dist/style.css";
import { useJobGraph } from "../../api/hooks";
import { Chips } from "../Chips";
import { edgeKey, layout, structuralKey } from "../../lib/graphLayout";
import { RoutedEdge } from "./RoutedEdge";
import { TaskNode, type BuildGate, type TaskNodeType } from "./TaskNode";

const NODE_TYPES = { task: TaskNode };
const EDGE_TYPES = { routed: RoutedEdge };
const CROWDED_NODE_COUNT = 300;

export function JobGraph({ refId, onPrompt }: { refId: string; onPrompt: (v: string) => void }) {
  const { data, isLoading, isError } = useJobGraph(refId);
  const [showBuildEdges, setShowBuildEdges] = useState(false);

  const rawNodes = useMemo(() => data?.nodes ?? [], [data]);
  const allEdges = useMemo(() => data?.edges ?? [], [data]);

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

  const layoutNodes = useMemo(() => rawNodes.map((n) => ({ id: String(n.id) })), [rawNodes]);
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
  const { positions, edgePoints } = useMemo(() => layout(layoutNodes, layoutEdges), [key]);

  const nodes: TaskNodeType[] = useMemo(
    () =>
      rawNodes.map((view) => ({
        id: String(view.id),
        type: "task" as const,
        position: positions.get(String(view.id)) ?? { x: 0, y: 0 },
        data: { view, buildGate: buildGates.get(String(view.id)), onPrompt },
      })),
    [rawNodes, positions, buildGates, onPrompt],
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
  if (rawNodes.length === 0) return <p className="sub">No tasks yet.</p>;

  return (
    <>
      {data && data.dropped_cycle_edges > 0 && (
        <div className="err">
          {data.dropped_cycle_edges} circular dependency edge(s) hidden to keep the graph renderable.
        </div>
      )}
      {rawNodes.length > CROWDED_NODE_COUNT && (
        <div className="sub">{rawNodes.length} tasks — the table view may be easier to scan.</div>
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
          onNodeClick={(_event, node) => onPrompt(`@task ${node.id}`)}
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
