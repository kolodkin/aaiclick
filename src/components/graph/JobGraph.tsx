import { useMemo } from "react";
import { Background, Controls, MarkerType, ReactFlow, type Edge } from "@xyflow/react";
import "@xyflow/react/dist/style.css";
import { useJobGraph } from "../../api/hooks";
import { layout, structuralKey } from "../../lib/graphLayout";
import { TaskNode, type BuildGate, type TaskNodeType } from "./TaskNode";

const NODE_TYPES = { task: TaskNode };
const CROWDED_NODE_COUNT = 300;

export function JobGraph({ refId, onPrompt }: { refId: string; onPrompt: (v: string) => void }) {
  const { data, isLoading, isError } = useJobGraph(refId);

  const rawNodes = useMemo(() => data?.nodes ?? [], [data]);
  const allEdges = useMemo(() => data?.edges ?? [], [data]);

  // A build gates every task sharing its image, so its out-degree is N-1 and
  // drawing those edges buries the real pipeline under a fan of lines that all
  // say the same thing. They are collapsed into a per-node badge instead —
  // stated once where it applies, in every build state, with no layout churn
  // when the build finishes.
  const buildNodes = useMemo(() => new Map(rawNodes.filter((n) => n.is_image_build).map((n) => [String(n.id), n])), [
    rawNodes,
  ]);

  const buildGates = useMemo(() => {
    const gates = new Map<string, BuildGate>();
    for (const edge of allEdges) {
      const build = buildNodes.get(String(edge.source_id));
      if (build) {
        gates.set(String(edge.target_id), { id: String(build.id), name: build.name, status: build.status });
      }
    }
    return gates;
  }, [allEdges, buildNodes]);

  const rawEdges = useMemo(
    () => allEdges.filter((e) => !buildNodes.has(String(e.source_id))),
    [allEdges, buildNodes],
  );

  const layoutNodes = useMemo(() => rawNodes.map((n) => ({ id: String(n.id) })), [rawNodes]);
  const layoutEdges = useMemo(
    () => rawEdges.map((e) => ({ source: String(e.source_id), target: String(e.target_id) })),
    [rawEdges],
  );

  const key = useMemo(() => structuralKey(layoutNodes, layoutEdges), [layoutNodes, layoutEdges]);

  // Deliberately keyed on structure alone. Including the node data here would
  // re-run dagre on every 2 s poll and shuffle nodes while an operator reads
  // them. Status changes must re-colour in place, never reposition.
  // eslint-disable-next-line react-hooks/exhaustive-deps
  const positions = useMemo(() => layout(layoutNodes, layoutEdges), [key]);

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

  const edges: Edge[] = useMemo(
    () =>
      rawEdges.map((e) => ({
        id: `${e.source_id}-${e.target_id}`,
        source: String(e.source_id),
        target: String(e.target_id),
        // Direction is the whole point of a dependency graph, and React Flow
        // draws no arrowhead by default.
        markerEnd: { type: MarkerType.ArrowClosed, width: 18, height: 18 },
        animated: false,
      })),
    [rawEdges],
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
        <div className="sub">{rawNodes.length} tasks — the table view may be easier to scan.</div>
      )}
      <div className="graph-canvas" data-testid="job-graph">
        <ReactFlow
          nodes={nodes}
          edges={edges}
          nodeTypes={NODE_TYPES}
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
