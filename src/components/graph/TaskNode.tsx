import { Handle, Position, type Node, type NodeProps } from "@xyflow/react";
import type { GraphNodeView } from "../../api/types";
import { durationBetween } from "../../lib/format";
import { StatusBadge } from "../StatusBadge";

export type BuildGate = { id: string; name: string; status: string };
export type TaskNodeData = {
  view: GraphNodeView;
  buildGate?: BuildGate;
  onPrompt: (v: string) => void;
};
export type TaskNodeType = Node<TaskNodeData, "task">;

function shortEntrypoint(entrypoint: string): string {
  if (entrypoint.includes(":")) return entrypoint.split(":").pop() ?? entrypoint;
  if (entrypoint.includes(".")) return entrypoint.split(".").pop() ?? entrypoint;
  return entrypoint;
}

export function TaskNode({ data }: NodeProps<TaskNodeType>) {
  const { view, buildGate, onPrompt } = data;
  return (
    <div className={`gnode gnode-${view.status}${view.is_image_build ? " gnode-build" : ""}`}>
      {/* A build gates every task sharing its image, so drawing those N-1
          dependencies as edges buries the real pipeline. The badge states the
          same fact once per node and opens the build task on click. */}
      {buildGate && (
        <button
          type="button"
          className={`gnode-buildgate gnode-buildgate-${buildGate.status}`}
          data-testid="build-gate"
          title={`Image build: ${buildGate.name} (${buildGate.status}) — click to open`}
          onClick={(e) => {
            e.stopPropagation();
            onPrompt(`@task ${buildGate.id}`);
          }}
        >
          B
        </button>
      )}
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
