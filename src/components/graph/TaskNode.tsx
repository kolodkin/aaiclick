import { Handle, Position, type Node, type NodeProps } from "@xyflow/react";
import type { GraphNodeView } from "../../api/types";
import { durationBetween } from "../../lib/format";
import { StatusBadge } from "../StatusBadge";

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
