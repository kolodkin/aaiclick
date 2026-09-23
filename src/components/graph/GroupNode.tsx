import { Handle, Position, type Node, type NodeProps } from "@xyflow/react";
import type { GraphNodeView } from "../../api/types";
import { StatusBadge } from "../StatusBadge";

export type GroupNodeData = { view: GraphNodeView };
export type GroupNodeType = Node<GroupNodeData, "group">;

/**
 * A frame around a group's member tasks, sized by the layout. The status is
 * the server's rollup of every task beneath the group. Its handles take the
 * single edge a group dependency draws, into or out of the whole frame.
 */
export function GroupNode({ data }: NodeProps<GroupNodeType>) {
  const { view } = data;
  return (
    <div className={`ggroup ggroup-${view.status}`} data-testid="group-node">
      <Handle type="target" position={Position.Left} />
      <div className="ggroup-head">
        <span className="mono ggroup-name" title={view.name}>
          {view.name}
        </span>
        <StatusBadge status={view.status} />
      </div>
      <Handle type="source" position={Position.Right} />
    </div>
  );
}
