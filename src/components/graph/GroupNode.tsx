import type { Node, NodeProps } from "@xyflow/react";
import type { GraphNodeView } from "../../api/types";
import { StatusBadge } from "../StatusBadge";

export type GroupNodeData = { view: GraphNodeView };
export type GroupNodeType = Node<GroupNodeData, "group">;

/**
 * A container drawn around a group's member tasks. Its size comes from the
 * layout, so the component only paints the frame and a compact header; the
 * status is the server's rollup of every task beneath the group.
 */
export function GroupNode({ data }: NodeProps<GroupNodeType>) {
  const { view } = data;
  return (
    <div className={`ggroup ggroup-${view.status}`} data-testid="group-node">
      <div className="ggroup-head">
        <span className="mono ggroup-name" title={view.name}>
          {view.name}
        </span>
        <StatusBadge status={view.status} />
      </div>
    </div>
  );
}
