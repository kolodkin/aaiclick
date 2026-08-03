import { BaseEdge, getBezierPath, type EdgeProps } from "@xyflow/react";
import type { Point } from "../../lib/graphLayout";

export type RoutedEdgeData = { points?: Point[] };

/**
 * Draws an edge along dagre's waypoints instead of a handle-to-handle bezier.
 *
 * dagre reserves space for edges that span several ranks, so its points route
 * clear of the nodes in between; the default bezier ignores them and cuts
 * straight through. Falls back to a bezier when no waypoints are available.
 */
export function RoutedEdge({
  id,
  data,
  markerEnd,
  style,
  sourceX,
  sourceY,
  targetX,
  targetY,
  sourcePosition,
  targetPosition,
}: EdgeProps) {
  const points = (data as RoutedEdgeData | undefined)?.points;

  if (!points || points.length < 2) {
    const [fallback] = getBezierPath({
      sourceX,
      sourceY,
      targetX,
      targetY,
      sourcePosition,
      targetPosition,
    });
    return <BaseEdge id={id} path={fallback} markerEnd={markerEnd} style={style} />;
  }

  // Anchor to the real handles — dagre's ends sit on the node box, which is a
  // few pixels off from where React Flow renders the handle.
  const via = points.slice(1, -1);
  return <BaseEdge id={id} path={smoothPath(sourceX, sourceY, via, targetX, targetY)} markerEnd={markerEnd} style={style} />;
}

/** Quadratic segments through the midpoints, so corners round off. */
function smoothPath(startX: number, startY: number, via: Point[], endX: number, endY: number): string {
  if (via.length === 0) return `M ${startX},${startY} L ${endX},${endY}`;

  let d = `M ${startX},${startY}`;
  for (let i = 0; i < via.length; i++) {
    const current = via[i];
    const next = via[i + 1];
    const midX = next ? (current.x + next.x) / 2 : endX;
    const midY = next ? (current.y + next.y) / 2 : endY;
    d += ` Q ${current.x},${current.y} ${midX},${midY}`;
  }
  return d;
}
