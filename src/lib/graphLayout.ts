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
