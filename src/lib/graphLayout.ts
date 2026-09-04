// The only module that imports dagre. Everything else works through
// `layout()` and `structuralKey()` — see docs/designs/frontend.md for the
// engine rationale.
import dagre from "@dagrejs/dagre";

export interface LayoutNode {
  id: string;
  /** Container this node sits inside. Containers are `LayoutNode`s too. */
  parent?: string;
}

export interface LayoutEdge {
  source: string;
  target: string;
}

export const NODE_SIZE = { width: 220, height: 76 };
// Extra vertical room under a container's top edge, so its header sits clear
// of the first member.
export const CONTAINER_HEADER_GAP = 24;

// Identifies the graph's *shape*. Status changes leave this untouched, so a
// 2 s poll re-colours nodes in place instead of repositioning them mid-read.
// Only structural change — dynamic tasks registering at runtime — re-lays out.
export function structuralKey(nodes: LayoutNode[], edges: LayoutEdge[]): string {
  const n = nodes
    .map((x) => `${x.id}@${x.parent ?? ""}`)
    .sort()
    .join(",");
  const e = edges
    .map((x) => edgeKey(x.source, x.target))
    .sort()
    .join(",");
  return `${n}|${e}`;
}

export interface Point {
  x: number;
  y: number;
}

export interface Size {
  width: number;
  height: number;
}

export interface Layout {
  /** Top-left corner of every node in absolute canvas coordinates. */
  positions: Map<string, Point>;
  /** Computed extent of each container; leaves are always `NODE_SIZE`. */
  sizes: Map<string, Size>;
  /**
   * Waypoints per `source>target`, strictly *between* the endpoints, in render
   * order, to be connected as a polyline.
   */
  edgePoints: Map<string, Point[]>;
}

export function edgeKey(source: string, target: string): string {
  return `${source}>${target}`;
}

export function layout(nodes: LayoutNode[], edges: LayoutEdge[]): Layout {
  const g = new dagre.graphlib.Graph({ compound: true });
  g.setGraph({ rankdir: "LR", nodesep: 24, ranksep: 72 });
  g.setDefaultEdgeLabel(() => ({}));

  // A container is whatever something claims as its parent. dagre sizes a
  // cluster from its members — declared dimensions would not reserve space —
  // and a container nothing points at is laid out as a leaf.
  const containers = new Set(nodes.flatMap((n) => (n.parent ? [n.parent] : [])));
  for (const node of nodes) {
    g.setNode(node.id, containers.has(node.id) ? {} : { width: NODE_SIZE.width, height: NODE_SIZE.height });
  }
  for (const node of nodes) {
    if (node.parent && containers.has(node.parent)) g.setParent(node.id, node.parent);
  }
  for (const edge of edges) {
    g.setEdge(edge.source, edge.target);
  }

  dagre.layout(g);

  // dagre pads a cluster symmetrically and offers no per-side padding, so
  // header room is carved afterwards: a horizontal strip of
  // CONTAINER_HEADER_GAP is inserted just below every container's top edge and
  // everything beneath shifts down. The shift is monotone in y, so nothing
  // that was clear of its neighbours can come to overlap them.
  const top = (id: string) => g.node(id).y - g.node(id).height / 2;
  const strips = [...new Set([...containers].map(top))].sort((a, b) => a - b);
  const shifted = (y: number) => y + CONTAINER_HEADER_GAP * strips.filter((s) => s < y).length;

  const positions = new Map<string, Point>();
  const sizes = new Map<string, Size>();
  for (const node of nodes) {
    const placed = g.node(node.id);
    // dagre returns node centres; React Flow positions by top-left corner.
    const y = shifted(placed.y - placed.height / 2);
    positions.set(node.id, { x: placed.x - placed.width / 2, y });
    if (containers.has(node.id)) {
      sizes.set(node.id, { width: placed.width, height: shifted(placed.y + placed.height / 2) - y });
    }
  }

  // dagre inserts virtual nodes for edges that span more than one rank and
  // reserves space for them, so its waypoints already avoid the node boxes.
  // React Flow's built-in edges draw a naive handle-to-handle bezier instead,
  // which cuts straight through whatever sits between — so keep the points.
  const edgePoints = new Map<string, Point[]>();
  for (const edge of edges) {
    // dagre brackets its polyline with both endpoints, which sit on the node
    // box a few px from where React Flow renders the handle. Trim them here so
    // consumers never encode that detail.
    const routed = g.edge(edge.source, edge.target);
    if (routed?.points?.length) {
      edgePoints.set(
        edgeKey(edge.source, edge.target),
        routed.points.slice(1, -1).map((p: Point) => ({ x: p.x, y: shifted(p.y) })),
      );
    }
  }

  return { positions, sizes, edgePoints };
}
