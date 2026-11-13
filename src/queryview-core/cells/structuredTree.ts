// Flatten parsed cell data (see structured.ts) into the row list the cell popup
// renders: one line per value, indented by depth, with containers collapsible.
// No React here — CellDataModal.tsx draws these rows.

import { isContainer } from "./structured";

export const ROOT_PATH = "$";

// Key names can contain anything, so paths join with a character that cannot
// appear in one; they are internal ids for the collapsed set, never displayed.
const SEP = "\u0000";

export type TreeRow = {
  path: string;
  depth: number;
  // The object key or array index this value sits under; null for the root.
  key: string | null;
  summary: string;
  expandable: boolean;
};

// A container shows its size (`{3}` / `[2]`), a scalar its text. `null` prints
// as `null`: in a parsed document the absence is the information, unlike a
// table cell where empty reads better.
function summarize(v: unknown): string {
  if (Array.isArray(v)) return `[${v.length}]`;
  if (isContainer(v)) return `{${Object.keys(v).length}}`;
  if (v === null) return "null";
  return String(v);
}

function entries(v: unknown[] | Record<string, unknown>): [string, unknown][] {
  return Array.isArray(v) ? v.map((x, i) => [String(i), x]) : Object.entries(v);
}

// Depth-first rows, skipping the children of any container in `collapsed`.
export function treeRows(data: unknown, collapsed: ReadonlySet<string>): TreeRow[] {
  const out: TreeRow[] = [];
  const walk = (value: unknown, path: string, depth: number, key: string | null) => {
    const expandable = isContainer(value);
    out.push({ path, depth, key, summary: summarize(value), expandable });
    if (!expandable || collapsed.has(path)) return;
    for (const [k, child] of entries(value)) walk(child, path + SEP + k, depth + 1, k);
  };
  walk(data, ROOT_PATH, 0, null);
  return out;
}

// Containers deeper than this start folded.
const OPEN_DEPTH = 1;

// The popup's initial collapsed set: the root and its immediate children stay
// open so the document's shape is visible, everything below folds.
export function defaultCollapsed(data: unknown): Set<string> {
  return new Set(
    treeRows(data, new Set())
      .filter((r) => r.expandable && r.depth > OPEN_DEPTH)
      .map((r) => r.path),
  );
}
