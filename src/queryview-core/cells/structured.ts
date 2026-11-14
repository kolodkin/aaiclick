// Whether a cell's text is serialized JSON/YAML worth showing parsed in the
// cell popup. See docs/query.md, "Long values".

import yaml from "js-yaml";

export type Format = "json" | "yaml";
export type Structured = { format: Format; data: unknown };

// Only containers are worth a parsed view: a bare scalar renders the same
// either way, so `123` or `"quoted"` is not "structured" here.
export function isContainer(v: unknown): v is unknown[] | Record<string, unknown> {
  return typeof v === "object" && v !== null;
}

// Detect a cell whose *text* is a serialized collection, and parse it.
//
// YAML claims nearly every single-line string (`plain text` is a valid scalar,
// `status: healthy` a valid mapping), so it is tried only on text that already
// spans lines — which keeps prose and Windows paths out. Multi-line prose still
// parses as a folded *scalar*, which the container check rejects.
export function detectStructured(text: string): Structured | null {
  const trimmed = text.trim();
  if (!trimmed) return null;
  try {
    const data: unknown = JSON.parse(trimmed);
    if (isContainer(data)) return { format: "json", data };
  } catch {
    /* not JSON; fall through to YAML */
  }
  if (!trimmed.includes("\n")) return null;
  try {
    const data: unknown = yaml.load(trimmed);
    if (isContainer(data)) return { format: "yaml", data };
  } catch {
    /* not YAML either */
  }
  return null;
}

// Past this, the grid doesn't try to name the format. Parsing a multi-megabyte
// value to pick a button glyph would block the first paint of a page of rows,
// and the popup parses the one cell that is actually opened anyway.
export const MAX_DETECT_CHARS = 16_000;

// The format alone, for the grid's button glyph. Returning just the name keeps
// the parsed graph garbage — a grid holding one per long cell would retain
// several times the result set.
export function detectFormat(text: string): Format | null {
  if (text.length > MAX_DETECT_CHARS) return null;
  return detectStructured(text)?.format ?? null;
}
