// See docs/designs/ui.md — "Prompt dropdown".
// Maps a prompt draft to the values its command takes: which endpoint lists
// them, how the typed text narrows them, and what a picked row inserts.
import { fetchJSON } from "../api/client";
import { JOB_VIEW_SUFFIX } from "../prompt";
import { shortId } from "../lib/viewer";
import type {
  DashboardSummary,
  JobStatus,
  JobView,
  ObjectView,
  Page,
  RegisteredJobView,
  SandboxFileView,
  TaskStatus,
  TaskView,
} from "../api/types";

export const PAGE = 50;
// Objects and dashboards are listed whole; the server's max page fetches them in one go.
const WHOLE = 1000;

export interface SourceItem {
  id: string;
  label: string;
  /** Secondary text, e.g. a task's job id or "disabled". */
  detail?: string;
  status?: JobStatus | TaskStatus;
  at?: string;
  /** Prompt that a pick commits. */
  insert: string;
}

export interface SourcePage {
  items: SourceItem[];
  more: boolean;
}

export interface SourceMatch {
  /** Query-key root; the narrowing text is added by the caller. */
  key: string;
  text: string;
  /** Narrows rows in the browser, for sources the server cannot filter (kept out of the cache key). */
  local?: string;
  fetchPage(offset: number): Promise<SourcePage>;
}

function query(params: Record<string, string | number | undefined>): string {
  const q = new URLSearchParams();
  for (const [k, v] of Object.entries(params)) if (v !== undefined && v !== "") q.set(k, String(v));
  return `?${q}`;
}

// Offset-paged list endpoint; `total` says whether more remain (a full page is the fallback guess).
function paged<T>(path: string, params: Record<string, string | undefined>, toItem: (t: T) => SourceItem) {
  return async (offset: number): Promise<SourcePage> => {
    const page = await fetchJSON<Page<T>>(`${path}${query({ limit: PAGE, offset, ...params })}`);
    const more = page.total != null ? offset + page.items.length < page.total : page.items.length === PAGE;
    return { items: page.items.map(toItem), more };
  };
}

const like = (text: string) => (text ? `%${text}%` : undefined);

// `job` (an id or a name) narrows to that job's tasks; it is in the key so the two lists never share a cache.
function tasks(text: string, job?: string): SourceMatch {
  return {
    key: job ? `tasks:${job}` : "tasks",
    text,
    fetchPage: paged<TaskView>("/tasks", { name: like(text), job }, (t) => ({
      id: t.id,
      label: `${t.name} (${shortId(t.id)})`,
      detail: `job ${shortId(t.job_id)}`,
      status: t.status,
      at: t.created_at,
      insert: `@task ${t.id}`,
    })),
  };
}

// `cmd` is in the key: cached rows carry a finished `insert`, so commands sharing an endpoint must not share a cache.
function jobs(cmd: string, text: string): SourceMatch {
  return {
    key: `jobs:${cmd}`,
    text,
    fetchPage: paged<JobView>("/jobs", { name: like(text) }, (j) => ({
      id: j.id,
      label: `${j.name} (${shortId(j.id)})`,
      status: j.status,
      at: j.created_at,
      insert: `${cmd} ${j.id}`,
    })),
  };
}

function registered(cmd: string, text: string): SourceMatch {
  return {
    key: `registered:${cmd}`,
    text,
    fetchPage: paged<RegisteredJobView>("/registered-jobs", { name: like(text) }, (r) => ({
      id: r.id,
      label: r.name,
      detail: r.enabled ? "enabled" : "disabled",
      insert: `${cmd} ${r.name}`,
    })),
  };
}

// `scope` is the prompt up to the object (`@data`, `@data persistent`, `@job etl data`).
function objects(scope: string, job: string | null, text: string): SourceMatch {
  return {
    key: `objects:${scope}`,
    text,
    fetchPage: async () => {
      const page = await fetchJSON<Page<ObjectView>>(
        `/objects${query({ limit: WHOLE, prefix: text, job: job ?? undefined })}`,
      );
      return {
        items: page.items.map((o) => ({ id: o.name, label: o.name, insert: `${scope} ${o.name}` })),
        more: false,
      };
    },
  };
}

function dashboards(text: string): SourceMatch {
  return {
    key: "dashboards",
    text: "",
    local: text,
    fetchPage: async () => {
      const page = await fetchJSON<Page<DashboardSummary>>("/viewer/dashboards");
      return {
        items: page.items.map((d) => ({ id: d.name, label: d.name, insert: `@dashboard ${d.name}` })),
        more: false,
      };
    },
  };
}

// The sandbox list has no name filter, so typed text is not part of the key.
function sandboxFiles(): SourceMatch {
  return {
    key: "sandbox",
    text: "",
    fetchPage: paged<SandboxFileView>("/sandbox", {}, (f) => ({
      id: f.id,
      label: f.name,
      detail: f.job_names.join(", "),
      insert: `@sandbox ${f.id}`,
    })),
  };
}

// The command's argument source for `draft`, or null when the draft is not yet
// `<command> ` (the dropdown then lists commands instead).
export function matchSource(draft: string, opts: { sandbox: boolean }): SourceMatch | null {
  let m = /^@job (\S+) data (.*)$/.exec(draft);
  if (m) return objects(`@job ${m[1]} data`, m[1], m[2]);
  m = /^@job (\S+) task (.*)$/.exec(draft);
  if (m) return tasks(m[2], m[1]);
  m = /^(@data) persistent (.*)$/.exec(draft);
  if (m) return objects(`${m[1]} persistent`, null, m[2]);
  m = /^(\S+) (.*)$/.exec(draft);
  if (!m) return null;
  const [, cmd, text] = m;
  switch (cmd) {
    case "@task":
      // A trailing view (`@task load data`) is not part of the name.
      return tasks(text.replace(/\s+data$/, ""));
    case "@job":
      // A trailing view (`@job etl graph`) is not part of the name.
      return jobs(cmd, text.replace(JOB_VIEW_SUFFIX, ""));
    case "cancel":
      return jobs(cmd, text);
    case "run":
    case "enable":
    case "disable":
    case "unregister":
      return registered(cmd, text);
    case "@data":
      return objects(cmd, null, text);
    case "@dashboard":
      return dashboards(text);
    case "@sandbox":
      return opts.sandbox ? sandboxFiles() : null;
    default:
      return null;
  }
}
