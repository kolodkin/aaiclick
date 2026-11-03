// See docs/designs/ui.md — "Prompt dropdown".
// Maps a prompt draft to the values its command takes: which endpoint lists
// them, how the typed text narrows them, and what a picked row inserts.
import { fetchJSON } from "../api/client";
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
  fetchPage(offset: number): Promise<SourcePage>;
}

function query(params: Record<string, string | number | undefined>): string {
  const q = new URLSearchParams();
  for (const [k, v] of Object.entries(params)) if (v !== undefined && v !== "") q.set(k, String(v));
  return `?${q}`;
}

// Offset-paged list endpoint; a full page means there may be more.
function paged<T>(path: string, params: Record<string, string | undefined>, toItem: (t: T) => SourceItem) {
  return async (offset: number): Promise<SourcePage> => {
    const page = await fetchJSON<Page<T>>(`${path}${query({ limit: PAGE, offset, ...params })}`);
    return { items: page.items.map(toItem), more: page.items.length === PAGE };
  };
}

const like = (text: string) => (text ? `%${text}%` : undefined);

function tasks(text: string): SourceMatch {
  return {
    key: "tasks",
    text,
    fetchPage: paged<TaskView>("/tasks", { name: text || undefined }, (t) => ({
      id: t.id,
      label: t.name,
      detail: `job ${t.job_id}`,
      status: t.status,
      at: t.created_at,
      insert: `@task ${t.id}`,
    })),
  };
}

// `cmd` is in the key: cached rows carry a finished `insert`, so commands sharing an endpoint must not share a cache.
function jobs(cmd: string, text: string, insert: (j: JobView) => string): SourceMatch {
  return {
    key: `jobs:${cmd}`,
    text,
    fetchPage: paged<JobView>("/jobs", { name: like(text) }, (j) => ({
      id: j.id,
      label: j.name,
      detail: j.id,
      status: j.status,
      at: j.created_at,
      insert: insert(j),
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

function objects(cmd: string, job: string | null, text: string): SourceMatch {
  const scope = job ? `${cmd} job ${job}` : cmd;
  return {
    key: job ? `objects:${job}` : "objects",
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
    text,
    fetchPage: async () => {
      const page = await fetchJSON<Page<DashboardSummary>>("/viewer/dashboards");
      const items = page.items
        .filter((d) => d.name.includes(text))
        .map((d) => ({ id: d.name, label: d.name, insert: `@dashboard ${d.name}` }));
      return { items, more: false };
    },
  };
}

function sandboxFiles(text: string): SourceMatch {
  return {
    key: "sandbox",
    text,
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
  let m = /^(@data|@query) job (\S+) (.*)$/.exec(draft);
  if (m) return objects(m[1], m[2], m[3]);
  m = /^(@data|@query) job (\S*)$/.exec(draft);
  if (m) {
    const cmd = m[1];
    return jobs(`${cmd} job`, m[2], (j) => `${cmd} job ${j.name}`);
  }
  m = /^(\S+) (.*)$/.exec(draft);
  if (!m) return null;
  const [, cmd, text] = m;
  switch (cmd) {
    case "@task":
      return tasks(text);
    case "@job":
      return jobs(cmd, text, (j) => `@job ${j.name}`);
    case "cancel":
      return jobs(cmd, text, (j) => `cancel ${j.id}`);
    case "run":
    case "enable":
    case "disable":
      return registered(cmd, text);
    case "@data":
    case "@query":
      return objects(cmd, null, text);
    case "@dashboard":
      return dashboards(text);
    case "@sandbox":
      return opts.sandbox ? sandboxFiles(text) : null;
    default:
      return null;
  }
}
