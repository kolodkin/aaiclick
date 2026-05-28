import type { Problem } from "./types";

export const API = "/api/v0";

export class ApiError extends Error {
  status: number;
  problem: Problem | null;
  constructor(status: number, problem: Problem | null, message: string) {
    super(message);
    this.status = status;
    this.problem = problem;
  }
}

async function parseError(res: Response): Promise<ApiError> {
  let problem: Problem | null = null;
  try {
    problem = (await res.json()) as Problem;
  } catch {
    problem = null;
  }
  const detail = problem?.detail ?? problem?.title ?? res.statusText;
  return new ApiError(res.status, problem, detail);
}

export async function fetchJSON<T>(path: string): Promise<T> {
  const res = await fetch(`${API}${path}`);
  if (!res.ok) throw await parseError(res);
  return (await res.json()) as T;
}

export async function postJSON<T>(path: string, body?: unknown): Promise<T> {
  const res = await fetch(`${API}${path}`, {
    method: "POST",
    headers: body === undefined ? {} : { "Content-Type": "application/json" },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  if (!res.ok) throw await parseError(res);
  return (await res.json()) as T;
}
