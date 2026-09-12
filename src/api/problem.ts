import type { Problem } from "./types";

// Neutral module so both `client.ts` and `lib/auth.ts` can raise and inspect
// the same error type — `lib/auth.ts` cannot import `client.ts` (it *is* the
// 401-refresh path that `client.ts` calls).
export class ApiError extends Error {
  status: number;
  problem: Problem | null;
  constructor(status: number, problem: Problem | null, message: string) {
    super(message);
    this.status = status;
    this.problem = problem;
  }

  /** The stable machine-readable code, e.g. `"mfa_required"`. */
  get code(): string | null {
    return this.problem?.code ?? null;
  }
}

export async function parseError(res: Response): Promise<ApiError> {
  let problem: Problem | null = null;
  try {
    problem = (await res.json()) as Problem;
  } catch {
    problem = null;
  }
  return new ApiError(res.status, problem, problem?.detail ?? problem?.title ?? res.statusText);
}
