// Ergonomic names over the generated OpenAPI schema (`schema.ts`, produced by
// `npm run gen-types`). Re-exporting here keeps consumers on stable names and
// lets the types track the server schema automatically. Edit the mappings when
// the server adds a model; never hand-edit `schema.ts`.
import type { components } from "./schema";

type S = components["schemas"];

export type MeView = S["MeView"];
export type JobStatus = S["JobView"]["status"];
export type TaskStatus = S["TaskView"]["status"];

export type JobView = S["JobView"];
export type TaskView = S["TaskView"];
export type JobDetail = S["JobDetail"];
export type TaskDetail = S["TaskDetail"];
export type TaskLogs = S["TaskLogsView"];
export type LogLine = S["LogLine"];
export type RegisteredJobView = S["RegisteredJobView"];
export type JobGraphView = S["JobGraphView"];
export type GraphNodeView = S["GraphNodeView"];
export type GraphEdgeView = S["GraphEdgeView"];
export type Problem = S["Problem"];
export type RunJobRequest = S["RunJobRequest"];
export type RegisterJobRequest = S["RegisterJobRequest"];
export type UserView = S["UserView"];
export type CreateUserRequest = S["CreateUserRequest"];
export type ChangePasswordRequest = S["ChangePasswordRequest"];
export type ApiTokenView = S["ApiTokenView"];
export type ApiTokenCreated = S["ApiTokenCreated"];
export type CreateApiTokenRequest = S["CreateApiTokenRequest"];

// FastAPI emits a concrete schema per instantiation (`Page_JobView_`, …); keep
// a hand-written generic so call sites stay `Page<JobView>`.
export interface Page<T> {
  items: T[];
  total: number | null;
  next_cursor: string | null;
}
