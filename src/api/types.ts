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

export type ObjectView = S["ObjectView"];
export type ObjectDetail = S["ObjectDetail"];
export type ColumnInfo = S["ColumnInfo"];
export type ColumnSchema = S["ColumnSchema"];
export type OrderByPair = S["OrderBy"];
export type ObjectQueryRequest = S["ObjectQueryRequest"];
export type ObjectQueryResult = S["ObjectQueryResult"];
export type SavedQuery = S["SavedQuery"];
export type SavedQueryBody = S["SavedQueryBody"];
export type DashboardSummary = S["DashboardSummary"];
export type Dashboard = S["Dashboard"];
export type DashboardResults = S["DashboardResults"];
export type Deleted = S["Deleted"];

// FastAPI emits a concrete schema per instantiation (`Page_JobView_`, …); keep
// a hand-written generic so call sites stay `Page<JobView>`.
export interface Page<T> {
  items: T[];
  total: number | null;
  next_cursor: string | null;
}
