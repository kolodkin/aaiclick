import type { JobView } from "../api/types";
import { durationBetween, relativeTime } from "../lib/format";
import { ProgressBar } from "./ProgressBar";
import { StatusBadge } from "./StatusBadge";

export function JobsTable({ jobs, onPrompt }: { jobs: JobView[]; onPrompt: (v: string) => void }) {
  return (
    <table>
      <thead>
        <tr>
          <th>Name</th>
          <th>Status</th>
          <th>Progress</th>
          <th>Created</th>
          <th>Duration</th>
        </tr>
      </thead>
      <tbody>
        {jobs.map((j) => (
          <tr key={j.id} className="clickable" onClick={() => onPrompt(`@job ${j.name}`)}>
            <td>
              <span className="name-link mono">{j.name}</span>
            </td>
            <td>
              <StatusBadge status={j.status} reason={j.error} />
            </td>
            <td>
              <ProgressBar done={j.completed_tasks} total={j.total_tasks} />
            </td>
            <td>{relativeTime(j.created_at)}</td>
            <td className="mono">{durationBetween(j.started_at, j.completed_at)}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}
