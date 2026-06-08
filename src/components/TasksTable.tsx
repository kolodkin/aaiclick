import type { TaskView } from "../api/types";
import { durationBetween, relativeTime } from "../lib/format";
import { StatusBadge } from "./StatusBadge";

export function TasksTable({ tasks, onPrompt }: { tasks: TaskView[]; onPrompt: (v: string) => void }) {
  return (
    <table>
      <thead>
        <tr>
          <th>Name</th>
          <th>Status</th>
          <th>Entrypoint</th>
          <th>Attempt</th>
          <th>Started</th>
          <th>Duration</th>
        </tr>
      </thead>
      <tbody>
        {tasks.map((t) => (
          <tr key={t.id} className="clickable" onClick={() => onPrompt(`@task ${t.id}`)}>
            <td>
              <span className="name-link mono">{t.name}</span>
            </td>
            <td>
              <StatusBadge status={t.status} reason={t.error} />
            </td>
            <td className="mono">{t.entrypoint}</td>
            <td className="mono">{t.attempt}</td>
            <td>{relativeTime(t.started_at)}</td>
            <td className="mono">{durationBetween(t.started_at, t.completed_at)}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}
