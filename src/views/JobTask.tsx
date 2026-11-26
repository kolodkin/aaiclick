// See docs/designs/ui.md — "Job Detail (`@job <id>`)".
import { useJob } from "../api/hooks";
import { Chips } from "../components/Chips";
import { TaskDetail } from "./TaskDetail";

// `@job <ref> task <name>`: a task reached by typing names. Opens the job's
// latest task of that name; the UI itself always links `@task <id>`.
export function JobTask({ job, task, onPrompt }: { job: string; task: string; onPrompt: (v: string) => void }) {
  const { data, isLoading, isError } = useJob(job);
  if (isLoading) return <p className="sub">loading…</p>;
  if (isError || !data)
    return (
      <>
        <h2>Job not found</h2>
        <p className="sub mono">{job}</p>
        <Chips chips={[{ label: "@jobs", cmd: "@jobs" }]} onPrompt={onPrompt} />
      </>
    );
  // Ids are 64-bit snowflakes in time order, too big to compare as numbers.
  const latest = data.tasks
    .filter((t) => t.name === task)
    .reduce<string | null>((id, t) => (id === null || BigInt(t.id) > BigInt(id) ? t.id : id), null);
  if (!latest)
    return (
      <>
        <h2>Task not found</h2>
        <p className="sub mono">
          {task} in job {job}
        </p>
        <Chips chips={[{ label: `← @job ${job}`, cmd: `@job ${job}` }]} onPrompt={onPrompt} />
      </>
    );
  return <TaskDetail id={latest} view="log" onPrompt={onPrompt} />;
}
