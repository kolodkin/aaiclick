import { useState } from "react";
import { useSandboxFiles, useSubmitSandbox } from "../api/hooks";
import type { SandboxFileView } from "../api/types";
import { Chips } from "../components/Chips";
import { LiveStatus } from "../components/LiveStatus";
import { Panel } from "../components/Panel";
import { relativeTime } from "../lib/format";

export const SANDBOX_EXAMPLE = `from aaiclick.orchestration import TaskResult, job, task


@task
async def hello():
    return "hello"


@job
def hello_job():
    return TaskResult(tasks=[hello()])
`;

export function SandboxJobLinks({ file, onPrompt }: { file: SandboxFileView; onPrompt: (v: string) => void }) {
  const module = file.path.split("/").pop()?.replace(/\.py$/, "") ?? "";
  return (
    <>
      {file.job_names.map((fn, i) => {
        const jobName = `${module}.${fn}`;
        const ran = file.job_ids !== null && file.job_ids !== undefined && i < file.job_ids.length;
        return ran ? (
          <span key={fn} className="name-link mono" onClick={() => onPrompt(`@job ${jobName}`)}>
            {fn}{" "}
          </span>
        ) : (
          <span key={fn} className="mono">
            {fn}{" "}
          </span>
        );
      })}
    </>
  );
}

export function Sandbox({ onPrompt }: { onPrompt: (v: string) => void }) {
  const files = useSandboxFiles();
  const submit = useSubmitSandbox();
  const [name, setName] = useState("");
  const [source, setSource] = useState(SANDBOX_EXAMPLE);

  const onSubmit = () => {
    submit.mutate({ name, source }, { onSuccess: () => setName("") });
  };

  return (
    <>
      <Chips chips={[{ label: "← @jobs", cmd: "@jobs" }]} onPrompt={onPrompt} />
      <Panel>
        <h2>Sandbox</h2>
        <p className="sub">
          One Python file of @task / @job definitions. It is committed to the sandbox repo and every @job in it runs
          once.
        </p>
        <div className="field">
          <label>
            Name <span className="help">— letters, digits, underscores</span>
          </label>
          <input id="sandbox-name" type="text" value={name} onChange={(e) => setName(e.target.value)} />
        </div>
        <div className="field">
          <label>Source</label>
          <textarea
            id="sandbox-source"
            className="mono"
            rows={16}
            value={source}
            spellCheck={false}
            onChange={(e) => setSource(e.target.value)}
          />
        </div>
        <button
          id="sandbox-submit"
          className="btn btn-primary"
          disabled={submit.isPending || name.trim() === ""}
          onClick={onSubmit}
        >
          {submit.isPending ? "Submitting…" : "Submit"}
        </button>
        {submit.isError && <p className="err">{submit.error.message}</p>}
      </Panel>
      <h2>Submissions</h2>
      <p className="sub">
        Newest first. · <LiveStatus updatedAt={files.dataUpdatedAt} queryKey="sandbox" />
      </p>
      {files.isLoading && <p className="sub">loading…</p>}
      {files.isError && <p className="err">failed to load submissions</p>}
      {files.data && (
        <table>
          <thead>
            <tr>
              <th>Name</th>
              <th>Submitted by</th>
              <th>Time</th>
              <th>Status</th>
              <th>SHA</th>
              <th>Jobs</th>
            </tr>
          </thead>
          <tbody>
            {files.data.items.map((f) => (
              <tr key={f.id} data-sandbox-id={f.id}>
                <td>
                  <span className="name-link mono" onClick={() => onPrompt(`@sandbox ${f.id}`)}>
                    {f.name}
                  </span>
                </td>
                <td>{f.submitted_by ?? "—"}</td>
                <td>{relativeTime(f.created_at)}</td>
                <td className="status">
                  <span className={`badge chip-${f.status}`} title={f.error ?? undefined}>
                    {f.status}
                  </span>
                </td>
                <td className="sha mono">{f.git_sha.slice(0, 7)}</td>
                <td>
                  <SandboxJobLinks file={f} onPrompt={onPrompt} />
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </>
  );
}
