import { useState } from "react";
import { useRunJob } from "../api/hooks";
import { Chips } from "../components/Chips";
import { Panel } from "../components/Panel";
import { useToast } from "../components/Toast";

export function RunForm({ name, onPrompt }: { name: string; onPrompt: (v: string) => void }) {
  const run = useRunJob();
  const toast = useToast();
  const [kwargs, setKwargs] = useState("{}");
  const [preservation, setPreservation] = useState<"" | "NONE" | "FULL">("");

  const onRun = () => {
    let parsed: Record<string, unknown>;
    try {
      parsed = JSON.parse(kwargs || "{}");
    } catch {
      toast("kwargs is not valid JSON");
      return;
    }
    run.mutate(
      { name, kwargs: parsed, preservation_mode: preservation || null },
      {
        onSuccess: (job) => {
          toast(`Started ${name} — job #${job.id}`);
          onPrompt(`@job ${name}`);
        },
        onError: (e) => toast(`Run failed: ${e.message}`),
      },
    );
  };

  return (
    <>
      <Chips chips={[{ label: "← @registered", cmd: "@registered" }]} onPrompt={onPrompt} />
      <Panel>
        <h2>
          Run <span className="mono">{name}</span>
        </h2>
        <p className="sub">POST /api/v0/jobs:run — edit parameters before launching.</p>
        <div className="field">
          <label>
            kwargs <span className="help">— JSON passed to the job entrypoint</span>
          </label>
          <textarea rows={5} value={kwargs} onChange={(e) => setKwargs(e.target.value)} />
        </div>
        <div className="field">
          <label>Preservation mode</label>
          <select value={preservation} onChange={(e) => setPreservation(e.target.value as "" | "NONE" | "FULL")}>
            <option value="">(registered default)</option>
            <option value="NONE">NONE</option>
            <option value="FULL">FULL</option>
          </select>
        </div>
        <div className="form-actions">
          <button className="btn btn-primary" disabled={run.isPending} onClick={onRun}>
            Run job
          </button>
          <button className="btn" onClick={() => onPrompt("@registered")}>
            Cancel
          </button>
        </div>
      </Panel>
    </>
  );
}
