import { useState } from "react";
import { useRegisterJob } from "../api/hooks";
import { Chips } from "../components/Chips";
import { Panel } from "../components/Panel";
import { useToast } from "../components/Toast";

export function RegisterForm({ name, onPrompt }: { name: string; onPrompt: (v: string) => void }) {
  const register = useRegisterJob();
  const toast = useToast();
  const [entrypoint, setEntrypoint] = useState("");
  const [jobName, setJobName] = useState(name);
  const [schedule, setSchedule] = useState("");
  const [kwargs, setKwargs] = useState("");
  const [enabled, setEnabled] = useState(true);

  const onRegister = () => {
    let parsed: Record<string, unknown> | null = null;
    if (kwargs.trim()) {
      try {
        parsed = JSON.parse(kwargs);
      } catch {
        toast("default_kwargs is not valid JSON");
        return;
      }
    }
    register.mutate(
      {
        entrypoint,
        // Server derives the name from the entrypoint when this is empty.
        name: jobName,
        schedule: schedule || null,
        default_kwargs: parsed,
        enabled,
        runner_mode: "subprocess",
      },
      {
        onSuccess: (rj) => {
          toast(`Registered ${rj.name}`);
          onPrompt("@registered");
        },
        onError: (e) => toast(`Register failed: ${e.message}`),
      },
    );
  };

  return (
    <>
      <Chips chips={[{ label: "← @registered", cmd: "@registered" }]} onPrompt={onPrompt} />
      <Panel>
        <h2>Register a job</h2>
        <p className="sub">POST /api/v0/registered-jobs — the entrypoint must exist in the deployed code.</p>
        <div className="field">
          <label>
            Entrypoint <span className="help">— dotted path, e.g. tasks.report.build</span>
          </label>
          <input type="text" value={entrypoint} onChange={(e) => setEntrypoint(e.target.value)} placeholder="package.module.callable" />
        </div>
        <div className="field">
          <label>
            Name <span className="help">— defaults to the last segment of the entrypoint</span>
          </label>
          <input type="text" value={jobName} onChange={(e) => setJobName(e.target.value)} placeholder="(optional)" />
        </div>
        <div className="field">
          <label>
            Schedule <span className="help">— cron, e.g. 0 2 * * * · leave blank for manual</span>
          </label>
          <input type="text" value={schedule} onChange={(e) => setSchedule(e.target.value)} placeholder="(optional cron expression)" />
        </div>
        <div className="field">
          <label>
            default_kwargs <span className="help">— JSON</span>
          </label>
          <textarea rows={3} value={kwargs} onChange={(e) => setKwargs(e.target.value)} placeholder="{}" />
        </div>
        <div className="field inline">
          <input type="checkbox" id="reg-enabled" checked={enabled} onChange={(e) => setEnabled(e.target.checked)} />
          <label htmlFor="reg-enabled">Enabled</label>
        </div>
        <div className="form-actions">
          <button className="btn btn-primary" disabled={register.isPending || !entrypoint} onClick={onRegister}>
            Register
          </button>
          <button className="btn" onClick={() => onPrompt("@registered")}>
            Cancel
          </button>
        </div>
      </Panel>
    </>
  );
}
