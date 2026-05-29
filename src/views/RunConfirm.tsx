import { useRunJob } from "../api/hooks";
import { Chips } from "../components/Chips";
import { Panel } from "../components/Panel";
import { useToast } from "../components/Toast";

export function RunConfirm({ name, onPrompt }: { name: string; onPrompt: (v: string) => void }) {
  const run = useRunJob();
  const toast = useToast();
  const onRun = () =>
    run.mutate(
      { name },
      {
        onSuccess: (job) => {
          toast(`Started ${name} — job #${job.id}`);
          onPrompt(`@job ${name}`);
        },
        onError: (e) => toast(`Run failed: ${e.message}`),
      },
    );

  return (
    <>
      <Chips chips={[{ label: "← @registered", cmd: "@registered" }]} onPrompt={onPrompt} />
      <Panel className="confirm info">
        <h2>
          Run <span className="mono">{name}</span>?
        </h2>
        <p className="sub">Starts a new job with the registered default parameters.</p>
        <div className="form-actions">
          <button className="btn btn-primary" disabled={run.isPending} onClick={onRun}>
            Run job
          </button>
          <button className="btn" onClick={() => onPrompt(`run ${name} ?`)}>
            Edit parameters…
          </button>
          <button className="btn" onClick={() => onPrompt("@registered")}>
            Cancel
          </button>
        </div>
      </Panel>
    </>
  );
}
