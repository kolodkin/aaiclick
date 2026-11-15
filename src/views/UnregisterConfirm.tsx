import { useUnregisterJob } from "../api/hooks";
import { AdminButton } from "../components/AdminButton";
import { Chips } from "../components/Chips";
import { Panel } from "../components/Panel";
import { useToast } from "../components/Toast";

export function UnregisterConfirm({ name, onPrompt }: { name: string; onPrompt: (v: string) => void }) {
  const unregister = useUnregisterJob();
  const toast = useToast();
  const onConfirm = () =>
    unregister.mutate(name, {
      onSuccess: () => {
        toast(`Unregistered ${name}`);
        onPrompt("@registered");
      },
      onError: (e) => toast(`Unregister failed: ${e.message}`),
    });

  return (
    <>
      <Chips chips={[{ label: "← @registered", cmd: "@registered" }]} onPrompt={onPrompt} />
      <Panel className="confirm">
        <h2>
          Unregister <span className="mono">{name}</span>?
        </h2>
        <p className="sub">The registration is deleted. Past runs stay in job history.</p>
        <div className="form-actions">
          <AdminButton className="btn" disabled={unregister.isPending} onClick={onConfirm}>
            Unregister
          </AdminButton>
          <button className="btn" onClick={() => onPrompt("@registered")}>
            Back
          </button>
        </div>
      </Panel>
    </>
  );
}
