import { useToggleRegisteredJob } from "../api/hooks";
import { AdminButton } from "../components/AdminButton";
import { Chips } from "../components/Chips";
import { Panel } from "../components/Panel";
import { useToast } from "../components/Toast";

export function ToggleConfirm({
  name,
  enabled,
  onPrompt,
}: {
  name: string;
  enabled: boolean;
  onPrompt: (v: string) => void;
}) {
  const toggle = useToggleRegisteredJob();
  const toast = useToast();
  const verb = enabled ? "Enable" : "Disable";
  const onConfirm = () =>
    toggle.mutate(
      { name, enabled },
      {
        onSuccess: () => {
          toast(`${verb}d ${name}`);
          onPrompt("@registered");
        },
        onError: (e) => toast(`${verb} failed: ${e.message}`),
      },
    );

  return (
    <>
      <Chips chips={[{ label: "← @registered", cmd: "@registered" }]} onPrompt={onPrompt} />
      <Panel className="confirm">
        <h2>
          {verb} <span className="mono">{name}</span>?
        </h2>
        <p className="sub">
          {enabled ? "Its schedule starts firing again." : "Its schedule stops firing; running jobs are not touched."}
        </p>
        <div className="form-actions">
          <AdminButton className="btn" disabled={toggle.isPending} onClick={onConfirm}>
            {verb}
          </AdminButton>
          <button className="btn" onClick={() => onPrompt("@registered")}>
            Back
          </button>
        </div>
      </Panel>
    </>
  );
}
