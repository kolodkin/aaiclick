import { useCancelJob } from "../api/hooks";
import { AdminButton } from "../components/AdminButton";
import { Chips } from "../components/Chips";
import { Panel } from "../components/Panel";
import { useToast } from "../components/Toast";

export function CancelConfirm({ refId, onPrompt }: { refId: string; onPrompt: (v: string) => void }) {
  const cancel = useCancelJob();
  const toast = useToast();
  const onCancel = () =>
    cancel.mutate(refId, {
      onSuccess: () => {
        toast(`Cancelling ${refId}…`);
        onPrompt(`@job ${refId}`);
      },
      onError: (e) => toast(`Cancel failed: ${e.message}`),
    });

  return (
    <>
      <Chips chips={[{ label: `← @job ${refId}`, cmd: `@job ${refId}` }]} onPrompt={onPrompt} />
      <Panel className="confirm">
        <h2>
          Cancel <span className="mono">{refId}</span>?
        </h2>
        <p className="sub">
          POST /api/v0/jobs/{refId}/cancel — pending tasks are cancelled and any running task is signalled to abort.
        </p>
        <div className="form-actions">
          <AdminButton className="btn btn-danger" disabled={cancel.isPending} onClick={onCancel}>
            Cancel job
          </AdminButton>
          <button className="btn" onClick={() => onPrompt(`@job ${refId}`)}>
            Keep running
          </button>
        </div>
      </Panel>
    </>
  );
}
