import { Panel } from "./Panel";
import { useToast } from "./Toast";

// A value the server returns exactly once — an API token, a password-reset
// link. It stays on screen until dismissed, because it cannot be fetched again.
export function SecretPanel({
  title,
  hint,
  value,
  onDone,
}: {
  title: string;
  hint: string;
  value: string;
  onDone: () => void;
}) {
  const toast = useToast();
  return (
    <Panel className="confirm info">
      <h2>{title}</h2>
      <p className="sub">{hint}</p>
      <p className="mono secret-value" data-testid="secret-value">
        {value}
      </p>
      <div className="form-actions">
        <button
          className="btn btn-primary"
          onClick={() => void navigator.clipboard?.writeText(value).then(() => toast("Copied"))}
        >
          Copy
        </button>
        <button className="btn" onClick={onDone}>
          Done
        </button>
      </div>
    </Panel>
  );
}
