import { useToggleRegisteredJob } from "../api/hooks";
import { AdminButton } from "./AdminButton";
import { useToast } from "./Toast";

export function EnabledToggle({ name, enabled }: { name: string; enabled: boolean }) {
  const toggle = useToggleRegisteredJob();
  const toast = useToast();
  const onClick = () => {
    const next = !enabled;
    toggle.mutate(
      { name, enabled: next },
      { onSuccess: () => toast(`${next ? "Enabled" : "Disabled"} ${name}`) },
    );
  };
  return (
    <AdminButton className={`toggle ${enabled ? "on" : "off"}`} onClick={onClick}>
      <span className="switch" />
      {enabled ? "enabled" : "disabled"}
    </AdminButton>
  );
}
