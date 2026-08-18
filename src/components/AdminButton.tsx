import type { ComponentProps } from "react";
import { useAuth } from "./Auth";

export const ADMIN_ONLY_HINT = "Requires the admin role — you are signed in as a viewer.";

// A button for an admin-only action. Viewers see it disabled with an
// explanatory tooltip rather than not at all: hiding the control makes the
// feature look missing or broken, where a greyed-out one shows the action
// exists and why it is unavailable. The server enforces the rule either way
// (see docs/designs/auth.md) — this only saves a viewer a pointless 403.
export function AdminButton({ disabled, title, ...rest }: ComponentProps<"button">) {
  const { isAdmin } = useAuth();
  return <button {...rest} disabled={disabled || !isAdmin} title={isAdmin ? title : ADMIN_ONLY_HINT} />;
}
