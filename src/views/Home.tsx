// See docs/designs/ui.md — "Home (empty prompt)".
import { type Cmd, useCommandGroups } from "../components/commands";

function CmdList({
  items,
  onPrompt,
  disabled = false,
}: {
  items: Cmd[];
  onPrompt: (v: string) => void;
  disabled?: boolean;
}) {
  return (
    <div className="cmd-list">
      {items.map((c) => (
        <div
          key={c.code}
          className={disabled ? "cmd disabled" : "cmd"}
          aria-disabled={disabled}
          onClick={disabled ? undefined : () => onPrompt(c.cmd)}
        >
          <code>{c.code}</code>
          <span className="desc">{c.desc}</span>
        </div>
      ))}
    </div>
  );
}

export function Home({ onPrompt }: { onPrompt: (v: string) => void }) {
  const { navigate, actions, account, admin, local } = useCommandGroups();
  const distOnly = local && <span className="dist-only"> (Distributed mode only)</span>;
  return (
    <>
      <h2>aaiclick</h2>
      <p className="sub">Prompt-driven operator dashboard. Type a command above, or click one below.</p>
      <div className="group-label">Navigate</div>
      <CmdList items={navigate} onPrompt={onPrompt} />
      <div className="group-label">Actions</div>
      <CmdList items={actions} onPrompt={onPrompt} />
      <div className="group-label">Account{distOnly}</div>
      <CmdList items={account} onPrompt={onPrompt} disabled={local} />
      {admin.length > 0 && (
        <>
          <div className="group-label">Administration{distOnly}</div>
          <CmdList items={admin} onPrompt={onPrompt} disabled={local} />
        </>
      )}
    </>
  );
}
