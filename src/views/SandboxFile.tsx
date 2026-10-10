import { useSandboxFile } from "../api/hooks";
import { Chips } from "../components/Chips";
import { Panel } from "../components/Panel";
import { relativeTime } from "../lib/format";
import { SandboxJobLinks, SandboxStatusBadge } from "./Sandbox";

export function SandboxFile({ id, onPrompt }: { id: string; onPrompt: (v: string) => void }) {
  const file = useSandboxFile(id);
  return (
    <>
      <Chips chips={[{ label: "← @sandbox", cmd: "@sandbox" }]} onPrompt={onPrompt} />
      {file.isLoading && <p className="sub">loading…</p>}
      {file.isError && <p className="err">{file.error.message}</p>}
      {file.data && (
        <Panel>
          <h2>
            {file.data.name} <SandboxStatusBadge file={file.data} />
          </h2>
          <p className="sub mono">
            {file.data.path} @ {file.data.git_sha.slice(0, 7)} · {file.data.submitted_by ?? "—"} ·{" "}
            {relativeTime(file.data.created_at)}
          </p>
          <p>
            Jobs: <SandboxJobLinks file={file.data} onPrompt={onPrompt} />
          </p>
          {file.data.error && <p className="err">{file.data.error}</p>}
          <pre className="mono">{file.data.source}</pre>
        </Panel>
      )}
    </>
  );
}
