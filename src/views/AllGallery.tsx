import { Chips } from "../components/Chips";

const SCREENS: { label: string; cmd: string }[] = [
  { label: "Home", cmd: "" },
  { label: "Jobs list", cmd: "@jobs" },
  { label: "Registered jobs", cmd: "@registered" },
  { label: "Register a job", cmd: "register" },
];

export function AllGallery({ onPrompt }: { onPrompt: (v: string) => void }) {
  return (
    <>
      <Chips chips={[{ label: "← home", cmd: "" }]} onPrompt={onPrompt} />
      <h2>All screens</h2>
      <p className="sub">Jump to any live screen.</p>
      <div className="cmd-list">
        {SCREENS.map((s) => (
          <div key={s.label} className="cmd" onClick={() => onPrompt(s.cmd)}>
            <code>{s.cmd || "(home)"}</code>
            <span className="desc">{s.label}</span>
          </div>
        ))}
      </div>
    </>
  );
}
