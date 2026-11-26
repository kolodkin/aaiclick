// See docs/designs/ui.md — "Data (`@data [<object>]`)".
import { useObjects } from "../api/hooks";
import { Chips } from "../components/Chips";
import { LiveStatus } from "../components/LiveStatus";
import { ScopeData } from "../components/ScopeData";
import { PERSISTENT } from "../lib/viewer";

export function Data({ object, onPrompt }: { object: string | null; onPrompt: (v: string) => void }) {
  // Shares ScopeData's query (same key), only to report its freshness.
  const objects = useObjects(PERSISTENT, true);
  return (
    <>
      <h2>Data</h2>
      <p className="sub">
        Persistent objects · click a row to browse it ·{" "}
        <LiveStatus updatedAt={objects.dataUpdatedAt} queryKey="objects" />
      </p>
      <Chips
        chips={[
          { label: "← home", cmd: "" },
          { label: "@dashboard", cmd: "@dashboard" },
        ]}
        onPrompt={onPrompt}
      />
      <ScopeData scope={PERSISTENT} object={object} onPrompt={onPrompt} />
    </>
  );
}
