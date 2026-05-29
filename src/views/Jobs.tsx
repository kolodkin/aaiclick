import { useJobs } from "../api/hooks";
import { Chips } from "../components/Chips";
import { JobsTable } from "../components/JobsTable";

export function Jobs({ onPrompt }: { onPrompt: (v: string) => void }) {
  const { data, isLoading, isError } = useJobs();
  return (
    <>
      <h2>Jobs</h2>
      <p className="sub">Sorted by created_at, newest first · auto-refreshes</p>
      <Chips
        chips={[
          { label: "← home", cmd: "" },
          { label: "@registered", cmd: "@registered" },
        ]}
        onPrompt={onPrompt}
      />
      {isLoading && <p className="sub">loading…</p>}
      {isError && <p className="err">failed to load jobs</p>}
      {data && <JobsTable jobs={data.items} onPrompt={onPrompt} />}
    </>
  );
}
