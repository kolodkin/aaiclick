import { useState } from "react";
import { useJobs } from "../api/hooks";
import { PERSISTENT, scopeKey } from "../lib/viewer";

// Left-hand scope selector: Persistent, then the jobs (newest first) with a
// name filter. Job nodes reuse the jobs list; a click sets the scope key.
export function ScopeTree({ scope, onScope }: { scope: string; onScope: (scope: string) => void }) {
  const { data } = useJobs();
  const [filter, setFilter] = useState("");
  const jobs = (data?.items ?? []).filter((j) => j.name.includes(filter));
  return (
    <nav className="scope-tree" data-testid="scope-tree">
      <div
        className={`scope-node${scope === PERSISTENT ? " is-active" : ""}`}
        data-testid="scope-persistent"
        onClick={() => onScope(PERSISTENT)}
      >
        Persistent
      </div>
      <div className="scope-group">Jobs</div>
      <input
        className="scope-filter mono"
        placeholder="filter jobs…"
        value={filter}
        onChange={(e) => setFilter(e.target.value)}
        aria-label="Filter jobs"
      />
      {jobs.map((j) => {
        // Ids are 64-bit snowflakes carried as strings — never parse them.
        const id = String(j.id);
        const key = scopeKey(id);
        return (
          <div
            key={id}
            className={`scope-node mono${scope === key ? " is-active" : ""}`}
            data-testid="scope-job"
            title={`#${id}`}
            onClick={() => onScope(key)}
          >
            {j.name} <span className="sub-inline">#{id.slice(-6)}</span>
          </div>
        );
      })}
    </nav>
  );
}
