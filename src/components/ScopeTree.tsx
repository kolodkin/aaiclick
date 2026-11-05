import { useState } from "react";
import { useJobs } from "../api/hooks";
import { ALL, PERSISTENT, scopeKey } from "../lib/viewer";

// Left-hand scope selector: All, Persistent, then the jobs (newest first) with a
// name filter. Job nodes reuse the jobs list; a click sets the scope key.
export function ScopeTree({ scope, onScope }: { scope: string; onScope: (scope: string) => void }) {
  const { data } = useJobs({ poll: false });
  const [filter, setFilter] = useState("");
  const [expanded, setExpanded] = useState(false);
  const jobs = (data?.items ?? []).filter((j) => j.name.includes(filter));
  return (
    <nav className="scope-tree" data-testid="scope-tree">
      {(
        [
          [ALL, "All", "scope-all"],
          [PERSISTENT, "Persistent", "scope-persistent"],
        ] as const
      ).map(([key, label, testid]) => (
        <div
          key={key}
          className={`scope-node${scope === key ? " is-active" : ""}`}
          data-testid={testid}
          onClick={() => onScope(key)}
        >
          {label}
        </div>
      ))}
      <div className="scope-group">
        Jobs
        <button
          type="button"
          className="scope-toggle"
          data-testid="scope-jobs-toggle"
          aria-expanded={expanded}
          aria-label={expanded ? "Shrink job list" : "Expand job list"}
          title={expanded ? "Shrink" : "Expand"}
          onClick={() => setExpanded(!expanded)}
        >
          {expanded ? "▴" : "▾"}
        </button>
      </div>
      <input
        className="scope-filter mono"
        placeholder="filter jobs…"
        value={filter}
        onChange={(e) => setFilter(e.target.value)}
        aria-label="Filter jobs"
      />
      <div className={`scope-jobs${expanded ? " is-expanded" : ""}`}>
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
      </div>
    </nav>
  );
}
