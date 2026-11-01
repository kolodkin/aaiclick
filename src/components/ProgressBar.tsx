export function ProgressBar({ done = 0, total = 0 }: { done?: number; total?: number }) {
  const pct = total > 0 ? Math.round((done / total) * 100) : 0;
  return (
    <div className="progress">
      <div className="bar">
        <span style={{ width: `${pct}%` }} />
      </div>
      <span className="mono">
        {done}/{total}
      </span>
    </div>
  );
}
