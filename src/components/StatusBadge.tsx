export function StatusBadge({ status }: { status: string }) {
  return <span className={`badge b-${status}`}>{status}</span>;
}
