export function Panel({ className = "", children }: { className?: string; children: React.ReactNode }) {
  return <div className={`panel ${className}`}>{children}</div>;
}
