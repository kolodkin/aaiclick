export interface MetaItem {
  k: string;
  v: React.ReactNode;
  mono?: boolean;
}

export function MetaGrid({ items }: { items: MetaItem[] }) {
  return (
    <div className="meta">
      {items.map((it) => (
        <div key={it.k}>
          <span className="k">{it.k}</span>
          {it.mono ? <span className="mono">{it.v}</span> : it.v}
        </div>
      ))}
    </div>
  );
}
