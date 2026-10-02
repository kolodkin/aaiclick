export interface MetaItem {
  k: string;
  v: React.ReactNode;
  mono?: boolean;
  /** Span the whole row — for long values such as entrypoints. */
  wide?: boolean;
}

export function MetaGrid({ items }: { items: MetaItem[] }) {
  return (
    <div className="meta">
      {items.map((it) => (
        <div key={it.k} className={it.wide ? "meta-wide" : undefined}>
          <span className="k">{it.k}</span>
          {it.mono ? <span className="mono">{it.v}</span> : it.v}
        </div>
      ))}
    </div>
  );
}
