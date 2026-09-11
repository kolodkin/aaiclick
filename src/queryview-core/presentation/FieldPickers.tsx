// The "Select fields" / "Order by" pickers shared by the query panel and the
// explorer. Field toggles only change client-side column visibility; order-by
// changes go back to the parent, which decides when to re-run the query.

export type Field = { name: string; type: string }

export type OrderCol = { name: string; dir: 'ASC' | 'DESC' }

export function FieldPickers({
  fields,
  visibleCols,
  orderBy,
  onVisibleColsChange,
  onOrderByChange,
  orderHeaderExtra,
}: {
  fields: Field[]
  visibleCols: string[]
  orderBy: OrderCol[]
  onVisibleColsChange: (cols: string[]) => void
  onOrderByChange: (order: OrderCol[]) => void
  // Rendered next to the "Order by" label (e.g. the query panel's Run button).
  orderHeaderExtra?: React.ReactNode
}) {
  function toggleField(name: string) {
    onVisibleColsChange(
      visibleCols.includes(name)
        ? visibleCols.filter((c) => c !== name)
        : [...visibleCols, name],
    )
  }

  function toggleOrder(name: string) {
    onOrderByChange(
      orderBy.some((o) => o.name === name)
        ? orderBy.filter((o) => o.name !== name)
        : [...orderBy, { name, dir: 'ASC' }],
    )
  }

  function flipDir(name: string) {
    onOrderByChange(
      orderBy.map((o) =>
        o.name === name ? { ...o, dir: o.dir === 'ASC' ? 'DESC' : 'ASC' } : o,
      ),
    )
  }

  return (
    <div
      data-testid="field-pickers"
      className="space-y-3 rounded-xl border border-white/10 bg-white/[0.03] p-3"
    >
      <div>
        <div className="mb-2 flex items-center gap-2">
          <span className="text-sm font-medium text-slate-200">Select fields</span>
          <button
            type="button"
            data-testid="fields-select-all"
            onClick={() => onVisibleColsChange(fields.map((f) => f.name))}
            className="glass-btn px-2 py-0.5 text-xs"
          >
            Select all
          </button>
          <button
            type="button"
            data-testid="fields-clear"
            onClick={() => onVisibleColsChange([])}
            className="glass-btn px-2 py-0.5 text-xs"
          >
            Clear all
          </button>
        </div>
        <div className="flex flex-wrap gap-2">
          {fields.map((f) => {
            const on = visibleCols.includes(f.name)
            return (
              <button
                key={f.name}
                type="button"
                onClick={() => toggleField(f.name)}
                data-testid="field-toggle"
                data-col={f.name}
                data-on={on}
                title={f.type}
                className={`glass-toggle px-2.5 py-1 text-xs ${on ? 'is-active' : ''}`}
              >
                {f.name}
              </button>
            )
          })}
        </div>
      </div>

      <div>
        <div className="mb-2 flex items-center gap-2">
          <span className="text-sm font-medium text-slate-200">Order by</span>
          {orderHeaderExtra}
        </div>
        <div className="flex flex-wrap gap-2">
          {fields.map((f) => {
            const on = orderBy.some((o) => o.name === f.name)
            return (
              <button
                key={f.name}
                type="button"
                onClick={() => toggleOrder(f.name)}
                data-testid="orderby-add"
                data-col={f.name}
                data-on={on}
                className={`glass-toggle px-2.5 py-1 text-xs ${on ? 'is-active-soft' : ''}`}
              >
                {f.name}
              </button>
            )
          })}
        </div>
        {orderBy.length > 0 && (
          <div className="mt-2 flex flex-wrap items-center gap-2">
            {orderBy.map((o, i) => (
              <span
                key={o.name}
                data-testid="orderby-chip"
                data-col={o.name}
                className="flex items-center gap-1 rounded-md border border-indigo-400/40 bg-white/[0.06] px-2 py-1 text-xs"
              >
                <span className="text-slate-400">{i + 1}.</span>
                <span className="font-medium">{o.name}</span>
                <button
                  type="button"
                  data-testid="orderby-dir"
                  onClick={() => flipDir(o.name)}
                  className="rounded bg-white/10 px-1.5 py-0.5 font-mono hover:bg-white/20"
                >
                  {o.dir}
                </button>
                <button
                  type="button"
                  data-testid="orderby-remove"
                  onClick={() => toggleOrder(o.name)}
                  aria-label={`remove ${o.name}`}
                  className="text-slate-400 hover:text-red-400"
                >
                  ×
                </button>
              </span>
            ))}
          </div>
        )}
      </div>
    </div>
  )
}
