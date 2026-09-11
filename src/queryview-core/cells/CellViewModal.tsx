import { useState } from 'react'

type Props = {
  initial: string
  onCancel: () => void
  onSave: (value: string) => void
  saveDisabled: boolean
}

export function CellViewModal({ initial, onCancel, onSave, saveDisabled }: Props) {
  const [value, setValue] = useState(initial)
  return (
    <div
      role="dialog"
      aria-modal="true"
      aria-label="Cell view editor"
      data-testid="cell-view-modal"
      className="fixed inset-0 z-30 flex items-center justify-center bg-black/50 p-6 backdrop-blur-sm"
      onClick={(e) => {
        if (e.target === e.currentTarget) onCancel()
      }}
    >
      <div className="glass-popover w-full max-w-2xl p-5">
        <div className="mb-3">
          <h3 className="text-base font-semibold text-slate-100">Cell view (YAML)</h3>
          <p className="mt-1 text-xs text-slate-400">
            Maps column → render rule. Applied on Save.
          </p>
        </div>
        <textarea
          value={value}
          onChange={(e) => setValue(e.target.value)}
          aria-label="Cell view YAML"
          data-testid="cell-view-input"
          rows={10}
          autoFocus
          placeholder={'cve_id:\n  type: link\n  value: https://nvd.nist.gov/vuln/detail/{cell}'}
          className="glass-input w-full px-3 py-2 font-mono text-xs"
        />
        <div className="mt-4 flex items-center justify-end gap-2">
          <button
            type="button"
            onClick={onCancel}
            data-testid="cell-view-cancel"
            className="glass-btn px-3 py-1.5 text-sm"
          >
            Cancel
          </button>
          <button
            type="button"
            onClick={() => onSave(value)}
            disabled={saveDisabled}
            data-testid="cell-view-save"
            className="glass-btn-primary px-4 py-1.5 text-sm"
          >
            Save
          </button>
        </div>
      </div>
    </div>
  )
}
