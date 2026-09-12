// The built-in "default views" of complex result cells (Array / Map / Tuple).
// Values arrive structured (JSON arrays and objects), so the only parsing left
// is the column *type* string, which decides map (`key → value`) versus tuple
// (`name: value`) labelling. No React here — see ComplexCell.tsx and
// docs/query.md ("Default views for complex types").

import { cellText, isContainer, type Cell } from '../results/rows'

export type ElementKind = 'tuple' | 'map' | null

export type ComplexType = { kind: 'array'; element: ElementKind } | { kind: 'map' } | { kind: 'tuple' }

// Match a `Name(<inner>)` wrapper, returning the constructor name and inner text.
function matchWrapper(type: string): { name: string; inner: string } | null {
  const m = /^([A-Za-z0-9_]+)\((.*)\)$/.exec(type.trim())
  if (!m) return null
  return { name: m[1], inner: m[2] }
}

// Parse a type string into a descriptor, or null for scalar/unsupported types.
// Nesting is honored one level deep (Array(Tuple(...)) / Array(Map(...))).
export function parseComplexType(type: string): ComplexType | null {
  // DESCRIBE pretty-prints complex types across indented lines; flatten any
  // whitespace (type strings never contain string literals).
  const flat = type.replace(/\\[nt]/g, ' ').replace(/\s+/g, ' ')
  const w = matchWrapper(flat)
  if (!w) return null
  if (w.name === 'Nullable' || w.name === 'LowCardinality') return parseComplexType(w.inner)
  if (w.name === 'Array') {
    const inner = matchWrapper(w.inner)
    const element: ElementKind = inner?.name === 'Tuple' ? 'tuple' : inner?.name === 'Map' ? 'map' : null
    return { kind: 'array', element }
  }
  if (w.name === 'Map') return { kind: 'map' }
  if (w.name === 'Tuple') return { kind: 'tuple' }
  return null
}

// Collapsed cells show at most this many items before the expander.
export const PREVIEW_COUNT = 3

// One rendered item: a single line (array element, map entry, tuple field) or
// a group of lines. The collapse threshold counts items.
export type CellItem = { lines: string[] }

// The lines of one container value: `key → value` for a map, `name: value`
// for a named tuple (an object), `index: value` for an unnamed one (an array).
function entryLines(v: Cell[] | { [key: string]: Cell }, kind: ElementKind): string[] {
  if (Array.isArray(v)) return v.map((x, i) => `${i}: ${cellText(x)}`)
  const sep = kind === 'map' ? ' → ' : ': '
  return Object.entries(v).map(([k, x]) => `${k}${sep}${cellText(x)}`)
}

// Turn a container value into its full (expanded) list of items per the type
// descriptor; a scalar yields none. Without a type, the value's own shape
// decides. The component applies PREVIEW_COUNT and the expander on top.
export function complexCellItems(type: ComplexType | null, value: Cell): CellItem[] {
  if (!isContainer(value)) return []
  if (Array.isArray(value)) {
    if (type?.kind === 'tuple') return entryLines(value, null).map((line) => ({ lines: [line] }))
    const element = type?.kind === 'array' ? type.element : null
    // A list of tuples/maps reads best with each element on one line.
    return value.map((el) => ({
      lines: [isContainer(el) && !Array.isArray(el) ? entryLines(el, element).join(', ') : cellText(el)],
    }))
  }
  const kind: ElementKind = type?.kind === 'map' ? 'map' : 'tuple'
  return entryLines(value, kind).map((line) => ({ lines: [line] }))
}
