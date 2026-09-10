import { describe, expect, test } from 'vitest'
import { escapeHtml, substituteCellTemplate } from './cellView'

const columns = ['id', 'name', 'severity']
const row = ['1', 'alpha', 'high']

describe('substituteCellTemplate', () => {
  test('replaces {cell} with the encoded raw value', () => {
    const out = substituteCellTemplate(
      'https://example.com/{cell}',
      'a b',
      row,
      columns,
      encodeURIComponent,
    )
    expect(out).toBe('https://example.com/a%20b')
  })

  test('replaces {row.<col>} with another column value in the same row', () => {
    const out = substituteCellTemplate(
      'https://example.com/{row.name}',
      '1',
      row,
      columns,
      encodeURIComponent,
    )
    expect(out).toBe('https://example.com/alpha')
  })

  test('combines {cell} and {row.<col>} in one template', () => {
    const out = substituteCellTemplate(
      '<a>{cell} ({row.severity})</a>',
      '1',
      row,
      columns,
      escapeHtml,
    )
    expect(out).toBe('<a>1 (high)</a>')
  })

  test('applies the encoder to row values too', () => {
    const out = substituteCellTemplate(
      '<b>{row.name}</b>',
      'x',
      ['1', '<script>'],
      ['id', 'name'],
      escapeHtml,
    )
    // The referenced column's value carries markup that must be escaped.
    expect(out).toBe('<b>&lt;script&gt;</b>')
  })

  test('leaves an unknown {row.<col>} placeholder untouched', () => {
    const out = substituteCellTemplate(
      '{row.missing}',
      '1',
      row,
      columns,
      encodeURIComponent,
    )
    expect(out).toBe('{row.missing}')
  })

  test('substitutes a missing row value as empty string', () => {
    const out = substituteCellTemplate(
      'x{row.severity}y',
      '1',
      ['1', 'alpha'], // shorter than columns: no severity value
      columns,
      encodeURIComponent,
    )
    expect(out).toBe('xy')
  })

  test('does not re-interpret an injected value as a placeholder', () => {
    // The cell's own value contains placeholder-looking text; a single pass means
    // it is encoded as literal text, never resolved against the row.
    const out = substituteCellTemplate(
      '{cell}',
      '{row.severity}',
      row,
      columns,
      escapeHtml,
    )
    expect(out).toBe('{row.severity}')
  })

  test('replaces every occurrence of a placeholder', () => {
    const out = substituteCellTemplate(
      '{row.name}-{row.name}',
      '1',
      row,
      columns,
      (s) => s,
    )
    expect(out).toBe('alpha-alpha')
  })
})

describe('escapeHtml', () => {
  test('escapes the five HTML-significant characters', () => {
    expect(escapeHtml(`<>&"'`)).toBe('&lt;&gt;&amp;&quot;&#39;')
  })
})
