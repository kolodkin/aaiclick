import { isValidElement, type ReactElement } from 'react'
import { describe, expect, test } from 'vitest'
import { parseCellViewYaml, renderCell } from './cellViewYaml'

const columns = ['id', 'tags']
const row = [7, ['a', 'b']]

describe('parseCellViewYaml', () => {
  test('empty or invalid input yields no views', () => {
    expect(parseCellViewYaml(null)).toEqual({})
    expect(parseCellViewYaml('')).toEqual({})
    expect(parseCellViewYaml('- not: a map')).toEqual({})
  })

  test('keeps {type, value} entries and drops malformed ones and params', () => {
    const yaml = [
      'id:',
      '  type: link',
      '  value: https://x/{cell}',
      'tags:',
      '  type: custom',
      'params:',
      '  - name: p',
      '    options: [a]',
    ].join('\n')
    expect(parseCellViewYaml(yaml)).toEqual({ id: { type: 'link', value: 'https://x/{cell}' } })
  })
})

function props(node: unknown): Record<string, unknown> {
  expect(isValidElement(node)).toBe(true)
  return (node as ReactElement<Record<string, unknown>>).props
}

describe('renderCell', () => {
  test('plain text when no view and a scalar value', () => {
    expect(renderCell('id', 7, {}, row, columns, { id: 'UInt64' })).toBe('7')
    expect(renderCell('id', null, {}, row, columns, {})).toBe('')
  })

  test('container values get the built-in view unless a cell view overrides', () => {
    const node = renderCell('tags', row[1], {}, row, columns, { tags: 'Array(String)' })
    expect(props(node).col).toBe('tags')
    expect(props(node).value).toEqual(['a', 'b'])
    // The value alone is enough — no column type needed (Postgres arrays, JSON).
    expect(props(renderCell('tags', { k: 1 }, {}, row, columns, {})).value).toEqual({ k: 1 })
    const views = { tags: { type: 'link', value: 'https://x/{cell}' } }
    expect(props(renderCell('tags', row[1], views, row, columns, { tags: 'Array(String)' })).href).toBe(
      'https://x/%5B%22a%22%2C%22b%22%5D',
    )
  })

  test('link view substitutes and URL-encodes, http(s) only', () => {
    const views = { id: { type: 'link', value: 'https://x/{cell}?t={row.tags}' } }
    expect(props(renderCell('id', 'a b', views, row, columns, {})).href).toBe(
      'https://x/a%20b?t=%5B%22a%22%2C%22b%22%5D',
    )
    const js = { id: { type: 'link', value: 'javascript:alert(1)' } }
    expect(renderCell('id', '7', js, row, columns, {})).toBe('7')
    const rel = { id: { type: 'link', value: '/local/{cell}' } }
    expect(renderCell('id', '7', rel, row, columns, {})).toBe('7')
  })

  test('custom view escapes the substituted values', () => {
    const views = { id: { type: 'custom', value: '<b>{cell}</b>' } }
    const html = props(renderCell('id', '<i>', views, row, columns, {})).dangerouslySetInnerHTML
    expect(html).toEqual({ __html: '<b>&lt;i&gt;</b>' })
  })

  test('unknown view type falls back to plain text', () => {
    expect(renderCell('id', '7', { id: { type: 'nope', value: '' } }, row, columns, {})).toBe('7')
  })
})
