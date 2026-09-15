import { describe, expect, test } from 'vitest'
import { cellText, columnNames, columnTypes, type QueryRows } from './rows'

const rows: QueryRows = {
  meta: [
    { name: 'id', type: 'UInt64' },
    { name: 'tags', type: 'Array(String)' },
  ],
  data: [['1', ['a', 'b']]],
}

describe('rows', () => {
  test('columnNames and columnTypes come from meta, in order', () => {
    expect(columnNames(rows)).toEqual(['id', 'tags'])
    expect(columnTypes(rows)).toEqual({ id: 'UInt64', tags: 'Array(String)' })
  })

  test('cellText renders scalars as text and null as empty', () => {
    expect(cellText('a')).toBe('a')
    expect(cellText(7)).toBe('7')
    expect(cellText(true)).toBe('true')
    expect(cellText(null)).toBe('')
  })

  test('cellText serializes containers as JSON', () => {
    expect(cellText(['a', 1])).toBe('["a",1]')
    expect(cellText({ x: 1 })).toBe('{"x":1}')
  })
})
