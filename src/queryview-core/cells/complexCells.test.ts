import { describe, expect, test } from 'vitest'
import { complexCellItems, parseComplexType, PREVIEW_COUNT } from './complexCells'

describe('parseComplexType', () => {
  test('Array(String) → scalar array', () => {
    expect(parseComplexType('Array(String)')).toEqual({ kind: 'array', element: null })
  })

  test('Map(String, UInt64) → map', () => {
    expect(parseComplexType('Map(String, UInt64)')).toEqual({ kind: 'map' })
  })

  test('Tuple(...) → tuple, named or not', () => {
    expect(parseComplexType('Tuple(id Int32, name String)')).toEqual({ kind: 'tuple' })
    expect(parseComplexType('Tuple(Int32, String)')).toEqual({ kind: 'tuple' })
  })

  test('Array(Tuple(...)) and Array(Map(...)) record the element kind', () => {
    expect(parseComplexType('Array(Tuple(id Int32, name String))')).toEqual({ kind: 'array', element: 'tuple' })
    expect(parseComplexType('Array(Map(String, UInt64))')).toEqual({ kind: 'array', element: 'map' })
  })

  test('Array(Nullable(String)) is still a scalar array', () => {
    expect(parseComplexType('Array(Nullable(String))')).toEqual({ kind: 'array', element: null })
  })

  test('outer Nullable/LowCardinality wrappers are unwrapped', () => {
    expect(parseComplexType('Nullable(Array(String))')).toEqual({ kind: 'array', element: null })
    expect(parseComplexType('LowCardinality(Map(String, String))')).toEqual({ kind: 'map' })
  })

  test('tolerates pretty-printed types with newlines and indentation', () => {
    expect(parseComplexType('Array(\n    Tuple(\n        id Int32,\n        name String))')).toEqual({
      kind: 'array',
      element: 'tuple',
    })
  })

  test('a non-complex type → null', () => {
    expect(parseComplexType('String')).toBeNull()
    expect(parseComplexType('')).toBeNull()
  })
})

describe('complexCellItems', () => {
  test('scalar array → one item per element, as text', () => {
    expect(complexCellItems({ kind: 'array', element: null }, ['a', 1, null])).toEqual([
      { lines: ['a'] },
      { lines: ['1'] },
      { lines: [''] },
    ])
  })

  test('map object → one "key → value" line per entry', () => {
    expect(complexCellItems({ kind: 'map' }, { x: 1, y: 2 })).toEqual([{ lines: ['x → 1'] }, { lines: ['y → 2'] }])
  })

  test('named tuple object → one "name: value" line per field', () => {
    expect(complexCellItems({ kind: 'tuple' }, { id: 1, name: 'a' })).toEqual([
      { lines: ['id: 1'] },
      { lines: ['name: a'] },
    ])
  })

  test('unnamed tuple array uses positional indices', () => {
    expect(complexCellItems({ kind: 'tuple' }, [1, 'a'])).toEqual([{ lines: ['0: 1'] }, { lines: ['1: a'] }])
  })

  test('Array(Tuple) → one item per element, each tuple on a single line', () => {
    expect(
      complexCellItems({ kind: 'array', element: 'tuple' }, [
        { id: 1, name: 'a' },
        { id: 2, name: 'b' },
      ]),
    ).toEqual([{ lines: ['id: 1, name: a'] }, { lines: ['id: 2, name: b'] }])
  })

  test('Array(Map) → one item per element, each map on a single line', () => {
    expect(complexCellItems({ kind: 'array', element: 'map' }, [{ x: 1 }, { y: 2, z: 3 }])).toEqual([
      { lines: ['x → 1'] },
      { lines: ['y → 2, z → 3'] },
    ])
  })

  test('without a known type, containers are inferred from the value', () => {
    expect(complexCellItems(null, ['a', 'b'])).toEqual([{ lines: ['a'] }, { lines: ['b'] }])
    expect(complexCellItems(null, { k: 'v' })).toEqual([{ lines: ['k: v'] }])
  })

  test('deeper nesting renders as JSON text inside a line', () => {
    expect(complexCellItems({ kind: 'array', element: null }, [['a', 'b']])).toEqual([{ lines: ['["a","b"]'] }])
  })

  test('empty collection or scalar → no items', () => {
    expect(complexCellItems({ kind: 'array', element: null }, [])).toEqual([])
    expect(complexCellItems({ kind: 'map' }, {})).toEqual([])
    expect(complexCellItems({ kind: 'array', element: null }, 'plain')).toEqual([])
  })

  test('PREVIEW_COUNT is 3', () => {
    expect(PREVIEW_COUNT).toBe(3)
  })
})
