import { describe, expect, test } from 'vitest'
import { applyParams, parseQueryParams, type ParamDef } from './queryParams'

describe('parseQueryParams', () => {
  test('parses a params list of name + options', () => {
    const yaml = 'params:\n  - name: source\n    options: [a, b, c]\n'
    expect(parseQueryParams(yaml)).toEqual([
      { name: 'source', options: ['a', 'b', 'c'] },
    ])
  })

  test('coerces non-string options to strings', () => {
    const yaml = 'params:\n  - name: n\n    options: [1, 2, true]\n'
    expect(parseQueryParams(yaml)).toEqual([
      { name: 'n', options: ['1', '2', 'true'] },
    ])
  })

  test('ignores the cell-view column keys alongside params', () => {
    const yaml =
      'params:\n  - name: source\n    options: [a, b]\n' +
      'cve_id:\n  type: link\n  value: https://x/{cell}\n'
    expect(parseQueryParams(yaml)).toEqual([
      { name: 'source', options: ['a', 'b'] },
    ])
  })

  test('returns [] when there is no params key', () => {
    expect(parseQueryParams('cve_id:\n  type: link\n  value: x\n')).toEqual([])
  })

  test('returns [] for malformed YAML', () => {
    expect(parseQueryParams('params: [unclosed')).toEqual([])
  })

  test('returns [] for null/empty input', () => {
    expect(parseQueryParams(null)).toEqual([])
    expect(parseQueryParams('')).toEqual([])
  })

  test('drops entries missing a name or with no options', () => {
    const yaml =
      'params:\n' +
      '  - options: [a]\n' + // no name
      '  - name: empty\n    options: []\n' + // no options
      '  - name: ok\n    options: [x]\n'
    expect(parseQueryParams(yaml)).toEqual([{ name: 'ok', options: ['x'] }])
  })

  test('returns [] when params is not a list', () => {
    expect(parseQueryParams('params: hello\n')).toEqual([])
  })
})

describe('applyParams', () => {
  const defs: ParamDef[] = [{ name: 'source', options: ['a', 'b', 'c'] }]

  test('replaces {name} with the selected value, quoted', () => {
    const out = applyParams('select * from t where source = {source}', defs, {
      source: 'b',
    })
    expect(out).toBe("select * from t where source = 'b'")
  })

  test('replaces every occurrence of the placeholder', () => {
    const out = applyParams('{source} = {source}', defs, { source: 'a' })
    expect(out).toBe("'a' = 'a'")
  })

  test('escapes single quotes in the value', () => {
    const out = applyParams('x = {source}', defs, { source: "O'Brien" })
    expect(out).toBe("x = 'O''Brien'")
  })

  test('falls back to the first option when no value is selected', () => {
    const out = applyParams('x = {source}', defs, {})
    expect(out).toBe("x = 'a'")
  })

  test('leaves the SQL unchanged when the placeholder is absent', () => {
    const out = applyParams('select 1', defs, { source: 'b' })
    expect(out).toBe('select 1')
  })

  test('substitutes multiple distinct params', () => {
    const multi: ParamDef[] = [
      { name: 'a', options: ['1'] },
      { name: 'b', options: ['2'] },
    ]
    const out = applyParams('{a} and {b}', multi, { a: '1', b: '2' })
    expect(out).toBe("'1' and '2'")
  })
})
