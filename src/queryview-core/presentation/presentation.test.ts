import { describe, it, expect } from 'vitest'
import { presentationForSave } from './presentation'

describe('presentationForSave', () => {
  it('passes through non-empty picks', () => {
    expect(presentationForSave([{ name: 'id', dir: 'DESC' }], ['id', 'name'])).toEqual({
      order_by: [{ name: 'id', dir: 'DESC' }],
      fields: ['id', 'name'],
    })
  })
  it('maps empty arrays to null', () => {
    expect(presentationForSave([], [])).toEqual({ order_by: null, fields: null })
  })
})
