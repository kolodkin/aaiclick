import { describe, expect, test } from 'vitest'
import { buildSrcDoc } from './srcDoc'

describe('buildSrcDoc', () => {
  test('exposes results as window.queries before the HTML', () => {
    const doc = buildSrcDoc('<h1>hi</h1>', { q: { n: [1, 2] } })
    expect(doc).toBe('<script>window.queries = {"q":{"n":[1,2]}};</script>\n<h1>hi</h1>')
  })

  test('escapes < in result data so it cannot close the prologue script', () => {
    const doc = buildSrcDoc('', { q: { s: ['</script><img onerror=x>'] } })
    expect(doc).not.toContain('</script><img')
    expect(doc).toContain('\\u003c/script>\\u003cimg')
  })
})
