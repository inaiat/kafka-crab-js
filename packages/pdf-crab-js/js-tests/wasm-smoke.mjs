import { equal, ok } from 'node:assert/strict'

import * as binding from '../pdf-crab-js.wasi.cjs'
import { configurePdfRuntime, createPdfAsync, PdfDocument } from '../dist/api.js'

const imageBytes = Buffer.from(
  'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII=',
  'base64',
)

configurePdfRuntime({
  binding,
  resolveImageSource(source) {
    if (typeof source === 'string') throw new TypeError('paths are not supported in WASM smoke')
    if (source instanceof ArrayBuffer) return new Uint8Array(source)
    return new Uint8Array(source.buffer, source.byteOffset, source.byteLength)
  },
})

const pdf = await createPdfAsync({
  title: 'WASM PDF smoke',
  pages: [
    {
      size: [120, 120],
      elements: [
        { type: 'text', text: 'WASM PDF smoke', x: 12, y: 12 },
        { type: 'image', source: imageBytes, x: 12, y: 40, width: 24 },
      ],
    },
  ],
})

ok(pdf instanceof Uint8Array)
equal(Buffer.from(pdf.subarray(0, 5)).toString('utf8'), '%PDF-')
equal(Buffer.from(pdf).toString('utf8').trimEnd().endsWith('%%EOF'), true)
ok(pdf.byteLength > 800)
ok(Buffer.from(pdf).toString('latin1').includes('WASM PDF smoke'))
ok(Buffer.from(pdf).toString('latin1').includes('/Subtype /Image'))

const document = new PdfDocument({ unit: 'pt', size: [120, 120], margin: 12 })
document.text('WASM facade')
const fluent = document.finish()
ok(fluent instanceof Uint8Array)
ok(Buffer.from(fluent).toString('latin1').includes('WASM facade'))
