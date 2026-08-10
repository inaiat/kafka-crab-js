import { equal, ok } from 'node:assert/strict'
import { readFile } from 'node:fs/promises'

import { configurePdfRuntime, PdfDocument, renderPdf } from '../dist/api.js'

const binding = await import(
  process.env.PDF_CRAB_WASM_THREADED === '1' ? '../pdf-crab-js.wasi.cjs' : '../pdf-crab-js.wasip1.cjs'
)

const imageBytes = Buffer.from(
  'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII=',
  'base64',
)
const fontBytes = await readFile(new URL('../../../examples/html-to-pdf-crab-js/assets/Tuffy.ttf', import.meta.url))

configurePdfRuntime({
  binding,
  resolveImageSource(source) {
    if (typeof source === 'string') throw new TypeError('paths are not supported in WASM smoke')
    if (source instanceof ArrayBuffer) return new Uint8Array(source)
    return new Uint8Array(source.buffer, source.byteOffset, source.byteLength)
  },
})

const output = renderPdf({
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
const pdf = await output.bytes()

ok(pdf instanceof Uint8Array)
equal(Buffer.from(pdf.subarray(0, 5)).toString('utf8'), '%PDF-')
equal(Buffer.from(pdf).toString('utf8').trimEnd().endsWith('%%EOF'), true)
ok(pdf.byteLength > 800)
ok(Buffer.from(pdf).toString('latin1').includes('/Filter /FlateDecode'))
ok(Buffer.from(pdf).toString('latin1').includes('/Subtype /Image'))

const wasmOutput = renderPdf({
  pages: [{ size: [120, 120], elements: [{ type: 'text', text: 'WASM stream', x: 12, y: 12 }] }],
})
let streamedBytes = 0
let streamedChunks = 0
for await (const chunk of wasmOutput) {
  ok(chunk instanceof Uint8Array)
  streamedBytes += chunk.byteLength
  streamedChunks += 1
}
ok(streamedChunks > 1)
ok(streamedBytes > 800)

const document = new PdfDocument({ unit: 'pt', size: [120, 120], margin: 12 })
document.text('WASM facade')
const fluent = await document.render().bytes()
ok(fluent instanceof Uint8Array)
ok(Buffer.from(fluent).toString('latin1').includes('/Filter /FlateDecode'))

const unicode = new PdfDocument({ unit: 'pt', size: [180, 180], margin: 12 })
unicode.registerFont('WasmTuffy', fontBytes, { fallback: 'Helvetica' }).font('WasmTuffy')
const unicodePdf = await unicode.text('Ação e café\u00a0final', { width: 150 }).render().bytes()
const unicodeBody = Buffer.from(unicodePdf).toString('latin1')
ok(unicodeBody.includes('/Subtype /Type0'))
ok(unicodeBody.includes('/ToUnicode'))
ok(unicodeBody.includes('/FontFile2'))
