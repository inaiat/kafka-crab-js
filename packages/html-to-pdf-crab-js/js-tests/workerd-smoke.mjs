import { equal, notStrictEqual, ok, strictEqual } from 'node:assert/strict'
import { readFile } from 'node:fs/promises'

import { createInstance, dispose, instantiate } from '../html-to-pdf-crab-js.wasip1-deferred.js'

const wasmBytes = await readFile(new URL('../html-to-pdf-crab-js.wasm32-wasip1.wasm', import.meta.url))
const wasmModule = await WebAssembly.compile(wasmBytes)

const [first, second] = await Promise.all([instantiate(wasmModule), instantiate(wasmModule)])
strictEqual(first, second)

const pdf = await first.createPdfFromHtml({
  html: '<!doctype html><html><body><h1>Workerd smoke</h1><p>Threadless WASI.</p></body></html>',
  page: { margin: 12, size: 'A4' },
})
equal(Buffer.from(pdf.subarray(0, 5)).toString('utf8'), '%PDF-')
ok(Buffer.from(pdf).toString('latin1').trimEnd().endsWith('%%EOF'))

await dispose()
const fresh = await instantiate(wasmModule)
notStrictEqual(fresh, first)
await dispose()

const independent = await createInstance(wasmModule)
const independentPdf = await independent.exports.createPdfFromHtml({
  html: '<html><body>Independent instance</body></html>',
})
equal(Buffer.from(independentPdf.subarray(0, 5)).toString('utf8'), '%PDF-')
await independent.dispose()
