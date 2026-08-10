import { equal, notStrictEqual, ok, strictEqual } from 'node:assert/strict'
import { readFile } from 'node:fs/promises'

import { createInstance, dispose, instantiate } from '../pdf-crab-js.wasip1-deferred.js'

const wasmBytes = await readFile(new URL('../pdf-crab-js.wasm32-wasip1.wasm', import.meta.url))
const wasmModule = await WebAssembly.compile(wasmBytes)

const [first, second] = await Promise.all([instantiate(wasmModule), instantiate(wasmModule)])
strictEqual(first, second)

const pdf = first.createPdf({
  pages: [{ elements: [{ type: 'text', text: 'Workerd smoke', x: 12, y: 12 }], height: 120, width: 120 }],
})
ok(Buffer.from(pdf.subarray(0, 5)).equals(Buffer.from('%PDF-')))
ok(Buffer.from(pdf).toString('latin1').trimEnd().endsWith('%%EOF'))

await dispose()
const fresh = await instantiate(wasmModule)
notStrictEqual(fresh, first)
await dispose()

const independent = await createInstance(wasmModule)
const independentPdf = independent.exports.createPdf({ pages: [{ height: 120, width: 120 }] })
equal(Buffer.from(independentPdf.subarray(0, 5)).toString('utf8'), '%PDF-')
await independent.dispose()
