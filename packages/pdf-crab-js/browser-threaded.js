import { Buffer as BrowserBuffer } from 'buffer'

import { configurePdfRuntime } from './dist/api.js'

globalThis.Buffer ??= BrowserBuffer
const binding = await import('./pdf-crab-js.wasi-browser.js')

configurePdfRuntime({
  binding,
  resolveImageSource(source) {
    if (typeof source === 'string') {
      throw new TypeError('image file paths are only supported in the Node.js entrypoint; pass Uint8Array or ArrayBuffer')
    }
    if (source instanceof ArrayBuffer) return new Uint8Array(source)
    return new Uint8Array(source.buffer, source.byteOffset, source.byteLength)
  },
})

export { PdfDocument, PdfError, renderPdf } from './dist/api.js'
