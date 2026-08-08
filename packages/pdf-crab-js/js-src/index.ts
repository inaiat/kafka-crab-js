import { readFileSync } from 'node:fs'

import * as binding from '../js-binding.js'
import { configurePdfRuntime, createPdf, createPdfAsync, PdfDocument } from './api.js'

configurePdfRuntime({
  binding: binding as never,
  resolveImageSource(source) {
    if (typeof source !== 'string') {
      if (source instanceof ArrayBuffer) return new Uint8Array(source)
      return new Uint8Array(source.buffer, source.byteOffset, source.byteLength)
    }
    return readFileSync(source)
  },
})

export { PdfDocument, createPdf, createPdfAsync }
export type {
  CreatePdfInput,
  PdfAnnotationInput,
  PdfDocumentOptions,
  PdfElementInput,
  PdfImageAlign,
  PdfImageBytes,
  PdfImageElement,
  PdfImageOptions,
  PdfImageSource,
  PdfImageValign,
  PdfLayout,
  PdfLineElement,
  PdfLinkOptions,
  PdfMargins,
  PdfMetadata,
  PdfPageInput,
  PdfPageOptions,
  PdfPageSize,
  PdfPathElement,
  PdfPolygonElement,
  PdfRectElement,
  PdfStyleOptions,
  PdfTextAlign,
  PdfTextBoxElement,
  PdfTextBoxOptions,
  PdfTextElement,
  PdfTextOptions,
  PdfUnit,
} from './api.js'
