import { readFileSync } from 'node:fs'

import * as binding from '../js-binding.js'
import { configurePdfRuntime } from './api.js'

configurePdfRuntime({
  binding: binding as never,
  resolveImageSource(source) {
    if (typeof source !== 'string') {
      if (source instanceof ArrayBuffer) return new Uint8Array(source)
      return new Uint8Array(source.buffer, source.byteOffset, source.byteLength)
    }
    return readFileSync(source.startsWith('file:') ? new URL(source) : source)
  },
})

export { PdfDocument, PdfError, renderPdf } from './api.js'
export type {
  PdfDocumentInput,
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
  PdfPolylineElement,
  PdfPolygonElement,
  PdfRectElement,
  PdfStrokeStyleOptions,
  PdfFillStyleOptions,
  PdfFontRegistrationOptions,
  PdfFontInput,
  PdfFontSource,
  PdfTextStyleOptions,
  PdfTableCellStyle,
  PdfTableCellValue,
  PdfTableColumn,
  PdfTableColumnWidth,
  PdfTableOptions,
  PdfOutput,
  PdfOutputOptions,
  PdfErrorCode,
  PdfStreamOptions,
  PdfTextAlign,
  PdfTextBoxOptions,
  PdfTextElement,
  PdfTextOptions,
  PdfTextOverflow,
  PdfUnit,
} from './api.js'
