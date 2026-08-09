import type {
  PdfDocument as NodePdfDocument,
  PdfDocumentInput as NodePdfDocumentInput,
  PdfImageBytes,
  PdfImageElement as NodePdfImageElement,
  PdfImageOptions,
  PdfFontRegistrationOptions,
  PdfFontInput as NodePdfFontInput,
  PdfElementInput as NodePdfElementInput,
  PdfPageInput as NodePdfPageInput,
} from './dist/api.js'

export { PdfError, renderPdf } from './dist/api.js'
export type {
  PdfAnnotationInput,
  PdfDocumentOptions,
  PdfFillStyleOptions,
  PdfFontRegistrationOptions,
  PdfImageAlign,
  PdfImageBytes,
  PdfImageOptions,
  PdfImageValign,
  PdfLayout,
  PdfLineElement,
  PdfLinkOptions,
  PdfMargins,
  PdfMetadata,
  PdfOutput,
  PdfOutputOptions,
  PdfPageOptions,
  PdfPageSize,
  PdfPolylineElement,
  PdfPolygonElement,
  PdfRectElement,
  PdfStreamOptions,
  PdfStrokeStyleOptions,
  PdfTableCellStyle,
  PdfTableCellValue,
  PdfTableColumn,
  PdfTableColumnWidth,
  PdfTableOptions,
  PdfTextAlign,
  PdfTextBoxOptions,
  PdfTextElement,
  PdfTextOptions,
  PdfTextOverflow,
  PdfTextStyleOptions,
  PdfUnit,
} from './dist/api.js'

export type PdfImageSource = PdfImageBytes
export type PdfFontSource = PdfImageBytes

export interface PdfFontInput extends Omit<NodePdfFontInput, 'source'> {
  source: PdfFontSource
}

export declare class PdfDocument extends NodePdfDocument {
  image(source: PdfImageBytes, options?: PdfImageOptions): this
  registerFont(family: string, source: PdfImageBytes, options?: PdfFontRegistrationOptions): this
}

export interface PdfImageElement extends Omit<NodePdfImageElement, 'source'> {
  source: PdfImageSource
}

export type PdfElementInput = Exclude<NodePdfElementInput, NodePdfImageElement> | PdfImageElement
export interface PdfPageInput extends Omit<NodePdfPageInput, 'elements'> {
  elements?: readonly PdfElementInput[]
}
export interface PdfDocumentInput extends Omit<NodePdfDocumentInput, 'fonts' | 'pages'> {
  fonts?: readonly PdfFontInput[]
  pages: readonly PdfPageInput[]
}
