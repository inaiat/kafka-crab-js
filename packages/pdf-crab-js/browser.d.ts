import type {
  CreatePdfInput as NodeCreatePdfInput,
  PdfElementInput as NodePdfElementInput,
  PdfImageBytes,
  PdfImageElement as NodePdfImageElement,
  PdfPageInput as NodePdfPageInput,
  PdfDocument as NodePdfDocument,
} from './dist/api.js'

export { createPdf, createPdfAsync } from './dist/api.js'
export type {
  PdfAnnotationInput,
  PdfDocumentOptions,
  PdfImageAlign,
  PdfImageBytes,
  PdfImageOptions,
  PdfImageValign,
  PdfLayout,
  PdfLineElement,
  PdfLinkOptions,
  PdfMargins,
  PdfMetadata,
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
} from './dist/api.js'

export type PdfImageSource = PdfImageBytes

export declare class PdfDocument extends NodePdfDocument {
  image(source: PdfImageBytes, options?: PdfImageOptions): this
}

export interface PdfImageElement extends Omit<NodePdfImageElement, 'source'> {
  source: PdfImageSource
}
export type PdfElementInput = Exclude<NodePdfElementInput, NodePdfImageElement> | PdfImageElement
export interface PdfPageInput extends Omit<NodePdfPageInput, 'elements'> {
  elements?: PdfElementInput[]
}
export interface CreatePdfInput extends Omit<NodeCreatePdfInput, 'pages'> {
  pages: [PdfPageInput, ...PdfPageInput[]]
}
