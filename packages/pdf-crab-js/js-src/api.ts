/* eslint-disable complexity, max-statements, no-continue, no-shadow, no-use-before-define, prefer-destructuring */
/* eslint-disable no-unused-expressions, typescript/no-floating-promises, typescript/no-this-alias */
/* eslint-disable typescript/parameter-properties, typescript/method-signature-style */
/* eslint-disable typescript/use-unknown-in-catch-callback-variable, unicorn/prefer-spread */

export type PdfUnit = 'mm' | 'pt'
export type PdfPageSize = 'A3' | 'A4' | 'LETTER' | readonly [number, number]
export type PdfLayout = 'portrait' | 'landscape'
export type PdfImageBytes = Uint8Array | ArrayBuffer
export type PdfImageSource = PdfImageBytes | string
export type PdfFontSource = PdfImageBytes | string | URL
export type PdfImageAlign = 'left' | 'center' | 'right'
export type PdfImageValign = 'top' | 'center' | 'bottom'
export type PdfTextAlign = 'left' | 'center' | 'right' | 'justify'
export type PdfTextOverflow = 'visible' | 'clip' | 'ellipsis' | 'paginate'
export type PdfErrorCode =
  | 'PDF_OUTPUT_USED'
  | 'PDF_DOCUMENT_FINISHED'
  | 'PDF_INVALID_ARGUMENT'
  | 'PDF_UNSUPPORTED_FORMAT'
  | 'PDF_FONT_NOT_FOUND'
  | 'PDF_MISSING_GLYPH'
  | 'PDF_LAYOUT_ERROR'
  | 'PDF_ABORTED'

export class PdfError extends Error {
  public readonly code: PdfErrorCode
  public readonly path?: string
  public readonly cause?: unknown

  public constructor(code: PdfErrorCode, message: string, options: { path?: string; cause?: unknown } = {}) {
    super(message)
    this.name = 'PdfError'
    this.code = code
    this.path = options.path
    this.cause = options.cause
  }
}

export interface PdfMargins {
  top: number
  right: number
  bottom: number
  left: number
}

export interface PdfMetadata {
  title?: string
  author?: string
  creator?: string
  producer?: string
  subject?: string
  keywords?: string[]
  trapped?: boolean
}

export interface PdfDocumentOptions {
  title?: string
  unit?: PdfUnit
  size?: PdfPageSize
  layout?: PdfLayout
  margin?: number | Partial<PdfMargins>
  metadata?: PdfMetadata
}

export interface PdfPageOptions {
  size?: PdfPageSize
  layout?: PdfLayout
  margin?: number | Partial<PdfMargins>
}

export interface PdfTextStyleOptions {
  font?: string
  fontSize?: number
  fill?: string
}

export interface PdfFontRegistrationOptions {
  weight?: number | 'normal' | 'bold'
  style?: 'normal' | 'italic' | 'oblique'
  fallback?: string
}

export interface PdfFontInput extends PdfFontRegistrationOptions {
  family: string
  source: PdfFontSource
}

export interface PdfStrokeStyleOptions {
  stroke?: string
  strokeWidth?: number
}

export interface PdfFillStyleOptions {
  fill?: string
  stroke?: string
  strokeWidth?: number
}

interface PdfTextOptionsBase extends PdfTextStyleOptions {
  lineHeight?: number
  hyphenate?: boolean
  overflow?: PdfTextOverflow
}

type PdfTextBounds =
  | { width?: never; height?: never; align?: never }
  | { width: number; height?: number; align?: PdfTextAlign }

export type PdfTextOptions = PdfTextOptionsBase & PdfTextBounds & ({ x?: never; y?: never } | { x: number; y: number })

export type PdfTextBoxOptions = PdfTextOptions

export interface PdfImageOptions {
  x?: number
  y?: number
  width?: number
  height?: number
  fit?: readonly [number, number]
  align?: PdfImageAlign
  valign?: PdfImageValign
}

export interface PdfLinkOptions {
  x: number
  y: number
  width: number
  height: number
  color?: string
}

export type PdfTableCellValue = string | number | boolean | null | undefined
export type PdfTableColumnWidth = number | 'auto' | '*'

export interface PdfTableCellStyle extends PdfTextStyleOptions, PdfStrokeStyleOptions {
  background?: string
  padding?: number
  align?: PdfTextAlign
}

export interface PdfTableColumn<
  Row extends Record<string, unknown> = Record<string, unknown>,
> extends PdfTableCellStyle {
  key?: keyof Row & string
  header?: PdfTableCellValue
  value?: (row: Row, index: number) => PdfTableCellValue
  formatter?: (value: PdfTableCellValue, row: Row, index: number) => PdfTableCellValue
  width?: PdfTableColumnWidth
  minWidth?: number
  maxWidth?: number
}

export interface PdfTableOptions<Row extends Record<string, unknown> = Record<string, unknown>> {
  columns: readonly PdfTableColumn<Row>[]
  rows: readonly Row[]
  x?: number
  y?: number
  width?: number
  rowHeight?: number
  headerHeight?: number
  repeatHeader?: boolean
  rowSplit?: 'avoid' | 'split'
  stripe?: string
  border?: string
  padding?: number
}

interface PdfTextElementBase extends PdfTextStyleOptions {
  type: 'text'
  text: string
  lineHeight?: number
  hyphenate?: boolean
  overflow?: PdfTextOverflow
}

export type PdfTextElement = PdfTextElementBase & PdfTextBounds & ({ x?: never; y?: never } | { x: number; y: number })

export interface PdfLineElement extends PdfStrokeStyleOptions {
  type: 'line'
  x1: number
  y1: number
  x2: number
  y2: number
}

export interface PdfRectElement extends PdfFillStyleOptions {
  type: 'rect'
  x: number
  y: number
  width: number
  height: number
}

export interface PdfPolygonElement extends PdfFillStyleOptions {
  type: 'polygon'
  points: readonly { x: number; y: number }[]
  winding?: 'nonZero' | 'evenOdd'
}

export interface PdfPolylineElement extends PdfFillStyleOptions {
  type: 'polyline'
  points: readonly { x: number; y: number }[]
  closed?: boolean
  winding?: 'nonZero' | 'evenOdd'
}

export interface PdfImageElement {
  type: 'image'
  source: PdfImageSource
  x: number
  y: number
  width?: number
  height?: number
  fit?: readonly [number, number]
  align?: PdfImageAlign
  valign?: PdfImageValign
}

export type PdfElementInput =
  | PdfTextElement
  | PdfLineElement
  | PdfRectElement
  | PdfPolygonElement
  | PdfPolylineElement
  | PdfImageElement

export interface PdfAnnotationInput {
  type: 'link'
  x: number
  y: number
  width: number
  height: number
  url: string
  color?: string
}

export interface PdfPageInput {
  size?: PdfPageSize
  layout?: PdfLayout
  elements?: readonly PdfElementInput[]
  annotations?: readonly PdfAnnotationInput[]
}

export interface PdfDocumentInput {
  title?: string
  unit?: PdfUnit
  metadata?: PdfMetadata
  fonts?: readonly PdfFontInput[]
  pages: readonly PdfPageInput[]
}

export interface PdfStreamOptions {
  chunkSize?: number
  signal?: AbortSignal
}

export interface PdfOutputOptions {
  signal?: AbortSignal
}

export interface PdfOutput {
  readonly used: boolean
  bytes: (options?: PdfOutputOptions) => Promise<Uint8Array>
  blob: (options?: PdfOutputOptions) => Promise<Blob>
  stream: (options?: PdfStreamOptions) => ReadableStream<Uint8Array>
  [Symbol.asyncIterator]: () => AsyncIterator<Uint8Array>
}

interface NativePdfOutput {
  nextChunk: (chunkSize?: number) => Uint8Array | null
  cancel: () => void
}

interface NativeBuilder {
  startPage: (page: { width: number; height: number }) => void
  registerFont: (input: {
    family: string
    data: Uint8Array
    fallback?: string
    weight?: number
    style?: 'normal' | 'italic' | 'oblique'
  }) => void
  layoutText: (
    text: string,
    font: string,
    fontSize: number,
    width: number,
    hyphenate?: boolean,
  ) => { text: string; paragraphEnd: boolean }[]
  measureTexts: (texts: string[], font: string, fontSize: number) => number[]
  appendElements: (elements: Record<string, unknown>[]) => void
  appendAnnotations: (annotations: Record<string, unknown>[]) => void
  addPage: (page: Record<string, unknown>) => void
  endPage: () => void
  finish: () => Uint8Array
  finishAsync: () => Promise<Uint8Array>
  finishStream: (startAfterHeader?: boolean) => NativePdfOutput
}

interface NativeBinding {
  createPdf: (input: Record<string, unknown>) => Uint8Array
  createPdfAsync: (input: Record<string, unknown>) => Promise<Uint8Array>
  createPdfStream: (input: Record<string, unknown>, startAfterHeader?: boolean) => NativePdfOutput
  getImageDimensions: (data: Uint8Array) => { width: number; height: number }
  PdfDocumentBuilder: new (input?: Record<string, unknown>) => NativeBuilder
}

interface PdfRuntime {
  binding: NativeBinding
  resolveImageSource: (source: PdfImageSource) => Uint8Array
}

let runtime: PdfRuntime | undefined

export function configurePdfRuntime(nextRuntime: PdfRuntime): void {
  runtime = nextRuntime
}

function getRuntime(): PdfRuntime {
  if (!runtime) {
    throw new PdfError('PDF_INVALID_ARGUMENT', 'pdf-crab-js runtime has not been initialized')
  }
  return runtime
}

type PdfOutputProducer = (signal?: AbortSignal) => Promise<NativePdfOutput>

const PDF_HEADER = new Uint8Array([37, 80, 68, 70, 45, 49, 46, 55, 10, 37, 128, 128, 128, 128, 10, 10])

class PdfOutputImpl implements PdfOutput {
  private consumed = false
  private producer: PdfOutputProducer | undefined

  public constructor(producer: PdfOutputProducer) {
    this.producer = producer
  }

  public get used(): boolean {
    return this.consumed
  }

  public async bytes(options: PdfOutputOptions = {}): Promise<Uint8Array> {
    const source = await this.consume(options.signal)
    const body = await drainNativeOutput(source, options.signal)
    return joinBytes(PDF_HEADER, body)
  }

  public async blob(options: PdfOutputOptions = {}): Promise<Blob> {
    const source = await this.consume(options.signal)
    const bytes = joinBytes(PDF_HEADER, await drainNativeOutput(source, options.signal))
    return new Blob([bytes.slice().buffer as ArrayBuffer], { type: 'application/pdf' })
  }

  public stream(options: PdfStreamOptions = {}): ReadableStream<Uint8Array> {
    const chunkSize = normalizeStreamChunkSize(options.chunkSize)
    const { signal } = options
    let producer: PdfOutputProducer | undefined
    let sourcePromise: Promise<NativePdfOutput> | undefined
    let source: NativePdfOutput | undefined
    let headerOffset = 0
    let cancelled = false
    const thisOutput = this

    return new ReadableStream<Uint8Array>(
      {
        async pull(controller) {
          try {
            if (cancelled) return
            if (signal?.aborted) throw abortedError()
            producer ??= thisOutput.claim(signal)
            if (headerOffset < PDF_HEADER.byteLength) {
              const end = Math.min(headerOffset + chunkSize, PDF_HEADER.byteLength)
              controller.enqueue(PDF_HEADER.slice(headerOffset, end))
              headerOffset = end
              return
            }
            sourcePromise ??= producer(signal)
            source ??= await sourcePromise
            if (signal?.aborted) throw abortedError()
            const chunk = source.nextChunk(chunkSize)
            if (chunk === null) {
              controller.close()
            } else {
              controller.enqueue(Uint8Array.from(chunk))
            }
          } catch (error) {
            if (source) {
              source.cancel()
            } else {
              sourcePromise?.then((value) => value.cancel())
            }
            controller.error(normalizePdfError(error))
          }
        },
        cancel() {
          cancelled = true
          if (source) {
            source.cancel()
          } else {
            sourcePromise?.then((value) => value.cancel())
          }
        },
      },
      { highWaterMark: 0 },
    )
  }

  public [Symbol.asyncIterator](): AsyncIterator<Uint8Array> {
    const chunkSize = DEFAULT_STREAM_CHUNK_SIZE
    const signal = undefined
    let producer: PdfOutputProducer | undefined
    let sourcePromise: Promise<NativePdfOutput> | undefined
    let source: NativePdfOutput | undefined
    let headerEmitted = false
    const thisOutput = this

    return {
      async next() {
        producer ??= thisOutput.claim(signal)
        if (!headerEmitted) {
          headerEmitted = true
          return { done: false, value: PDF_HEADER.slice() }
        }
        sourcePromise ??= producer(signal)
        source ??= await sourcePromise
        const value = source.nextChunk(chunkSize)
        return value === null ? { done: true, value: undefined } : { done: false, value: Uint8Array.from(value) }
      },
      async return() {
        source?.cancel()
        return { done: true, value: undefined }
      },
    }
  }

  private consume(signal?: AbortSignal): Promise<NativePdfOutput> {
    try {
      return this.claim(signal)(signal)
    } catch (error) {
      return Promise.reject(error)
    }
  }

  private claim(signal?: AbortSignal): PdfOutputProducer {
    if (this.consumed) {
      throw new PdfError('PDF_OUTPUT_USED', 'PdfOutput has already been consumed')
    }
    this.consumed = true
    if (signal?.aborted) throw abortedError()
    const producer = this.producer
    this.producer = undefined
    if (!producer) throw new PdfError('PDF_OUTPUT_USED', 'PdfOutput has already been consumed')
    return (producerSignal) => producer(producerSignal).catch((error) => Promise.reject(normalizePdfError(error)))
  }
}

async function drainNativeOutput(source: NativePdfOutput, signal?: AbortSignal): Promise<Uint8Array> {
  const chunks: Uint8Array[] = []
  let total = 0
  for (;;) {
    if (signal?.aborted) {
      source.cancel()
      throw abortedError()
    }
    const chunk = source.nextChunk(DEFAULT_STREAM_CHUNK_SIZE)
    if (chunk === null) break
    const owned = Uint8Array.from(chunk)
    chunks.push(owned)
    total += owned.byteLength
  }
  const bytes = new Uint8Array(total)
  let offset = 0
  for (const chunk of chunks) {
    bytes.set(chunk, offset)
    offset += chunk.byteLength
  }
  return bytes
}

function joinBytes(left: Uint8Array, right: Uint8Array): Uint8Array {
  const bytes = new Uint8Array(left.byteLength + right.byteLength)
  bytes.set(left)
  bytes.set(right, left.byteLength)
  return bytes
}

function abortedError(): PdfError {
  return new PdfError('PDF_ABORTED', 'PDF rendering was aborted')
}

function normalizePdfError(error: unknown, path?: string): PdfError {
  if (error instanceof PdfError) {
    if (error.path !== undefined || path === undefined) return error
    return new PdfError(error.code, error.message, { path, cause: error.cause })
  }
  if (error instanceof Error) {
    const code: PdfErrorCode = /unsupported glyph|missing glyph/i.test(error.message)
      ? 'PDF_MISSING_GLYPH'
      : /built-in PDF fonts|font not found/i.test(error.message)
        ? 'PDF_FONT_NOT_FOUND'
        : /unsupported image|unsupported format|image format/i.test(error.message)
          ? 'PDF_UNSUPPORTED_FORMAT'
          : 'PDF_INVALID_ARGUMENT'
    return new PdfError(code, error.message, { path, cause: error })
  }
  return new PdfError('PDF_INVALID_ARGUMENT', String(error), { path, cause: error })
}

function callNative<Result>(operation: () => Result, path?: string): Result {
  try {
    return operation()
  } catch (error) {
    throw normalizePdfError(error, path)
  }
}

function callUserCallback<Result>(operation: () => Result, path: string): Result {
  try {
    return operation()
  } catch (error) {
    if (error instanceof PdfError) throw normalizePdfError(error, path)
    throw new PdfError('PDF_INVALID_ARGUMENT', error instanceof Error ? error.message : String(error), {
      path,
      cause: error,
    })
  }
}

function output(producer: PdfOutputProducer): PdfOutput {
  return new PdfOutputImpl(producer)
}

const PAGE_SIZES_MM: Record<Exclude<PdfPageSize, readonly [number, number]>, readonly [number, number]> = {
  A3: [297, 420],
  A4: [210, 297],
  LETTER: [215.9, 279.4],
}

const POINTS_PER_MM = 72 / 25.4

function assertFinitePositive(value: number, name: string): number {
  if (!Number.isFinite(value) || value <= 0) {
    throw new PdfError('PDF_INVALID_ARGUMENT', `${name} must be a finite number greater than 0`, { path: name })
  }
  return value
}

function assertFiniteNonNegative(value: number, name: string): number {
  if (!Number.isFinite(value) || value < 0) {
    throw new PdfError('PDF_INVALID_ARGUMENT', `${name} must be a finite number greater than or equal to 0`, {
      path: name,
    })
  }
  return value
}

function assertFiniteNumber(value: number, name: string): number {
  if (!Number.isFinite(value))
    throw new PdfError('PDF_INVALID_ARGUMENT', `${name} must be a finite number`, { path: name })
  return value
}

function normalizeUnit(unit: PdfUnit | undefined): PdfUnit {
  if (unit === undefined) return 'mm'
  if (unit !== 'mm' && unit !== 'pt')
    throw new PdfError('PDF_INVALID_ARGUMENT', 'unit must be "mm" or "pt"', { path: 'unit' })
  return unit
}

function normalizeLayout(layout: PdfLayout | undefined): PdfLayout {
  if (layout === undefined) return 'portrait'
  if (layout !== 'portrait' && layout !== 'landscape') {
    throw new PdfError('PDF_INVALID_ARGUMENT', 'layout must be "portrait" or "landscape"', { path: 'layout' })
  }
  return layout
}

function normalizeMargins(margin: PdfDocumentOptions['margin'] | undefined, unit: PdfUnit): PdfMargins {
  const defaultMargin = unit === 'mm' ? 20 : 20 * POINTS_PER_MM
  if (margin === undefined)
    return { top: defaultMargin, right: defaultMargin, bottom: defaultMargin, left: defaultMargin }
  if (typeof margin === 'number') {
    assertFiniteNonNegative(margin, 'margin')
    return { top: margin, right: margin, bottom: margin, left: margin }
  }
  if (!margin || typeof margin !== 'object') {
    throw new PdfError('PDF_INVALID_ARGUMENT', 'margin must be a number or margin object', { path: 'margin' })
  }
  return {
    top: assertFiniteNonNegative(margin.top ?? defaultMargin, 'margin.top'),
    right: assertFiniteNonNegative(margin.right ?? defaultMargin, 'margin.right'),
    bottom: assertFiniteNonNegative(margin.bottom ?? defaultMargin, 'margin.bottom'),
    left: assertFiniteNonNegative(margin.left ?? defaultMargin, 'margin.left'),
  }
}

function pageSizeInUnit(size: PdfPageSize | undefined, unit: PdfUnit): [number, number] {
  const resolved = size ?? 'A4'
  if (typeof resolved !== 'string') {
    if (!Array.isArray(resolved) || resolved.length !== 2) {
      throw new PdfError('PDF_INVALID_ARGUMENT', 'size must be A3, A4, LETTER, or [width, height]', { path: 'size' })
    }
    return [
      assertFinitePositive(resolved[0]!, 'page size width'),
      assertFinitePositive(resolved[1]!, 'page size height'),
    ]
  }
  const millimeters = PAGE_SIZES_MM[resolved as Exclude<PdfPageSize, readonly [number, number]>]
  if (!millimeters)
    throw new PdfError('PDF_INVALID_ARGUMENT', 'size must be A3, A4, LETTER, or [width, height]', { path: 'size' })
  return unit === 'mm'
    ? [millimeters[0], millimeters[1]]
    : [millimeters[0] * POINTS_PER_MM, millimeters[1] * POINTS_PER_MM]
}

function pageDimensions(options: PdfPageOptions | PdfDocumentOptions, unit: PdfUnit): [number, number] {
  const dimensions = pageSizeInUnit(options.size, unit)
  return normalizeLayout(options.layout) === 'landscape' ? [dimensions[1], dimensions[0]] : dimensions
}

function fromPoints(value: number, unit: PdfUnit): number {
  return unit === 'mm' ? (value * 25.4) / 72 : value
}

function normalizeBytes(source: PdfImageSource | PdfFontSource, path = 'image.source'): Uint8Array {
  if (typeof source === 'string' || source instanceof URL) {
    try {
      return getRuntime().resolveImageSource(source instanceof URL ? source.href : source)
    } catch (error) {
      throw normalizePdfError(error)
    }
  }
  if (source instanceof ArrayBuffer) return new Uint8Array(source)
  if (ArrayBuffer.isView(source)) {
    return new Uint8Array(source.buffer, source.byteOffset, source.byteLength)
  }
  throw new PdfError('PDF_INVALID_ARGUMENT', 'source must be Uint8Array, ArrayBuffer, a file path, or a file URL', {
    path,
  })
}

function registerFontWithBuilder(
  builder: NativeBuilder,
  registeredFonts: Set<string>,
  input: PdfFontInput,
  path = 'font',
): void {
  const { family, source, fallback, style } = input
  if (typeof family !== 'string' || !family.trim()) {
    throw new PdfError('PDF_INVALID_ARGUMENT', 'font family must be a non-empty string', { path: `${path}.family` })
  }
  if (registeredFonts.has(family) || isBuiltinFont(family)) {
    throw new PdfError('PDF_INVALID_ARGUMENT', `font family "${family}" has already been registered`, {
      path: `${path}.family`,
    })
  }
  const weight = input.weight === 'bold' ? 700 : input.weight === 'normal' ? 400 : input.weight
  if (weight !== undefined && (!Number.isInteger(weight) || weight < 1 || weight > 1000)) {
    throw new PdfError('PDF_INVALID_ARGUMENT', 'font weight must be normal, bold, or an integer from 1 to 1000', {
      path: `${path}.weight`,
    })
  }
  if (style !== undefined && !['normal', 'italic', 'oblique'].includes(style)) {
    throw new PdfError('PDF_INVALID_ARGUMENT', 'font style must be normal, italic, or oblique', {
      path: `${path}.style`,
    })
  }
  if (fallback !== undefined) {
    if (typeof fallback !== 'string' || !fallback.trim()) {
      throw new PdfError('PDF_INVALID_ARGUMENT', 'font fallback must be a non-empty string', {
        path: `${path}.fallback`,
      })
    }
    if (!isBuiltinFont(fallback) && !registeredFonts.has(fallback)) {
      throw new PdfError('PDF_FONT_NOT_FOUND', `fallback font "${fallback}" must be registered first`, {
        path: `${path}.fallback`,
      })
    }
  }
  const bytes = normalizeBytes(source, `${path}.source`)
  if (
    bytes.byteLength < 4 ||
    (!(bytes[0] === 0 && bytes[1] === 1 && bytes[2] === 0 && bytes[3] === 0) &&
      String.fromCharCode(bytes[0]!, bytes[1]!, bytes[2]!, bytes[3]!) !== 'OTTO')
  ) {
    throw new PdfError('PDF_UNSUPPORTED_FORMAT', 'font source must be a TTF or OTF file', {
      path: `${path}.source`,
    })
  }
  try {
    builder.registerFont({ family, data: bytes, fallback, weight, style })
  } catch (error) {
    const normalized = normalizePdfError(error)
    throw new PdfError(normalized.code, normalized.message, { path, cause: normalized.cause ?? error })
  }
  registeredFonts.add(family)
}

function normalizeElement(element: PdfElementInput, unit: PdfUnit): Record<string, unknown> {
  if (!element || typeof element !== 'object' || typeof element.type !== 'string') {
    throw new PdfError('PDF_INVALID_ARGUMENT', 'element.type must be a string', { path: 'element.type' })
  }
  if (element.type === 'polyline') return { ...element, type: 'path' }
  if (element.type === 'text') {
    if (typeof element.text !== 'string') {
      throw new PdfError('PDF_INVALID_ARGUMENT', 'text.text must be a string', { path: 'text.text' })
    }
    const hasX = element.x !== undefined
    const hasY = element.y !== undefined
    if (hasX !== hasY)
      throw new PdfError('PDF_INVALID_ARGUMENT', 'text.x and text.y must be provided together', { path: 'text' })
    if (element.align !== undefined && element.width === undefined) {
      throw new PdfError('PDF_INVALID_ARGUMENT', 'text.align requires text.width', { path: 'text.align' })
    }
    if (element.height !== undefined && element.width === undefined) {
      throw new PdfError('PDF_INVALID_ARGUMENT', 'text.height requires text.width', { path: 'text.height' })
    }
    if (element.width !== undefined) return { ...element, type: 'textBox' }
    return { ...element }
  }
  if (element.type !== 'image') return { ...element }
  const {
    source,
    fit,
    align,
    valign,
    width: elementWidth,
    height: elementHeight,
    x: elementX,
    y: elementY,
    ...rest
  } = element
  const bytes = normalizeBytes(source)
  if (fit !== undefined && (!Array.isArray(fit) || fit.length !== 2)) {
    throw new PdfError('PDF_INVALID_ARGUMENT', 'image.fit must contain width and height', { path: 'image.fit' })
  }
  const dimensions = callNative(() => getRuntime().binding.getImageDimensions(bytes), 'image.source')
  const naturalWidth = unit === 'mm' ? (dimensions.width * 25.4) / 72 : dimensions.width
  const naturalHeight = unit === 'mm' ? (dimensions.height * 25.4) / 72 : dimensions.height
  let width = elementWidth
  let height = elementHeight
  if (fit) {
    width = assertFinitePositive(fit[0]!, 'fit[0]')
    height = assertFinitePositive(fit[1]!, 'fit[1]')
  }
  if (width === undefined && height === undefined) {
    width = naturalWidth
    height = naturalHeight
  } else if (width === undefined) {
    height = assertFinitePositive(height!, 'height')
    width = (height * naturalWidth) / naturalHeight
  } else if (height === undefined) {
    width = assertFinitePositive(width, 'width')
    height = (width * naturalHeight) / naturalWidth
  } else {
    width = assertFinitePositive(width, 'width')
    height = assertFinitePositive(height, 'height')
  }
  let x = elementX
  let y = elementY
  if (fit) {
    const scale = Math.min(width / naturalWidth, height / naturalHeight)
    const drawWidth = naturalWidth * scale
    const drawHeight = naturalHeight * scale
    x += align === 'center' ? (width - drawWidth) / 2 : align === 'right' ? width - drawWidth : 0
    y += valign === 'center' ? (height - drawHeight) / 2 : valign === 'bottom' ? height - drawHeight : 0
    width = drawWidth
    height = drawHeight
  }
  return { ...rest, type: 'image', x, y, width, height, imageData: bytes }
}

function validatePageInput(page: PdfPageInput, unit: PdfUnit, path: string): void {
  if (!page || typeof page !== 'object') {
    throw new PdfError('PDF_INVALID_ARGUMENT', 'page must be an object', { path })
  }
  if (page.elements !== undefined && !Array.isArray(page.elements)) {
    throw new PdfError('PDF_INVALID_ARGUMENT', 'page.elements must be an array', { path: `${path}.elements` })
  }
  if (page.annotations !== undefined && !Array.isArray(page.annotations)) {
    throw new PdfError('PDF_INVALID_ARGUMENT', 'page.annotations must be an array', { path: `${path}.annotations` })
  }
  pageDimensions(page, unit)
}

function appendDeclarativePage(builder: NativeBuilder, page: PdfPageInput, unit: PdfUnit): void {
  const [width, height] = pageDimensions(page, unit)
  const margins = normalizeMargins(undefined, unit)
  const pageBottom = height - margins.bottom
  let elements: Record<string, unknown>[] = []
  let annotations: Record<string, unknown>[] = Array.from(page.annotations ?? [], (annotation) => ({ ...annotation }))
  let cursorY = margins.top

  const startPage = (): void => callNative(() => builder.startPage({ width, height }), 'page')
  const flushPage = (): void => {
    callNative(() => builder.appendElements(elements), 'page.elements')
    if (annotations.length > 0) callNative(() => builder.appendAnnotations(annotations), 'page.annotations')
    callNative(() => builder.endPage(), 'page')
    elements = []
    annotations = []
  }
  const nextPage = (): void => {
    flushPage()
    startPage()
    cursorY = margins.top
  }

  startPage()
  for (const element of page.elements ?? []) {
    if (element.type !== 'text' || (element.x !== undefined && element.y !== undefined)) {
      elements.push(normalizeElement(element, unit))
      continue
    }

    if (typeof element.text !== 'string') {
      throw new PdfError('PDF_INVALID_ARGUMENT', 'text.text must be a string', { path: 'text.text' })
    }
    const textWidth = element.width ?? width - margins.left - margins.right
    assertFinitePositive(textWidth, 'text.width')
    if (element.align !== undefined && element.width === undefined) {
      throw new PdfError('PDF_INVALID_ARGUMENT', 'text.align requires text.width', { path: 'text.align' })
    }
    const fontSize = assertFinitePositive(element.fontSize ?? 12, 'text.fontSize')
    const lineHeightPoints = assertFinitePositive(element.lineHeight ?? fontSize * 1.2, 'text.lineHeight')
    const lineHeight = fromPoints(lineHeightPoints, unit)
    const lines = callNative(
      () => builder.layoutText(element.text, element.font ?? 'Helvetica', fontSize, textWidth, element.hyphenate),
      'text',
    )
    const overflow = element.overflow ?? 'paginate'

    if (overflow === 'paginate') {
      let lineIndex = 0
      while (lineIndex < lines.length) {
        if (cursorY + lineHeight > pageBottom && elements.length > 0) nextPage()
        const availableLines = Math.max(1, Math.floor((pageBottom - cursorY) / lineHeight))
        const chunkLines = Math.min(availableLines, lines.length - lineIndex)
        const layoutLines = lines.slice(lineIndex, lineIndex + chunkLines)
        elements.push({
          ...element,
          type: 'textBox',
          text: layoutLines.map((line) => line.text).join('\n'),
          x: margins.left,
          y: cursorY,
          width: textWidth,
          height: chunkLines * lineHeight,
          font: element.font ?? 'Helvetica',
          fontSize,
          lineHeight: lineHeightPoints,
          overflow: 'clip',
          layoutLines,
        })
        cursorY += chunkLines * lineHeight
        lineIndex += chunkLines
        if (lineIndex < lines.length) nextPage()
      }
      continue
    }

    const blockHeight = element.height ?? lines.length * lineHeight
    if (cursorY + blockHeight > pageBottom && elements.length > 0) nextPage()
    elements.push({
      ...element,
      type: 'textBox',
      x: margins.left,
      y: cursorY,
      width: textWidth,
      font: element.font ?? 'Helvetica',
      fontSize,
      lineHeight: lineHeightPoints,
      layoutLines: lines,
    })
    cursorY += blockHeight
  }
  flushPage()
}

interface PageState {
  width: number
  height: number
  size: PdfPageSize
  layout: PdfLayout
  margin: PdfMargins
  elements: Record<string, unknown>[]
  annotations: Record<string, unknown>[]
}

interface PathPoint {
  x: number
  y: number
}

interface StyleState {
  font: string
  fontSize: number
  fill: string
  stroke: string
  strokeWidth: number
  lineHeight: number
}

const DEFAULT_STYLE: StyleState = {
  font: 'Helvetica',
  fontSize: 12,
  fill: '#000000',
  stroke: '#000000',
  strokeWidth: 1,
  lineHeight: 14.4,
}

const BUILTIN_FONT_NAMES = new Set([
  'times',
  'timesroman',
  'timesnewroman',
  'timesbold',
  'timesnewromanbold',
  'timesitalic',
  'timesnewromanitalic',
  'timesbolditalic',
  'timesitalicbold',
  'timesnewromanbolditalic',
  'helvetica',
  'arial',
  'helveticabold',
  'arialbold',
  'helveticaoblique',
  'helveticaitalic',
  'arialitalic',
  'helveticaboldoblique',
  'helveticabolditalic',
  'arialbolditalic',
  'courier',
  'couriernew',
  'courieroblique',
  'courieritalic',
  'couriernewitalic',
  'courierbold',
  'couriernewbold',
  'courierboldoblique',
  'courierbolditalic',
  'couriernewbolditalic',
  'symbol',
  'zapfdingbats',
])

function isBuiltinFont(font: string): boolean {
  return BUILTIN_FONT_NAMES.has(font.toLowerCase().replaceAll(/[\s_-]/g, ''))
}

const DEFAULT_STREAM_CHUNK_SIZE = 64 * 1024

function normalizeStreamChunkSize(chunkSize: number | undefined): number {
  const value = chunkSize ?? DEFAULT_STREAM_CHUNK_SIZE
  if (!Number.isInteger(value) || value <= 0) {
    throw new PdfError('PDF_INVALID_ARGUMENT', 'chunkSize must be a positive integer', { path: 'chunkSize' })
  }
  return value
}

export class PdfDocument {
  private readonly unit: PdfUnit
  private readonly builder: NativeBuilder
  private readonly defaults: { size: PdfPageSize; layout: PdfLayout; margin: PdfMargins }
  private readonly styleStack: StyleState[] = []
  private readonly registeredFonts = new Set<string>()
  private style: StyleState = { ...DEFAULT_STYLE }
  private page: PageState
  private path: PathPoint[] | undefined
  private pathClosed = false
  private cursorX: number
  private cursorY: number
  private finished = false

  public constructor(options: PdfDocumentOptions = {}) {
    this.unit = normalizeUnit(options.unit)
    const margins = normalizeMargins(options.margin, this.unit)
    const size = options.size ?? 'A4'
    const layout = normalizeLayout(options.layout)
    const [width, height] = pageDimensions(options, this.unit)
    const Binding = getRuntime().binding.PdfDocumentBuilder
    this.builder = callNative(
      () => new Binding({ title: options.title, unit: this.unit, metadata: options.metadata }),
      'document',
    )
    this.defaults = { size, layout, margin: margins }
    this.page = { width, height, size, layout, margin: margins, elements: [], annotations: [] }
    callNative(() => this.builder.startPage({ width, height }), 'page')
    this.cursorX = margins.left
    this.cursorY = margins.top
  }

  public get x(): number {
    return this.cursorX
  }

  public get y(): number {
    return this.cursorY
  }

  public addPage(options: PdfPageOptions = {}): this {
    this.assertOpen()
    this.assertNoPendingPath()
    this.flushPage()
    const size = options.size ?? this.defaults.size
    const layout = normalizeLayout(options.layout ?? this.defaults.layout)
    const margin = normalizeMargins(options.margin ?? this.defaults.margin, this.unit)
    const [width, height] = pageDimensions({ size, layout }, this.unit)
    this.page = { width, height, size, layout, margin, elements: [], annotations: [] }
    callNative(() => this.builder.startPage({ width, height }), 'page')
    this.cursorX = margin.left
    this.cursorY = margin.top
    return this
  }

  public text(text: string, options?: PdfTextOptions): this
  public text(text: string, x: number, y: number, options?: PdfTextOptions): this
  public text(
    text: string,
    optionsOrX: PdfTextOptions | number = {},
    y?: number,
    positionalOptions: PdfTextOptionsBase & PdfTextBounds = {},
  ): this {
    this.assertOpen()
    if (typeof text !== 'string') throw new PdfError('PDF_INVALID_ARGUMENT', 'text must be a string', { path: 'text' })
    const options = typeof optionsOrX === 'number' ? { ...positionalOptions, x: optionsOrX, y } : optionsOrX
    const hasX = options.x !== undefined
    const hasY = options.y !== undefined
    if (hasX !== hasY)
      throw new PdfError('PDF_INVALID_ARGUMENT', 'text.x and text.y must be provided together', { path: 'text' })
    if (hasX) {
      assertFiniteNumber(options.x!, 'text.x')
      assertFiniteNumber(options.y!, 'text.y')
    }
    if (options.align !== undefined && options.width === undefined) {
      throw new PdfError('PDF_INVALID_ARGUMENT', 'text.align requires text.width', { path: 'text.align' })
    }
    if (options.height !== undefined && options.width === undefined) {
      throw new PdfError('PDF_INVALID_ARGUMENT', 'text.height requires text.width', { path: 'text.height' })
    }
    if (options.width !== undefined) assertFinitePositive(options.width, 'text.width')
    if (options.height !== undefined) assertFinitePositive(options.height, 'text.height')
    const style = this.resolveStyle(options)
    const flowing = !hasX && !hasY
    const x = options.x ?? this.cursorX
    let top = options.y ?? this.cursorY
    const lineHeightPoints = options.lineHeight ?? style.lineHeight
    const lineHeight = fromPoints(lineHeightPoints, this.unit)
    const width =
      options.width ?? (flowing ? this.page.width - this.page.margin.left - this.page.margin.right : undefined)
    const lines = width
      ? callNative(() => this.builder.layoutText(text, style.font, style.fontSize, width, options.hyphenate), 'text')
      : [{ text, paragraphEnd: true }]
    const totalHeight = lineHeight * Math.max(1, lines.length)
    if (flowing && width !== undefined && (options.overflow ?? 'paginate') === 'paginate') {
      let lineIndex = 0
      while (lineIndex < lines.length) {
        if (this.needsPageBreak(lineHeight)) {
          this.addPage({ size: this.page.size, layout: this.page.layout, margin: this.page.margin })
        }
        const availableLines = Math.max(
          1,
          Math.floor((this.page.height - this.page.margin.bottom - this.cursorY) / lineHeight),
        )
        const chunkLines = Math.min(availableLines, lines.length - lineIndex)
        const layoutLines = lines.slice(lineIndex, lineIndex + chunkLines)
        const chunk = layoutLines.map((line) => line.text).join('\n')
        this.page.elements.push({
          type: 'textBox',
          text: chunk,
          x: this.cursorX,
          y: this.cursorY,
          width,
          height: chunkLines * lineHeight,
          font: style.font,
          fontSize: style.fontSize,
          fill: style.fill,
          align: options.align,
          overflow: options.overflow,
          lineHeight: lineHeightPoints,
          hyphenate: options.hyphenate,
          layoutLines,
        })
        this.cursorY += chunkLines * lineHeight
        lineIndex += chunkLines
        if (lineIndex < lines.length) {
          this.addPage({ size: this.page.size, layout: this.page.layout, margin: this.page.margin })
        }
      }
      return this
    }
    if (flowing && this.needsPageBreak(totalHeight)) {
      this.addPage({ size: this.page.size, layout: this.page.layout, margin: this.page.margin })
      top = this.cursorY
    }
    if (width !== undefined) {
      this.page.elements.push({
        type: 'textBox',
        text,
        x,
        y: top,
        width,
        height: options.height,
        font: style.font,
        fontSize: style.fontSize,
        fill: style.fill,
        align: options.align,
        overflow: options.overflow,
        lineHeight: lineHeightPoints,
        hyphenate: options.hyphenate,
        layoutLines: lines,
      })
    } else {
      this.page.elements.push({
        type: 'text',
        text,
        x,
        y: top,
        font: style.font,
        fontSize: style.fontSize,
        fill: style.fill,
      })
    }
    if (flowing) this.cursorY = top + (options.height ?? totalHeight)
    return this
  }

  public textBox(text: string, options: PdfTextBoxOptions): this {
    return this.text(text, options)
  }

  public font(font: string): this {
    this.assertOpen()
    if (typeof font !== 'string' || !font.trim())
      throw new PdfError('PDF_INVALID_ARGUMENT', 'font must be a non-empty string', { path: 'font' })
    this.style.font = this.resolveFont(font)
    return this
  }

  public registerFont(family: string, source: PdfFontSource, options: PdfFontRegistrationOptions = {}): this {
    this.assertOpen()
    registerFontWithBuilder(this.builder, this.registeredFonts, { family, source, ...options })
    return this
  }

  public fontSize(fontSize: number): this {
    this.assertOpen()
    this.style.fontSize = assertFinitePositive(fontSize, 'fontSize')
    this.style.lineHeight = this.style.fontSize * 1.2
    return this
  }

  public fillColor(fill: string): this {
    this.assertOpen()
    this.style.fill = fill
    return this
  }

  public strokeColor(stroke: string): this {
    this.assertOpen()
    this.style.stroke = stroke
    return this
  }

  public lineWidth(width: number): this {
    this.assertOpen()
    this.style.strokeWidth = assertFinitePositive(width, 'lineWidth')
    return this
  }

  public save(): this {
    this.assertOpen()
    this.styleStack.push({ ...this.style })
    return this
  }

  public restore(): this {
    this.assertOpen()
    const style = this.styleStack.pop()
    if (!style)
      throw new PdfError('PDF_INVALID_ARGUMENT', 'restore() called without a matching save()', { path: 'restore' })
    this.style = style
    return this
  }

  public moveDown(lines = 1): this {
    this.assertOpen()
    this.cursorY += assertFinitePositive(lines, 'lines') * fromPoints(this.style.lineHeight, this.unit)
    return this
  }

  public moveUp(lines = 1): this {
    this.assertOpen()
    this.cursorY -= assertFinitePositive(lines, 'lines') * fromPoints(this.style.lineHeight, this.unit)
    return this
  }

  public moveTo(x: number, y: number): this {
    this.assertOpen()
    assertFiniteNumber(x, 'x')
    assertFiniteNumber(y, 'y')
    this.path = [{ x, y }]
    this.pathClosed = false
    return this
  }

  public lineTo(x: number, y: number): this {
    this.assertOpen()
    if (!this.path)
      throw new PdfError('PDF_INVALID_ARGUMENT', 'lineTo() requires an active path; call moveTo() first', {
        path: 'lineTo',
      })
    assertFiniteNumber(x, 'x')
    assertFiniteNumber(y, 'y')
    this.path.push({ x, y })
    return this
  }

  public closePath(): this {
    this.assertOpen()
    if (!this.path)
      throw new PdfError('PDF_INVALID_ARGUMENT', 'closePath() requires an active path; call moveTo() first', {
        path: 'closePath',
      })
    this.pathClosed = true
    return this
  }

  public rect(x: number, y: number, width: number, height: number): this {
    this.assertOpen()
    assertFiniteNumber(x, 'x')
    assertFiniteNumber(y, 'y')
    assertFinitePositive(width, 'width')
    assertFinitePositive(height, 'height')
    this.path = [
      { x, y },
      { x: x + width, y },
      { x: x + width, y: y + height },
      { x, y: y + height },
    ]
    this.pathClosed = true
    return this
  }

  public fill(): this {
    return this.paintPath('fill')
  }

  public stroke(): this {
    return this.paintPath('stroke')
  }

  public fillAndStroke(): this {
    return this.paintPath('fillAndStroke')
  }

  public discardPath(): this {
    this.assertOpen()
    this.path = undefined
    this.pathClosed = false
    return this
  }

  public image(source: PdfImageSource, options: PdfImageOptions = {}): this {
    this.assertOpen()
    const hasX = options.x !== undefined
    const hasY = options.y !== undefined
    if (hasX !== hasY)
      throw new PdfError('PDF_INVALID_ARGUMENT', 'image.x and image.y must be provided together', { path: 'image' })
    const bytes = normalizeBytes(source)
    if (options.fit !== undefined && (!Array.isArray(options.fit) || options.fit.length !== 2)) {
      throw new PdfError('PDF_INVALID_ARGUMENT', 'image.fit must contain width and height', { path: 'image.fit' })
    }
    if (options.align !== undefined && !['left', 'center', 'right'].includes(options.align)) {
      throw new PdfError('PDF_INVALID_ARGUMENT', 'image.align must be left, center, or right', { path: 'image.align' })
    }
    if (options.valign !== undefined && !['top', 'center', 'bottom'].includes(options.valign)) {
      throw new PdfError('PDF_INVALID_ARGUMENT', 'image.valign must be top, center, or bottom', {
        path: 'image.valign',
      })
    }
    if (options.x !== undefined) assertFiniteNumber(options.x, 'image.x')
    if (options.y !== undefined) assertFiniteNumber(options.y, 'image.y')
    const dimensions = callNative(() => getRuntime().binding.getImageDimensions(bytes), 'image.source')
    const naturalWidth = this.unit === 'mm' ? (dimensions.width * 25.4) / 72 : dimensions.width
    const naturalHeight = this.unit === 'mm' ? (dimensions.height * 25.4) / 72 : dimensions.height
    const flowing = !hasX && !hasY
    let boxWidth = options.width
    let boxHeight = options.height
    if (options.fit) {
      boxWidth = assertFinitePositive(options.fit[0]!, 'fit[0]')
      boxHeight = assertFinitePositive(options.fit[1]!, 'fit[1]')
    }
    if (boxWidth === undefined && boxHeight === undefined) {
      boxWidth = naturalWidth
      boxHeight = naturalHeight
    } else if (boxWidth === undefined) {
      boxHeight = assertFinitePositive(boxHeight!, 'height')
      boxWidth = (boxHeight * naturalWidth) / naturalHeight
    } else if (boxHeight === undefined) {
      boxWidth = assertFinitePositive(boxWidth, 'width')
      boxHeight = (boxWidth * naturalHeight) / naturalWidth
    } else {
      boxWidth = assertFinitePositive(boxWidth, 'width')
      boxHeight = assertFinitePositive(boxHeight, 'height')
    }
    let x = options.x ?? this.cursorX
    let y = options.y ?? this.cursorY
    if (flowing && this.needsPageBreak(boxHeight)) {
      this.addPage({ size: this.page.size, layout: this.page.layout, margin: this.page.margin })
      x = this.cursorX
      y = this.cursorY
    }
    let drawWidth = boxWidth
    let drawHeight = boxHeight
    if (options.fit) {
      const scale = Math.min(boxWidth / naturalWidth, boxHeight / naturalHeight)
      drawWidth = naturalWidth * scale
      drawHeight = naturalHeight * scale
      x +=
        options.align === 'center' ? (boxWidth - drawWidth) / 2 : options.align === 'right' ? boxWidth - drawWidth : 0
      y +=
        options.valign === 'center'
          ? (boxHeight - drawHeight) / 2
          : options.valign === 'bottom'
            ? boxHeight - drawHeight
            : 0
    }
    this.page.elements.push({ type: 'image', x, y, width: drawWidth, height: drawHeight, imageData: bytes })
    if (flowing) this.cursorY = y + boxHeight
    return this
  }

  public table<Row extends Record<string, unknown>>(options: PdfTableOptions<Row>): this {
    this.assertOpen()
    const previousStyle = { ...this.style }
    const baseStyle = { ...this.style }
    if (!Array.isArray(options.columns) || options.columns.length === 0) {
      throw new PdfError('PDF_INVALID_ARGUMENT', 'table.columns must contain at least one column', {
        path: 'table.columns',
      })
    }
    if (!Array.isArray(options.rows))
      throw new PdfError('PDF_INVALID_ARGUMENT', 'table.rows must be an array', { path: 'table.rows' })
    if ((options.x === undefined) !== (options.y === undefined)) {
      throw new PdfError('PDF_INVALID_ARGUMENT', 'table.x and table.y must be provided together', { path: 'table' })
    }
    if (options.x !== undefined) assertFiniteNumber(options.x, 'table.x')
    if (options.y !== undefined) assertFiniteNumber(options.y, 'table.y')
    if (options.rowSplit !== undefined && options.rowSplit !== 'avoid' && options.rowSplit !== 'split') {
      throw new PdfError('PDF_INVALID_ARGUMENT', 'table.rowSplit must be "avoid" or "split"', {
        path: 'table.rowSplit',
      })
    }
    const x = options.x ?? this.cursorX
    let y = options.y ?? this.cursorY
    const width = options.width ?? this.page.width - x - this.page.margin.right
    assertFinitePositive(width, 'table.width')
    const padding = assertFiniteNonNegative(options.padding ?? 2, 'table.padding')
    const rowHeight = assertFinitePositive(options.rowHeight ?? fromPoints(24, this.unit), 'table.rowHeight')
    const headerHeight = assertFinitePositive(options.headerHeight ?? rowHeight, 'table.headerHeight')
    const border = options.border ?? '#cbd5e1'
    const repeatHeader = options.repeatHeader ?? true
    const headers = options.columns.map((column) => String(column.header ?? ''))
    const values = options.rows.map((row, rowIndex) => {
      if (!row || typeof row !== 'object') {
        throw new PdfError('PDF_INVALID_ARGUMENT', 'table rows must be objects', {
          path: `table.rows[${rowIndex}]`,
        })
      }
      return options.columns.map((column, columnIndex) => {
        const path = `table.rows[${rowIndex}].columns[${columnIndex}]`
        const rawValue = column.value
          ? callUserCallback(() => column.value!(row, rowIndex), path)
          : column.key
            ? (row[column.key] as PdfTableCellValue)
            : ''
        const formatted = column.formatter
          ? callUserCallback(() => column.formatter!(rawValue, row, rowIndex), path)
          : rawValue
        return String(formatted ?? '')
      })
    })
    for (const [index, column] of options.columns.entries()) {
      if (column.padding !== undefined) assertFiniteNonNegative(column.padding, `table.columns[${index}].padding`)
      if (column.minWidth !== undefined) assertFinitePositive(column.minWidth, `table.columns[${index}].minWidth`)
      if (column.maxWidth !== undefined) assertFinitePositive(column.maxWidth, `table.columns[${index}].maxWidth`)
      if (column.minWidth !== undefined && column.maxWidth !== undefined && column.minWidth > column.maxWidth) {
        throw new PdfError('PDF_INVALID_ARGUMENT', 'column minWidth cannot exceed maxWidth', {
          path: `table.columns[${index}]`,
        })
      }
      if (column.align !== undefined && !['left', 'center', 'right', 'justify'].includes(column.align)) {
        throw new PdfError('PDF_INVALID_ARGUMENT', 'column.align must be left, center, right, or justify', {
          path: `table.columns[${index}].align`,
        })
      }
    }
    const columnWidths = this.resolveTableColumnWidths(options.columns, headers, values, width)

    const drawRow = (
      cells: readonly string[],
      top: number,
      height: number,
      header: boolean,
      rowIndex: number,
    ): void => {
      let left = x
      for (let columnIndex = 0; columnIndex < options.columns.length; columnIndex += 1) {
        const column = options.columns[columnIndex]!
        const cellWidth = columnWidths[columnIndex]!
        const cellPadding = column.padding ?? padding
        const background = header
          ? (column.background ?? '#e2e8f0')
          : rowIndex % 2 === 1
            ? options.stripe
            : column.background
        this.rect(left, top, cellWidth, height)
        if (background || border) {
          if (background) this.fillColor(background)
          this.strokeColor(column.stroke ?? border)
          this.lineWidth(column.strokeWidth ?? 0.5)
          background ? this.fillAndStroke() : this.stroke()
        } else {
          this.discardPath()
        }
        const textColor = column.fill ?? (header ? '#0f172a' : baseStyle.fill)
        this.text(cells[columnIndex] ?? '', {
          x: left + cellPadding,
          y: top + cellPadding / 2,
          width: Math.max(1, cellWidth - cellPadding * 2),
          height: Math.max(1, height - cellPadding),
          font: column.font ?? (header ? 'HelveticaBold' : this.style.font),
          fontSize: column.fontSize ?? (header ? Math.max(7, this.style.fontSize - 1) : this.style.fontSize),
          fill: textColor,
          align: column.align ?? 'left',
          overflow: 'clip',
        })
        left += cellWidth
      }
    }

    const drawHeader = (): void => {
      if (headers.some(Boolean)) drawRow(headers, y, headerHeight, true, -1)
      if (headers.some(Boolean)) y += headerHeight
    }

    const nextTablePage = (): void => {
      this.addPage({ size: this.page.size, layout: this.page.layout, margin: this.page.margin })
      y = this.cursorY
      if (repeatHeader) drawHeader()
    }

    drawHeader()
    for (let rowIndex = 0; rowIndex < values.length; rowIndex += 1) {
      const pageBottom = this.page.height - this.page.margin.bottom
      const fullPageHeight = this.page.height - this.page.margin.top - this.page.margin.bottom
      if ((options.rowSplit ?? 'avoid') === 'avoid') {
        if (rowHeight > fullPageHeight) {
          this.style = previousStyle
          throw new PdfError('PDF_LAYOUT_ERROR', `table row ${rowIndex} cannot fit on a page`, {
            path: `table.rows[${rowIndex}]`,
          })
        }
        if (y + rowHeight > pageBottom) nextTablePage()
        drawRow(values[rowIndex]!, y, rowHeight, false, rowIndex)
        y += rowHeight
      } else {
        let remainingHeight = rowHeight
        while (remainingHeight > 0) {
          if (y >= pageBottom) nextTablePage()
          const fragmentHeight = Math.min(remainingHeight, pageBottom - y)
          drawRow(values[rowIndex]!, y, fragmentHeight, false, rowIndex)
          y += fragmentHeight
          remainingHeight -= fragmentHeight
          if (remainingHeight > 0) nextTablePage()
        }
      }
    }
    this.cursorX = x
    this.cursorY = y
    this.style = previousStyle
    return this
  }

  public link(url: string, options: PdfLinkOptions): this {
    this.assertOpen()
    if (!url.trim()) throw new PdfError('PDF_INVALID_ARGUMENT', 'url must not be empty', { path: 'url' })
    assertFiniteNumber(options.x, 'x')
    assertFiniteNumber(options.y, 'y')
    assertFinitePositive(options.width, 'width')
    assertFinitePositive(options.height, 'height')
    this.page.annotations.push({ type: 'link', ...options, url })
    return this
  }

  public render(): PdfOutput {
    this.assertOpen()
    this.assertNoPendingPath()
    this.flushPage()
    this.finished = true
    return output(async (signal) => {
      if (signal?.aborted) throw abortedError()
      return this.builder.finishStream(true)
    })
  }

  private resolveStyle(options: PdfTextStyleOptions & { lineHeight?: number }): StyleState {
    const style = { ...this.style }
    style.font = this.resolveFont(options.font ?? style.font)
    if (options.fontSize !== undefined) style.fontSize = assertFinitePositive(options.fontSize, 'fontSize')
    if (options.fill !== undefined) style.fill = options.fill
    if (options.lineHeight !== undefined) style.lineHeight = assertFinitePositive(options.lineHeight, 'lineHeight')
    else if (options.fontSize !== undefined) style.lineHeight = style.fontSize * 1.2
    return style
  }

  private resolveFont(font: string): string {
    if (this.registeredFonts.has(font) || isBuiltinFont(font)) return font
    throw new PdfError('PDF_FONT_NOT_FOUND', `font "${font}" was not registered`, { path: 'font' })
  }

  private resolveTableColumnWidths(
    columns: readonly PdfTableColumn[],
    headers: readonly string[],
    values: readonly (readonly string[])[],
    totalWidth: number,
  ): number[] {
    const widths = columns.map((column, index) => {
      if (typeof column.width === 'number') return assertFinitePositive(column.width, `table.columns[${index}].width`)
      if (column.width === '*') return 0
      const candidates = [headers[index] ?? '', ...values.map((row) => row[index] ?? '')]
      const measured = callNative(
        () =>
          this.builder.measureTexts(candidates, column.font ?? this.style.font, column.fontSize ?? this.style.fontSize),
        `table.columns[${index}]`,
      )
      const estimate = fromPoints(Math.max(...measured, 0) + 8, this.unit)
      const minWidth = column.minWidth ?? 0
      const maxWidth = column.maxWidth ?? Number.POSITIVE_INFINITY
      return Math.min(maxWidth, Math.max(minWidth, estimate))
    })
    const starCount = columns.filter((column) => column.width === '*').length
    const used = widths.reduce((sum, width) => sum + width, 0)
    const remaining = totalWidth - used
    if (remaining < 0 && starCount === 0) {
      throw new PdfError('PDF_LAYOUT_ERROR', 'table columns exceed the available width')
    }
    const starWidth = starCount > 0 ? Math.max(1, remaining / starCount) : 0
    return widths.map((width, index) => (columns[index]?.width === '*' ? starWidth : width))
  }

  private paintPath(mode: 'fill' | 'stroke' | 'fillAndStroke'): this {
    this.assertOpen()
    if (!this.path || this.path.length < 2) {
      throw new PdfError('PDF_LAYOUT_ERROR', 'painting requires an active path with at least two points', {
        path: 'path',
      })
    }
    const { style } = this
    this.page.elements.push({
      type: 'path',
      points: this.path,
      closed: this.pathClosed,
      fill: mode === 'stroke' ? undefined : style.fill,
      stroke: mode === 'fill' ? undefined : style.stroke,
      strokeWidth: style.strokeWidth,
    })
    this.discardPath()
    return this
  }

  private needsPageBreak(height: number): boolean {
    return this.cursorY + height > this.page.height - this.page.margin.bottom
  }

  private flushPage(): void {
    callNative(() => this.builder.appendElements(this.page.elements), 'currentPage.elements')
    callNative(() => this.builder.appendAnnotations(this.page.annotations), 'currentPage.annotations')
    callNative(() => this.builder.endPage(), 'currentPage')
  }

  private assertNoPendingPath(): void {
    if (this.path)
      throw new PdfError('PDF_LAYOUT_ERROR', 'cannot change pages or finish with an unpainted path', { path: 'path' })
  }

  private assertOpen(): void {
    if (this.finished) throw new PdfError('PDF_DOCUMENT_FINISHED', 'PdfDocument has already been rendered')
  }
}

export function renderPdf(input: PdfDocumentInput): PdfOutput {
  if (!input || !Array.isArray(input.pages) || input.pages.length === 0) {
    throw new PdfError('PDF_INVALID_ARGUMENT', 'pages must contain at least one page', { path: 'pages' })
  }
  if (input.fonts !== undefined && !Array.isArray(input.fonts)) {
    throw new PdfError('PDF_INVALID_ARGUMENT', 'fonts must be an array', { path: 'fonts' })
  }
  const unit = normalizeUnit(input.unit)
  const pages = [...input.pages]
  const fonts = [...(input.fonts ?? [])]
  for (const [index, page] of pages.entries()) validatePageInput(page, unit, `pages[${index}]`)

  return output(async (signal) => {
    if (signal?.aborted) throw abortedError()
    const Binding = getRuntime().binding.PdfDocumentBuilder
    const builder = callNative(() => new Binding({ title: input.title, unit, metadata: input.metadata }), 'document')
    const registeredFonts = new Set<string>()
    for (const [index, font] of fonts.entries()) {
      if (signal?.aborted) throw abortedError()
      registerFontWithBuilder(builder, registeredFonts, font, `fonts[${index}]`)
    }
    for (const page of pages) {
      if (signal?.aborted) throw abortedError()
      appendDeclarativePage(builder, page, unit)
    }
    return builder.finishStream(true)
  })
}
