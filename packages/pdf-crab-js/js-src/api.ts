export type PdfUnit = 'mm' | 'pt'
export type PdfPageSize = 'A3' | 'A4' | 'LETTER' | readonly [number, number]
export type PdfLayout = 'portrait' | 'landscape'
export type PdfImageBytes = Uint8Array | ArrayBuffer
export type PdfImageSource = PdfImageBytes | string
export type PdfImageAlign = 'left' | 'center' | 'right'
export type PdfImageValign = 'top' | 'center' | 'bottom'
export type PdfTextAlign = 'left' | 'center' | 'right' | 'justify'

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

export interface PdfStyleOptions {
  font?: string
  fontSize?: number
  fill?: string
  stroke?: string
  strokeWidth?: number
}

export interface PdfTextOptions extends PdfStyleOptions {
  x?: number
  y?: number
  align?: PdfTextAlign
  lineHeight?: number
  hyphenate?: boolean
  width?: number
}

export interface PdfTextBoxOptions extends PdfStyleOptions {
  x?: number
  y?: number
  width: number
  height?: number
  align?: PdfTextAlign
  lineHeight?: number
  hyphenate?: boolean
}

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

export interface PdfTextElement extends PdfStyleOptions {
  type: 'text'
  text: string
  x: number
  y: number
  align?: PdfTextAlign
}

export interface PdfTextBoxElement extends PdfStyleOptions {
  type: 'textBox'
  text: string
  x: number
  y: number
  width: number
  height?: number
  align?: PdfTextAlign
  lineHeight?: number
  hyphenate?: boolean
}

export interface PdfLineElement extends PdfStyleOptions {
  type: 'line'
  x1: number
  y1: number
  x2: number
  y2: number
}

export interface PdfRectElement extends PdfStyleOptions {
  type: 'rect'
  x: number
  y: number
  width: number
  height: number
}

export interface PdfPolygonElement extends PdfStyleOptions {
  type: 'polygon'
  points: { x: number; y: number }[]
  closed?: boolean
  winding?: 'nonZero' | 'evenOdd'
}

export interface PdfPathElement extends PdfStyleOptions {
  type: 'path'
  points: { x: number; y: number }[]
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
  | PdfTextBoxElement
  | PdfLineElement
  | PdfRectElement
  | PdfPolygonElement
  | PdfPathElement
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

export interface PdfPageInput extends PdfPageOptions {
  elements?: PdfElementInput[]
  annotations?: PdfAnnotationInput[]
}

export interface CreatePdfInput {
  title?: string
  unit?: PdfUnit
  metadata?: PdfMetadata
  pages: [PdfPageInput, ...PdfPageInput[]]
}

interface NativeBuilder {
  startPage: (page: { width: number; height: number }) => void
  appendElements: (elements: Record<string, unknown>[]) => void
  appendAnnotations: (annotations: Record<string, unknown>[]) => void
  endPage: () => void
  finish: () => Uint8Array
  finishAsync: () => Promise<Uint8Array>
}

interface NativeBinding {
  createPdf: (input: Record<string, unknown>) => Uint8Array
  createPdfAsync: (input: Record<string, unknown>) => Promise<Uint8Array>
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
    throw new Error('pdf-crab-js runtime has not been initialized')
  }
  return runtime
}

const PAGE_SIZES_MM: Record<Exclude<PdfPageSize, readonly [number, number]>, readonly [number, number]> = {
  A3: [297, 420],
  A4: [210, 297],
  LETTER: [215.9, 279.4],
}

const POINTS_PER_MM = 72 / 25.4

function assertFinitePositive(value: number, name: string): number {
  if (!Number.isFinite(value) || value <= 0) {
    throw new TypeError(`${name} must be a finite number greater than 0`)
  }
  return value
}

function assertFiniteNonNegative(value: number, name: string): number {
  if (!Number.isFinite(value) || value < 0) {
    throw new TypeError(`${name} must be a finite number greater than or equal to 0`)
  }
  return value
}

function normalizeUnit(unit: PdfUnit | undefined): PdfUnit {
  if (unit === undefined) return 'mm'
  if (unit !== 'mm' && unit !== 'pt') throw new TypeError('unit must be "mm" or "pt"')
  return unit
}

function normalizeMargins(margin: PdfDocumentOptions['margin'] | undefined): PdfMargins {
  if (margin === undefined) return { top: 20, right: 20, bottom: 20, left: 20 }
  if (typeof margin === 'number') {
    assertFiniteNonNegative(margin, 'margin')
    return { top: margin, right: margin, bottom: margin, left: margin }
  }
  const defaultMargin = 20
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
    return [
      assertFinitePositive(resolved[0]!, 'page size width'),
      assertFinitePositive(resolved[1]!, 'page size height'),
    ]
  }
  const millimeters = PAGE_SIZES_MM[resolved as Exclude<PdfPageSize, readonly [number, number]>]
  if (!millimeters) throw new TypeError('size must be A3, A4, LETTER, or [width, height]')
  return unit === 'mm'
    ? [millimeters[0], millimeters[1]]
    : [millimeters[0] * POINTS_PER_MM, millimeters[1] * POINTS_PER_MM]
}

function pageDimensions(options: PdfPageOptions | PdfDocumentOptions, unit: PdfUnit): [number, number] {
  const dimensions = pageSizeInUnit(options.size, unit)
  return options.layout === 'landscape' ? [dimensions[1], dimensions[0]] : dimensions
}

function toPoints(value: number, unit: PdfUnit): number {
  return unit === 'mm' ? value * POINTS_PER_MM : value
}

function fromPoints(value: number, unit: PdfUnit): number {
  return unit === 'mm' ? (value * 25.4) / 72 : value
}

function normalizeBytes(source: PdfImageSource): Uint8Array {
  if (typeof source === 'string') return getRuntime().resolveImageSource(source)
  if (source instanceof ArrayBuffer) return new Uint8Array(source)
  if (ArrayBuffer.isView(source)) {
    return new Uint8Array(source.buffer, source.byteOffset, source.byteLength)
  }
  throw new TypeError('image source must be Uint8Array, ArrayBuffer, or a Node.js file path')
}

function normalizeElement(element: PdfElementInput, unit: PdfUnit): Record<string, unknown> {
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
  const dimensions = getRuntime().binding.getImageDimensions(bytes)
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

function normalizePage(page: PdfPageInput, unit: PdfUnit): Record<string, unknown> {
  const [width, height] = pageDimensions(page, unit)
  return {
    width,
    height,
    elements: (page.elements ?? []).map((element) => normalizeElement(element, unit)),
    annotations: page.annotations,
  }
}

function normalizeCreatePdfInput(input: CreatePdfInput): Record<string, unknown> {
  if (!input || !Array.isArray(input.pages) || input.pages.length === 0) {
    throw new TypeError('pages must contain at least one page')
  }
  const unit = normalizeUnit(input.unit)
  return {
    title: input.title,
    unit,
    metadata: input.metadata,
    pages: input.pages.map((page) => normalizePage(page, unit)),
  }
}

function estimateTextLines(text: string, width: number, fontSize: number, unit: PdfUnit): number {
  const maxChars = Math.max(1, Math.floor(toPoints(width, unit) / (fontSize * 0.5)))
  let count = 0
  for (const paragraph of text.split('\n')) {
    count += paragraph ? Math.max(1, Math.ceil(paragraph.length / maxChars)) : 1
  }
  return count
}

interface PageState {
  width: number
  height: number
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

export class PdfDocument {
  private readonly unit: PdfUnit
  private readonly builder: NativeBuilder
  private readonly styleStack: StyleState[] = []
  private style: StyleState = { ...DEFAULT_STYLE }
  private page: PageState
  private path: PathPoint[] | undefined
  private pathClosed = false
  private cursorX: number
  private cursorY: number
  private finished = false

  public constructor(options: PdfDocumentOptions = {}) {
    this.unit = normalizeUnit(options.unit)
    const margins = normalizeMargins(options.margin)
    const [width, height] = pageDimensions(options, this.unit)
    const Binding = getRuntime().binding.PdfDocumentBuilder
    this.builder = new Binding({ title: options.title, unit: this.unit, metadata: options.metadata })
    this.page = { width, height, margin: margins, elements: [], annotations: [] }
    this.builder.startPage({ width, height })
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
    const margin = normalizeMargins(options.margin ?? this.page.margin)
    const [width, height] = pageDimensions(options, this.unit)
    this.page = { width, height, margin, elements: [], annotations: [] }
    this.builder.startPage({ width, height })
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
    positionalOptions: PdfTextOptions = {},
  ): this {
    this.assertOpen()
    const options = typeof optionsOrX === 'number' ? { ...positionalOptions, x: optionsOrX, y } : optionsOrX
    const style = this.resolveStyle(options)
    const flowing = options.x === undefined && options.y === undefined
    const x = options.x ?? this.cursorX
    let top = options.y ?? this.cursorY
    const lineHeightPoints = options.lineHeight ?? style.lineHeight
    const lineHeight = fromPoints(lineHeightPoints, this.unit)
    const lines = options.width ? estimateTextLines(text, options.width, style.fontSize, this.unit) : 1
    const totalHeight = lineHeight * lines
    if (flowing && this.needsPageBreak(totalHeight)) {
      this.addPage()
      top = this.cursorY
    }
    if (options.width !== undefined) {
      this.page.elements.push({
        type: 'textBox',
        text,
        x,
        y: top,
        width: options.width,
        font: style.font,
        fontSize: style.fontSize,
        fill: style.fill,
        align: options.align,
        lineHeight: lineHeightPoints,
        hyphenate: options.hyphenate,
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
    if (flowing) this.cursorY = top + totalHeight
    return this
  }

  public textBox(text: string, options: PdfTextBoxOptions): this {
    return this.text(text, options)
  }

  public font(font: string): this {
    this.style.font = font
    return this
  }

  public fontSize(fontSize: number): this {
    this.style.fontSize = assertFinitePositive(fontSize, 'fontSize')
    this.style.lineHeight = this.style.fontSize * 1.2
    return this
  }

  public fillColor(fill: string): this {
    this.style.fill = fill
    return this
  }

  public strokeColor(stroke: string): this {
    this.style.stroke = stroke
    return this
  }

  public lineWidth(width: number): this {
    this.style.strokeWidth = assertFinitePositive(width, 'lineWidth')
    return this
  }

  public save(): this {
    this.styleStack.push({ ...this.style })
    return this
  }

  public restore(): this {
    const style = this.styleStack.pop()
    if (!style) throw new Error('restore() called without a matching save()')
    this.style = style
    return this
  }

  public moveDown(lines = 1): this {
    this.cursorY += assertFinitePositive(lines, 'lines') * fromPoints(this.style.lineHeight, this.unit)
    return this
  }

  public moveUp(lines = 1): this {
    this.cursorY -= assertFinitePositive(lines, 'lines') * fromPoints(this.style.lineHeight, this.unit)
    return this
  }

  public moveTo(x: number, y: number): this {
    this.path = [{ x, y }]
    this.pathClosed = false
    return this
  }

  public lineTo(x: number, y: number): this {
    if (!this.path) throw new Error('lineTo() requires an active path; call moveTo() first')
    this.path.push({ x, y })
    return this
  }

  public closePath(): this {
    if (!this.path) throw new Error('closePath() requires an active path; call moveTo() first')
    this.pathClosed = true
    return this
  }

  public rect(x: number, y: number, width: number, height: number): this {
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
    this.path = undefined
    this.pathClosed = false
    return this
  }

  public image(source: PdfImageSource, options: PdfImageOptions = {}): this {
    this.assertOpen()
    const bytes = normalizeBytes(source)
    const dimensions = getRuntime().binding.getImageDimensions(bytes)
    const naturalWidth = this.unit === 'mm' ? (dimensions.width * 25.4) / 72 : dimensions.width
    const naturalHeight = this.unit === 'mm' ? (dimensions.height * 25.4) / 72 : dimensions.height
    const flowing = options.x === undefined && options.y === undefined
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
      this.addPage()
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

  public link(url: string, options: PdfLinkOptions): this {
    if (!url.trim()) throw new TypeError('url must not be empty')
    this.page.annotations.push({ type: 'link', ...options, url })
    return this
  }

  public finish(): Uint8Array {
    this.assertOpen()
    this.assertNoPendingPath()
    this.flushPage()
    this.finished = true
    return this.builder.finish()
  }

  public async finishAsync(): Promise<Uint8Array> {
    this.assertOpen()
    this.assertNoPendingPath()
    this.flushPage()
    this.finished = true
    return this.builder.finishAsync()
  }

  private resolveStyle(options: PdfStyleOptions & { lineHeight?: number }): StyleState {
    const style = { ...this.style }
    if (options.font !== undefined) style.font = options.font
    if (options.fontSize !== undefined) style.fontSize = assertFinitePositive(options.fontSize, 'fontSize')
    if (options.fill !== undefined) style.fill = options.fill
    if (options.stroke !== undefined) style.stroke = options.stroke
    if (options.strokeWidth !== undefined) style.strokeWidth = assertFinitePositive(options.strokeWidth, 'strokeWidth')
    if (options.lineHeight !== undefined) style.lineHeight = assertFinitePositive(options.lineHeight, 'lineHeight')
    else if (options.fontSize !== undefined) style.lineHeight = style.fontSize * 1.2
    return style
  }

  private paintPath(mode: 'fill' | 'stroke' | 'fillAndStroke'): this {
    this.assertOpen()
    if (!this.path || this.path.length < 2) throw new Error('painting requires an active path with at least two points')
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
    this.builder.appendElements(this.page.elements)
    this.builder.appendAnnotations(this.page.annotations)
    this.builder.endPage()
  }

  private assertNoPendingPath(): void {
    if (this.path) throw new Error('cannot change pages or finish with an unpainted path')
  }

  private assertOpen(): void {
    if (this.finished) throw new Error('PdfDocument has already finished')
  }
}

export function createPdf(input: CreatePdfInput): Uint8Array {
  return getRuntime().binding.createPdf(normalizeCreatePdfInput(input))
}

export function createPdfAsync(input: CreatePdfInput): Promise<Uint8Array> {
  return getRuntime().binding.createPdfAsync(normalizeCreatePdfInput(input))
}
