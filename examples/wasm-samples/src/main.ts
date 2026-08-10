import { Buffer as BrowserBuffer } from 'buffer'
import type { PdfOutput } from 'pdf-crab-js/browser.js'

import brandImageUrl from '../../pdf-crab-js/assets/pdf-crab-js-image-source.png?url'
import tuffyFontUrl from '../assets/Tuffy.ttf?url'
import { buildCatalog } from './samples/catalog.js'
import catalogSource from './samples/catalog.ts?raw'
import { buildInvoice } from './samples/invoice.js'
import invoiceSource from './samples/invoice.ts?raw'
import { buildReport } from './samples/report.js'
import reportSource from './samples/report.ts?raw'
import type { RenderedExample, SampleContext } from './samples/types.js'
import './styles.css'

globalThis.Buffer ??= BrowserBuffer

type ElementConstructor<T extends Element> = new (...args: never[]) => T
type TemplateName = 'invoice' | 'report' | 'catalog'
type ModeName = 'structured' | 'html'
type DeliveryMode = 'bytes' | 'stream'
type PreviewState = {
  delivery: string
  filename: string
  size: string
  time: string
  title: string
  url: string
}
type HtmlPdfInput = {
  css?: string
  fonts?: string[]
  html: string
  page?: {
    margin?: number
    size?: 'A3' | 'A4' | 'LETTER'
  }
  systemFonts?: boolean
  tagged?: boolean
  title?: string
}
type HtmlPdfBinding = {
  createPdfFromHtml: (input: HtmlPdfInput) => Promise<Uint8Array> | Uint8Array
}

const sourceFiles: Record<TemplateName, { filename: string; source: string }> = {
  catalog: { filename: 'src/samples/catalog.ts', source: catalogSource },
  invoice: { filename: 'src/samples/invoice.ts', source: invoiceSource },
  report: { filename: 'src/samples/report.ts', source: reportSource },
}

const defaultTitles: Record<TemplateName, string> = {
  catalog: 'Declarative catalog',
  invoice: 'Structured invoice',
  report: 'Multi-page report',
}

const defaultHtml = `<article class="report">
  <header class="hero-report">
    <div>
      <p class="kicker">Executive report / Q3 2026</p>
      <h1>Operation Crab</h1>
      <p>A rich HTML document, paginated and rendered locally with WebAssembly.</p>
    </div>
    <div class="quarter"><span>Q3</span><strong>26</strong></div>
  </header>

  <section class="metadata">
    <div><span>Prepared for</span><strong>Flash team</strong></div>
    <div><span>Period</span><strong>Jul - Sep</strong></div>
    <div><span>Confidentiality</span><strong>Internal</strong></div>
  </section>

  <section class="metrics">
    <article><span>Recurring revenue</span><strong>$4.8M</strong><em>+18.4% this quarter</em></article>
    <article><span>Documents generated</span><strong>1.24M</strong><em>99.98% success rate</em></article>
    <article><span>Median render time</span><strong>6.2 ms</strong><em>Structured API</em></article>
  </section>

  <section class="split">
    <article class="panel">
      <span class="section-number">01 / SUMMARY</span>
      <h2>Quarter decisions</h2>
      <p>We consolidated one output contract for bytes, Blob, Web Stream, and async iteration. The same incremental renderer serves Node and browsers.</p>
      <ul>
        <li>Embedded TTF font with a ToUnicode map.</li>
        <li>Tables with repeated headers.</li>
        <li>Lazy, cancellable, single-use output.</li>
      </ul>
    </article>
    <article class="panel panel-accent">
      <span class="section-number">02 / FOCUS</span>
      <h2>Next cycle</h2>
      <p>Expand invoice, report, and structured-document use cases while keeping the bundle predictable.</p>
      <blockquote>A focused API can deliver exceptional DX when its contracts are honest.</blockquote>
    </article>
  </section>

  <section class="table-section">
    <span class="section-number">03 / CAPABILITIES</span>
    <h2>Rendering matrix</h2>
    <table>
      <thead><tr><th>Flow</th><th>Input</th><th>Output</th><th>Best for</th></tr></thead>
      <tbody>
        <tr><td>Structured</td><td>Typed objects</td><td>Incremental</td><td>Invoices and tables</td></tr>
        <tr><td>Document</td><td>Fluent API</td><td>Lazy output</td><td>Programmatic reports</td></tr>
        <tr><td>HTML</td><td>HTML + CSS</td><td>Threadless WASM</td><td>Existing templates</td></tr>
      </tbody>
    </table>
  </section>

  <footer><span>crab-js / PDF laboratory</span><strong>Generated in your browser</strong></footer>
</article>`

const defaultCss = `body {
  margin: 0;
  background: #fff;
  color: #20212a;
  font-family: Tuffy, Arial, sans-serif;
}

.report { padding: 30px; }
.hero-report {
  display: flex;
  justify-content: space-between;
  gap: 24px;
  padding: 26px;
  background: #231b1d;
  color: #fff;
}
.hero-report h1 { margin: 3px 0 8px; font-size: 34px; }
.hero-report p { margin: 0; color: #ffdce8; font-size: 11px; }
.kicker, .section-number, section span, footer span {
  color: #7f636b;
  font-size: 9px;
  font-weight: 700;
  letter-spacing: 1px;
  text-transform: uppercase;
}
.hero-report .kicker { color: #ffb3d0; }
.quarter {
  min-width: 70px;
  border: 1px solid #77708f;
  padding: 12px;
  text-align: center;
}
.quarter span, .quarter strong { display: block; }
.quarter span { color: #ffb3d0; font-size: 11px; }
.quarter strong { font-size: 27px; }
.metadata, .metrics, .split { display: flex; gap: 12px; }
.metadata { margin: 17px 0; }
.metadata div, .metrics article, .panel { flex: 1; border: 1px solid #e8dcdf; padding: 13px; }
.metadata strong, .metrics strong { display: block; margin-top: 4px; font-size: 16px; }
.metrics article { background: #f8f1f3; }
.metrics em { display: block; margin-top: 5px; color: #067647; font-size: 9px; font-style: normal; }
.split { margin: 18px 0; }
.panel h2, .table-section h2 { margin: 7px 0 9px; font-size: 15px; }
.panel p, li, blockquote { color: #5e494e; font-size: 10px; line-height: 1.5; }
.panel-accent { border-color: #ffdce8; background: #fff0f4; }
blockquote { margin: 12px 0 0; border-left: 3px solid #f33690; padding-left: 10px; }
ul { margin: 8px 0 0; padding-left: 17px; }
table { width: 100%; border-collapse: collapse; font-size: 10px; }
th { background: #231b1d; color: #fff; padding: 9px; text-align: left; }
td { border-bottom: 1px solid #e8dcdf; padding: 9px; }
footer {
  display: flex;
  justify-content: space-between;
  margin-top: 22px;
  border-top: 2px solid #231b1d;
  padding-top: 10px;
  font-size: 9px;
}`

function requiredElement<T extends Element>(selector: string, constructor: ElementConstructor<T>): T {
  const element = document.querySelector(selector)

  if (!(element instanceof constructor)) {
    throw new Error(`The example markup is missing ${selector}`)
  }

  return element
}

const elements = {
  accentColor: requiredElement('#accentColor', HTMLInputElement),
  companyName: requiredElement('#companyName', HTMLInputElement),
  copySourceButton: requiredElement('#copySourceButton', HTMLButtonElement),
  cssInput: requiredElement('#cssInput', HTMLTextAreaElement),
  deliveryMetric: requiredElement('#deliveryMetric', HTMLElement),
  deliveryMode: requiredElement('#deliveryMode', HTMLSelectElement),
  downloadLink: requiredElement('#downloadLink', HTMLAnchorElement),
  editorPanels: [...document.querySelectorAll<HTMLElement>('[data-editor-panel]')],
  editorTabs: [...document.querySelectorAll<HTMLButtonElement>('[data-editor]')],
  generateActions: [...document.querySelectorAll<HTMLButtonElement>('.generate-action')],
  htmlForm: requiredElement('#html-panel', HTMLFormElement),
  htmlInput: requiredElement('#htmlInput', HTMLTextAreaElement),
  htmlPreviewFrame: requiredElement('#htmlPreviewFrame', HTMLIFrameElement),
  htmlRuntimeNotice: requiredElement('#htmlRuntimeNotice', HTMLElement),
  modePanels: [...document.querySelectorAll<HTMLElement>('[data-panel]')],
  modeTabs: [...document.querySelectorAll<HTMLButtonElement>('[data-mode]')],
  openButton: requiredElement('#openButton', HTMLButtonElement),
  outputCard: requiredElement('.output-card', HTMLElement),
  outputTitle: requiredElement('#outputTitle', HTMLElement),
  pageMargin: requiredElement('#pageMargin', HTMLInputElement),
  pageSize: requiredElement('#pageSize', HTMLSelectElement),
  pdfPreview: requiredElement('#pdfPreview', HTMLIFrameElement),
  recipientName: requiredElement('#recipientName', HTMLInputElement),
  rowCount: requiredElement('#rowCount', HTMLSelectElement),
  runtimeBadge: requiredElement('#runtimeBadge', HTMLElement),
  runtimeDetail: requiredElement('#runtimeDetail', HTMLElement),
  sizeMetric: requiredElement('#sizeMetric', HTMLElement),
  sourceCode: requiredElement('#sourceCode', HTMLOListElement),
  sourceFilename: requiredElement('#sourceFilename', HTMLElement),
  status: requiredElement('#status', HTMLElement),
  structuredForm: requiredElement('#structured-panel', HTMLFormElement),
  templateInputs: [...document.querySelectorAll<HTMLInputElement>('input[name="template"]')],
  timeMetric: requiredElement('#timeMetric', HTMLElement),
}

function modeFromHash(): ModeName {
  return globalThis.location.hash === '#html-to-pdf' ? 'html' : 'structured'
}

let currentMode: ModeName = modeFromHash()
let fontPromise: Promise<Uint8Array> | undefined
let imagePromise: Promise<Uint8Array> | undefined
const previews: Partial<Record<ModeName, PreviewState>> = {}

elements.htmlInput.value = defaultHtml
elements.cssInput.value = defaultCss

function selectedTemplate(): TemplateName {
  const selected = elements.templateInputs.find((input) => input.checked)?.value
  return selected === 'report' || selected === 'catalog' ? selected : 'invoice'
}

function selectedDelivery(): DeliveryMode {
  return elements.deliveryMode.value === 'stream' ? 'stream' : 'bytes'
}

function safeText(input: HTMLInputElement, fallback: string): string {
  return input.value.trim() || fallback
}

function selectedRowCount(): number {
  const value = Number.parseInt(elements.rowCount.value, 10)
  return Number.isFinite(value) ? Math.max(1, value) : 12
}

function setStatus(message: string, error = false): void {
  elements.status.textContent = message
  elements.status.classList.toggle('is-error', error)
}

function htmlRuntimeReady(): boolean {
  return true
}

function setBusy(busy: boolean): void {
  for (const action of elements.generateActions) {
    action.disabled = busy
  }
}

function refreshRuntimeState(): void {
  const htmlReady = htmlRuntimeReady()
  elements.runtimeBadge.textContent = 'both renderers ready'
  elements.runtimeBadge.classList.remove('is-partial')
  elements.runtimeDetail.textContent = 'Both renderers use threadless WASM and run without cross-origin isolation.'
  elements.htmlRuntimeNotice.textContent =
    'The threadless runtime is ready. HTML, CSS, and font data stay in this browser.'
  elements.htmlRuntimeNotice.classList.add('is-ready')
  const htmlButton = elements.htmlForm.querySelector<HTMLButtonElement>('.generate-action')
  if (htmlButton) {
    htmlButton.disabled = !htmlReady
  }
}

async function fetchBytes(url: string, label: string): Promise<Uint8Array> {
  const response = await fetch(url, { cache: 'force-cache' })
  if (!response.ok) {
    throw new Error(`Unable to load ${label}: HTTP ${response.status}`)
  }
  return new Uint8Array(await response.arrayBuffer())
}

async function loadFont(): Promise<Uint8Array> {
  fontPromise ??= fetchBytes(tuffyFontUrl, 'Tuffy.ttf')
  return fontPromise
}

async function loadBrandImage(): Promise<Uint8Array> {
  imagePromise ??= fetchBytes(brandImageUrl, 'the catalog image')
  return imagePromise
}

async function createSampleContext(template: TemplateName): Promise<SampleContext> {
  const font = await loadFont()
  return {
    accent: elements.accentColor.value,
    company: safeText(elements.companyName, 'Crab Labs'),
    font,
    image: template === 'catalog' ? await loadBrandImage() : undefined,
    recipient: safeText(elements.recipientName, 'Flash team'),
    rowCount: selectedRowCount(),
  }
}

function concatChunks(chunks: readonly Uint8Array[]): Uint8Array {
  const total = chunks.reduce((sum, chunk) => sum + chunk.byteLength, 0)
  const result = new Uint8Array(total)
  let offset = 0
  for (const chunk of chunks) {
    result.set(chunk, offset)
    offset += chunk.byteLength
  }
  return result
}

async function consumeOutput(output: PdfOutput, mode: DeliveryMode): Promise<{ bytes: Uint8Array; chunks: number }> {
  if (mode === 'bytes') {
    return { bytes: await output.bytes(), chunks: 1 }
  }

  const reader = output.stream({ chunkSize: 4096 }).getReader()
  const chunks: Uint8Array[] = []
  try {
    for (;;) {
      const result = await reader.read()
      if (result.done) {
        break
      }
      chunks.push(result.value)
    }
  } finally {
    reader.releaseLock()
  }
  return { bytes: concatChunks(chunks), chunks: chunks.length }
}

function pdfBlob(bytes: Uint8Array): Blob {
  const owned = new Uint8Array(bytes.byteLength)
  owned.set(bytes)
  return new Blob([owned.buffer], { type: 'application/pdf' })
}

function displayPreview(mode: ModeName): void {
  const preview = previews[mode]
  if (!preview) {
    elements.pdfPreview.src = 'about:blank'
    elements.downloadLink.href = '#'
    elements.downloadLink.classList.add('is-disabled')
    elements.openButton.disabled = true
    elements.outputCard.classList.remove('has-pdf')
    elements.outputTitle.textContent = mode === 'html' ? 'HTML/CSS report' : defaultTitles[selectedTemplate()]
    elements.sizeMetric.textContent = '-'
    elements.timeMetric.textContent = '-'
    elements.deliveryMetric.textContent = '-'
    return
  }

  elements.pdfPreview.src = preview.url
  elements.downloadLink.href = preview.url
  elements.downloadLink.download = preview.filename
  elements.downloadLink.classList.remove('is-disabled')
  elements.openButton.disabled = false
  elements.outputCard.classList.add('has-pdf')
  elements.outputTitle.textContent = preview.title
  elements.sizeMetric.textContent = preview.size
  elements.timeMetric.textContent = preview.time
  elements.deliveryMetric.textContent = preview.delivery
}

function savePreview(mode: ModeName, blob: Blob, preview: Omit<PreviewState, 'url'>): void {
  const previous = previews[mode]
  if (previous) {
    URL.revokeObjectURL(previous.url)
  }
  previews[mode] = { ...preview, url: URL.createObjectURL(blob) }
  if (currentMode === mode) {
    displayPreview(mode)
  }
}

async function buildStructuredExample(template: TemplateName): Promise<RenderedExample> {
  const context = await createSampleContext(template)
  return template === 'invoice'
    ? buildInvoice(context)
    : template === 'report'
      ? buildReport(context)
      : buildCatalog(context)
}

async function generateStructured(): Promise<void> {
  setBusy(true)
  const template = selectedTemplate()
  const delivery = selectedDelivery()
  const startedAt = performance.now()
  setStatus(`Preparing the ${template} sample...`)

  try {
    const example = await buildStructuredExample(template)
    const result = await consumeOutput(example.output, delivery)
    const elapsed = performance.now() - startedAt
    savePreview('structured', pdfBlob(result.bytes), {
      delivery: delivery === 'stream' ? `${result.chunks} chunks` : 'bytes()',
      filename: example.filename,
      size: `${(result.bytes.byteLength / 1024).toFixed(1)} KB`,
      time: `${elapsed.toFixed(1)} ms`,
      title: example.title,
    })
    setStatus(`${example.title} generated locally: ${result.bytes.byteLength.toLocaleString('en-US')} bytes.`)
  } catch (error) {
    setStatus(error instanceof Error ? error.message : 'Unable to generate the structured PDF.', true)
  } finally {
    setBusy(false)
  }
}

function toBase64(bytes: Uint8Array): string {
  let binary = ''
  for (const byte of bytes) {
    binary += String.fromCodePoint(byte)
  }
  return btoa(binary)
}

function isHtmlPdfBinding(value: unknown): value is HtmlPdfBinding {
  return typeof value === 'object' && value !== null && typeof Reflect.get(value, 'createPdfFromHtml') === 'function'
}

async function loadHtmlBinding(): Promise<HtmlPdfBinding> {
  const module = await import('html-to-pdf-crab-js/browser.js')
  if (!isHtmlPdfBinding(module)) {
    throw new TypeError('The browser binding did not export createPdfFromHtml().')
  }
  return module
}

function selectedHtmlPageSize(): 'A3' | 'A4' | 'LETTER' {
  const { value } = elements.pageSize
  return value === 'A3' || value === 'LETTER' ? value : 'A4'
}

async function generateHtmlPdf(): Promise<void> {
  setBusy(true)
  const startedAt = performance.now()
  setStatus('Rendering HTML, CSS, and the TTF font in threadless WASM...')

  try {
    const [binding, font] = await Promise.all([loadHtmlBinding(), loadFont()])
    const bytes = await binding.createPdfFromHtml({
      css: elements.cssInput.value,
      fonts: [toBase64(font)],
      html: `<!doctype html><html><head><meta charset="UTF-8"></head><body>${elements.htmlInput.value}</body></html>`,
      page: {
        margin: Math.max(0, Number(elements.pageMargin.value) || 0),
        size: selectedHtmlPageSize(),
      },
      systemFonts: false,
      tagged: true,
      title: 'Crab HTML report',
    })
    const elapsed = performance.now() - startedAt
    savePreview('html', pdfBlob(bytes), {
      delivery: 'Promise<bytes>',
      filename: 'html-to-pdf-crab-report.pdf',
      size: `${(bytes.byteLength / 1024).toFixed(1)} KB`,
      time: `${elapsed.toFixed(1)} ms`,
      title: 'HTML/CSS report',
    })
    setStatus(`HTML report generated locally: ${bytes.byteLength.toLocaleString('en-US')} bytes.`)
  } catch (error) {
    setStatus(error instanceof Error ? error.message : 'Unable to render the HTML document.', true)
  } finally {
    setBusy(false)
  }
}

function refreshHtmlPreview(): void {
  elements.htmlPreviewFrame.srcdoc = `<!doctype html><html><head><meta charset="UTF-8"><style>${elements.cssInput.value}</style></head><body>${elements.htmlInput.value}</body></html>`
}

function renderSource(source: string): void {
  elements.sourceCode.replaceChildren()
  for (const line of source.trim().split('\n')) {
    const item = document.createElement('li')
    const code = document.createElement('code')
    code.textContent = line || ' '
    item.append(code)
    elements.sourceCode.append(item)
  }
}

function refreshTemplate(): void {
  const template = selectedTemplate()
  for (const input of elements.templateInputs) {
    input.closest('.template-card')?.classList.toggle('is-selected', input.checked)
  }
  const sourceFile = sourceFiles[template]
  elements.sourceFilename.textContent = sourceFile.filename
  renderSource(sourceFile.source)
  if (currentMode === 'structured' && !previews.structured) {
    elements.outputTitle.textContent = defaultTitles[template]
  }
}

function switchMode(mode: ModeName, updateHash = false): void {
  currentMode = mode
  for (const tab of elements.modeTabs) {
    const active = tab.dataset.mode === mode
    tab.classList.toggle('is-active', active)
    tab.setAttribute('aria-selected', String(active))
  }
  for (const panel of elements.modePanels) {
    const active = panel.dataset.panel === mode
    panel.classList.toggle('is-active', active)
    panel.hidden = !active
  }
  if (updateHash) {
    globalThis.history.pushState(null, '', mode === 'html' ? '#html-to-pdf' : '#pdf-crab-js')
  }
  displayPreview(mode)
  const preview = previews[mode]
  setStatus(
    preview
      ? `${preview.title} is ready in the local preview.`
      : mode === 'structured'
        ? 'The structured API is ready. Its single-thread WASM build needs no cross-origin isolation.'
        : htmlRuntimeReady()
          ? 'Edit the live HTML or CSS, then render it with the threadless WASM engine.'
          : 'The HTML renderer is ready without cross-origin isolation.',
    mode === 'html' && !htmlRuntimeReady(),
  )
}

for (const tab of elements.modeTabs) {
  tab.addEventListener('click', () => switchMode(tab.dataset.mode === 'html' ? 'html' : 'structured', true))
}

for (const input of elements.templateInputs) {
  input.addEventListener('change', refreshTemplate)
}

for (const tab of elements.editorTabs) {
  tab.addEventListener('click', () => {
    const { editor } = tab.dataset
    for (const candidate of elements.editorTabs) {
      candidate.classList.toggle('is-active', candidate === tab)
    }
    for (const panel of elements.editorPanels) {
      const active = panel.dataset.editorPanel === editor
      panel.classList.toggle('is-active', active)
      panel.hidden = !active
    }
  })
}

elements.copySourceButton.addEventListener('click', () => {
  const { source } = sourceFiles[selectedTemplate()]
  globalThis.navigator.clipboard
    .writeText(source)
    .then(() => {
      elements.copySourceButton.textContent = 'Copied'
      globalThis.setTimeout(() => {
        elements.copySourceButton.textContent = 'Copy source'
      }, 1500)
    })
    .catch((error: unknown) => setStatus(error instanceof Error ? error.message : 'Unable to copy source.', true))
})
elements.structuredForm.addEventListener('submit', (event) => {
  event.preventDefault()
  generateStructured().catch((error: unknown) => setStatus(String(error), true))
})
elements.htmlForm.addEventListener('submit', (event) => {
  event.preventDefault()
  generateHtmlPdf().catch((error: unknown) => setStatus(String(error), true))
})
elements.htmlInput.addEventListener('input', refreshHtmlPreview)
elements.cssInput.addEventListener('input', refreshHtmlPreview)
elements.openButton.addEventListener('click', () => {
  const preview = previews[currentMode]
  if (preview) {
    globalThis.open(preview.url, '_blank', 'noopener,noreferrer')
  }
})
globalThis.addEventListener('hashchange', () => switchMode(modeFromHash()))
globalThis.addEventListener('beforeunload', () => {
  for (const preview of Object.values(previews)) {
    if (preview) {
      URL.revokeObjectURL(preview.url)
    }
  }
})

refreshRuntimeState()
refreshTemplate()
refreshHtmlPreview()
switchMode(currentMode)
if (currentMode === 'structured') {
  generateStructured().catch((error: unknown) => setStatus(String(error), true))
}
