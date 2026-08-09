import { renderPdf, type PdfDocumentInput } from 'pdf-crab-js/browser.js'
import { Buffer as BrowserBuffer } from 'buffer'
import '../wasm/browser.css'

type ElementConstructor<T extends Element> = new (...args: never[]) => T

globalThis.Buffer ??= BrowserBuffer

const documentInput: PdfDocumentInput = {
  metadata: {
    creator: 'pdf-crab-js stream browser example',
    producer: 'pdf-crab-js',
    title: 'pdf-crab-js stream browser example',
  },
  title: 'pdf-crab-js stream browser example',
  unit: 'mm',
  pages: [
    {
      size: 'A4',
      elements: [
        { type: 'rect', x: 20, y: 20, width: 170, height: 44, fill: '#fff7ed', stroke: '#ea580c' },
        {
          type: 'text',
          text: 'Browser/WASM stream',
          x: 30,
          y: 33,
          font: 'HelveticaBold',
          fontSize: 18,
          fill: '#9a3412',
        },
        {
          type: 'text',
          text: 'The browser consumes AsyncIterable<Uint8Array> chunks directly.',
          x: 30,
          y: 46,
          fontSize: 10,
          fill: '#7c2d12',
        },
      ],
    },
  ],
}

function requiredElement<T extends Element>(selector: string, constructor: ElementConstructor<T>): T {
  const element = document.querySelector(selector)

  if (!(element instanceof constructor)) {
    throw new Error(`Browser stream example markup is missing ${selector}`)
  }

  return element
}

function concatChunks(chunks: readonly Uint8Array[]): Uint8Array {
  const size = chunks.reduce((total, chunk) => total + chunk.byteLength, 0)
  const pdf = new Uint8Array(size)
  let offset = 0

  for (const chunk of chunks) {
    pdf.set(chunk, offset)
    offset += chunk.byteLength
  }

  return pdf
}

const renderButton = requiredElement('#render-pdf', HTMLButtonElement)
const downloadLink = requiredElement('#download-pdf', HTMLAnchorElement)
const inputSource = requiredElement('#input-source', HTMLPreElement)
const inputSize = requiredElement('#input-size', HTMLElement)
const pdfPreview = requiredElement('#pdf-preview', HTMLIFrameElement)
const pdfSize = requiredElement('#pdf-size', HTMLElement)
const status = requiredElement('#status', HTMLElement)

let currentPdfUrl: string | undefined

function setStatus(message: string): void {
  status.textContent = message
}

function createPdfBlob(pdf: Uint8Array): Blob {
  const arrayBuffer = new ArrayBuffer(pdf.byteLength)
  new Uint8Array(arrayBuffer).set(pdf)
  return new Blob([arrayBuffer], { type: 'application/pdf' })
}

async function renderDocument(): Promise<void> {
  renderButton.disabled = true
  setStatus('Rendering and consuming PDF chunks with the WASM package...')

  try {
    const stream = renderPdf(documentInput).stream({ chunkSize: 1024 })
    const chunks: Uint8Array[] = []

    for await (const chunk of stream) {
      if (!(chunk instanceof Uint8Array) || chunk.byteLength === 0) {
        throw new TypeError('WASI browser stream returned an invalid chunk')
      }
      chunks.push(chunk)
    }

    const pdf = concatChunks(chunks)

    if (currentPdfUrl) {
      URL.revokeObjectURL(currentPdfUrl)
    }
    currentPdfUrl = URL.createObjectURL(createPdfBlob(pdf))
    pdfPreview.src = currentPdfUrl
    pdfSize.textContent = `${pdf.byteLength.toLocaleString()} bytes · ${chunks.length} chunks`
    downloadLink.href = currentPdfUrl
    downloadLink.download = 'pdf-crab-js-stream-browser-example.pdf'
    downloadLink.setAttribute('aria-disabled', 'false')
    setStatus('Rendered with pdf-crab-js/browser.js and consumed as an async iterable.')
  } catch (error) {
    setStatus(error instanceof Error ? error.message : 'Failed to render streamed PDF')
  } finally {
    renderButton.disabled = false
  }
}

const formattedInput = JSON.stringify(documentInput, null, 2)
inputSource.textContent = formattedInput
inputSize.textContent = `${new TextEncoder().encode(formattedInput).byteLength.toLocaleString()} bytes`

renderButton.addEventListener('click', () => {
  renderDocument().catch((error: unknown) => setStatus(error instanceof Error ? error.message : 'Failed to render PDF'))
})
renderDocument().catch((error: unknown) => setStatus(error instanceof Error ? error.message : 'Failed to render PDF'))
