import { readFileSync } from 'node:fs'
import { equal, match, ok, throws } from 'node:assert/strict'
import { test } from 'vite-plus/test'

import * as pdfCrab from '../../dist/index.js'
import { PdfDocument, PdfError, renderPdf, type PdfDocumentInput } from '../../dist/index.js'

function assertPdfBuffer(pdf: Uint8Array): void {
  ok(pdf instanceof Uint8Array)
  const bytes = Buffer.from(pdf)
  equal(bytes.subarray(0, 5).toString('utf8'), '%PDF-')
  equal(bytes.toString('utf8').trimEnd().endsWith('%%EOF'), true)
}

async function collectChunks(chunks: AsyncIterable<Uint8Array>): Promise<Buffer> {
  const buffers: Buffer[] = []
  for await (const chunk of chunks) {
    ok(chunk instanceof Uint8Array)
    ok(chunk.byteLength > 0)
    buffers.push(Buffer.from(chunk))
  }
  return Buffer.concat(buffers)
}

async function* outputForIteration(input: PdfDocumentInput): AsyncIterable<Uint8Array> {
  for await (const chunk of renderPdf(input)) yield chunk
}

const imagePath = new URL('assets/test-image.png', import.meta.url)
const imageBytes = readFileSync(imagePath)
const fontPath = new URL('assets/Tuffy.ttf', import.meta.url)
const fontBytes = readFileSync(fontPath)

test('public API exposes the unified render contract', () => {
  const publicApi = pdfCrab as Record<string, unknown>
  equal(typeof renderPdf, 'function')
  equal(typeof PdfDocument, 'function')
  equal(typeof PdfError, 'function')
  equal(publicApi.createPdf, undefined)
  equal(publicApi.createPdfStream, undefined)
})

test('PdfDocument uses A4/mm/20mm defaults and returns lazy bytes', async () => {
  const document = new PdfDocument()
  document.fillColor('#111111').fontSize(14).text('Hello from pdf-crab-js').moveDown().rect(20, 50, 40, 20).fill()

  const output = document.render()
  equal(output.used, false)
  const pdf = await output.bytes()

  assertPdfBuffer(pdf)
  match(Buffer.from(pdf).toString('latin1'), /\/Filter \/FlateDecode/)
  equal(output.used, true)
  equal(document.x, 20)
  ok(document.y > 25)
  await output.bytes().then(
    () => ok(false, 'second consumption should fail'),
    (error: unknown) => equal((error as PdfError).code, 'PDF_OUTPUT_USED'),
  )
})

test('PdfDocument supports paths, links, pages, and mutation guards', async () => {
  const document = new PdfDocument({ unit: 'pt', size: [200, 200], margin: 12 })
  document
    .moveTo(20, 20)
    .lineTo(100, 20)
    .lineTo(100, 80)
    .closePath()
    .fillAndStroke()
    .link('https://example.com', { x: 20, y: 90, width: 40, height: 12 })
    .addPage({ size: 'A4' })
    .text('second page')

  const pdf = await document.render().bytes()
  assertPdfBuffer(pdf)
  const body = Buffer.from(pdf).toString('latin1')
  match(body, /https:\/\/example\.com/)
  match(body, /\/Count 2/)
  throws(
    () => document.text('after render'),
    (error: unknown) => (error as PdfError).code === 'PDF_DOCUMENT_FINISHED',
  )
})

test('flow text, tables, images, and declarative pages are supported', async () => {
  const document = new PdfDocument({ unit: 'mm', size: 'LETTER', layout: 'landscape' })
  document
    .text('before')
    .image(imageBytes, { fit: [60, 40], align: 'center' })
    .table({
      columns: [
        { key: 'name', header: 'Name', width: '*' },
        { key: 'amount', header: 'Amount', width: 30, align: 'right' },
      ],
      rows: [{ name: 'Invoice', amount: 42 }],
    })
  const pdf = await document.render().bytes()
  assertPdfBuffer(pdf)
  const body = Buffer.from(pdf).toString('latin1')
  match(body, /\/Subtype \/Image/)
  match(body, /\/BaseFont \/Helvetica-Bold/)

  const input: PdfDocumentInput = {
    unit: 'pt',
    pages: [{ size: [300, 300], elements: [{ type: 'text', text: 'Mixed elements', x: 40, y: 32, fontSize: 16 }] }],
  }
  assertPdfBuffer(await renderPdf(input).bytes())

  const declarativeFlow = await renderPdf({
    unit: 'pt',
    pages: [{ size: [220, 220], elements: [{ type: 'text', text: 'flow without coordinates', width: 100 }] }],
  }).bytes()
  assertPdfBuffer(declarativeFlow)
})

test('font registration embeds a Unicode subset with ToUnicode', async () => {
  const document = new PdfDocument({ unit: 'pt', size: [200, 200], margin: 10 })
  document.registerFont('InvoiceFont', fontPath, { fallback: 'Helvetica' }).font('InvoiceFont')
  const pdf = await document.text('Ação, café, coração e € 42,00', { width: 180 }).render().bytes()
  assertPdfBuffer(pdf)
  const body = Buffer.from(pdf).toString('latin1')
  match(body, /\/Subtype \/Type0/)
  match(body, /\/FontFile2/)
  match(body, /\/ToUnicode/)
  ok(pdf.byteLength < fontBytes.byteLength, 'the embedded font should be subset')

  throws(() => new PdfDocument().registerFont('Invalid', new Uint8Array([0, 1, 0, 0])), /not a valid TTF or OTF font/)

  await renderPdf({ pages: [{ elements: [{ type: 'text', text: 'مرحبا', x: 10, y: 10 }] }] })
    .bytes()
    .then(
      () => ok(false, 'missing glyph should reject'),
      (error: unknown) => equal((error as PdfError).code, 'PDF_MISSING_GLYPH'),
    )
})

test('declarative documents share custom fonts, fallback, and metric pagination', async () => {
  const pdf = await renderPdf({
    unit: 'pt',
    fonts: [{ family: 'ReportFont', source: fontPath, fallback: 'Helvetica' }],
    pages: [
      {
        size: [240, 240],
        elements: [
          { type: 'text', text: 'Relatório\u00a0final: ação e café', width: 110, font: 'ReportFont' },
          { type: 'text', text: Array.from({ length: 180 }, () => 'WWW iii').join(' '), width: 110 },
        ],
      },
    ],
  }).bytes()
  assertPdfBuffer(pdf)
  const body = Buffer.from(pdf).toString('latin1')
  match(body, /\/Subtype \/Type0/)
  match(body, /\/BaseFont \/Helvetica/)
  const pageCount = Number(/\/Count (?<count>\d+)/.exec(body)?.groups?.count ?? 0)
  ok(pageCount > 1, 'flowing declarative text should paginate using measured lines')
  equal((body.match(/\/MediaBox \[0 0 240 240\]/g) ?? []).length, pageCount)

  await renderPdf({
    unit: 'pt',
    fonts: [{ family: 'NoFallback', source: fontBytes }],
    pages: [{ elements: [{ type: 'text', text: 'A\u00a0B', width: 100, font: 'NoFallback' }] }],
  })
    .bytes()
    .then(
      () => ok(false, 'a missing custom-font glyph must reject without fallback'),
      (error: unknown) => equal((error as PdfError).code, 'PDF_MISSING_GLYPH'),
    )
})

test('stream, bytes, and async iteration are deterministic and pull-driven', async () => {
  const input: PdfDocumentInput = {
    unit: 'pt',
    pages: [{ size: [180, 180], elements: [{ type: 'text', text: 'streamed PDF', x: 20, y: 20 }] }],
  }
  const expected = await renderPdf(input).bytes()
  const output = renderPdf(input)
  const streamed = await new Response(output.stream({ chunkSize: 17 })).arrayBuffer()
  equal(Buffer.from(streamed).compare(Buffer.from(expected)), 0)

  const iterated = await collectChunks(outputForIteration(input))
  equal(iterated.compare(Buffer.from(expected)), 0)

  const headerOnly = renderPdf(input)
  const reader = headerOnly.stream({ chunkSize: 5 }).getReader()
  equal(headerOnly.used, false)
  const first = await reader.read()
  ok(first.value)
  equal(Buffer.from(first.value).toString('ascii'), '%PDF-')
  equal(headerOnly.used, true)
  await reader.cancel()
})

test('AbortSignal cancels a lazy output', async () => {
  const controller = new AbortController()
  const output = renderPdf({ pages: [{ elements: [{ type: 'text', text: 'cancel me' }] }] })
  controller.abort()
  await output.bytes({ signal: controller.signal }).then(
    () => ok(false, 'aborted output should reject'),
    (error: unknown) => equal((error as PdfError).code, 'PDF_ABORTED'),
  )
  equal(output.used, true)

  const afterHeader = new AbortController()
  const streamed = renderPdf({ pages: [{ elements: [{ type: 'text', text: 'abort after header' }] }] })
  const reader = streamed.stream({ signal: afterHeader.signal }).getReader()
  const first = await reader.read()
  ok(first.value)
  equal(Buffer.from(first.value).subarray(0, 5).toString('ascii'), '%PDF-')
  afterHeader.abort()
  await reader.read().then(
    () => ok(false, 'aborted stream should reject before native serialization'),
    (error: unknown) => equal((error as PdfError).code, 'PDF_ABORTED'),
  )
})

test('invalid combinations fail at the nearest call site', () => {
  const document = new PdfDocument()
  // @ts-expect-error alignment without a width is invalid by construction.
  throws(() => document.text('invalid', { align: 'center' }), /text\.align requires text\.width/)
  // @ts-expect-error x/y are intentionally incomplete for runtime validation.
  throws(() => document.text('invalid', { x: 10 }), /text\.x and text\.y must be provided together/)
  throws(
    () => renderPdf({ pages: [{ elements: [] }] }).stream({ chunkSize: 0 }),
    /chunkSize must be a positive integer/,
  )
  document.moveTo(10, 10).lineTo(20, 20)
  throws(() => document.render(), /unpainted path/)
})

test('every mutating PdfDocument method rejects after render', () => {
  const document = new PdfDocument()
  document.text('sealed').render()
  const mutations: (() => unknown)[] = [
    () => document.addPage(),
    () => document.text('x'),
    () => document.textBox('x', { width: 40 }),
    () => document.font('Helvetica'),
    () => document.fontSize(10),
    () => document.fillColor('#000'),
    () => document.strokeColor('#000'),
    () => document.lineWidth(1),
    () => document.save(),
    () => document.restore(),
    () => document.moveDown(),
    () => document.moveUp(),
    () => document.moveTo(1, 1),
    () => document.lineTo(2, 2),
    () => document.closePath(),
    () => document.rect(1, 1, 2, 2),
    () => document.fill(),
    () => document.stroke(),
    () => document.fillAndStroke(),
    () => document.image(imageBytes),
    () => document.table({ columns: [{ key: 'value' }], rows: [{ value: 1 }] }),
    () => document.link('https://example.com', { x: 1, y: 1, width: 2, height: 2 }),
    () => document.registerFont('LateFont', fontBytes),
    () => document.render(),
  ]
  for (const mutate of mutations) {
    throws(mutate, (error: unknown) => (error as PdfError).code === 'PDF_DOCUMENT_FINISHED')
  }
})
