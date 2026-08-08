import { readFileSync } from 'node:fs'
import { equal, match, ok, throws } from 'node:assert/strict'
import { test } from 'vite-plus/test'

import * as pdfCrab from '../../dist/index.js'
import { createPdf, createPdfAsync, PdfDocument } from '../../dist/index.js'

function assertPdfBuffer(pdf: Uint8Array): void {
  ok(pdf instanceof Uint8Array)
  const bytes = Buffer.from(pdf)
  equal(bytes.subarray(0, 5).toString('utf8'), '%PDF-')
  equal(bytes.toString('utf8').trimEnd().endsWith('%%EOF'), true)
}

const imagePath = new URL('../../../../examples/pdf-crab-js/screenshots/pdf-crab-js-example.pdf.png', import.meta.url)
const imageBytes = readFileSync(imagePath)
const tinyJpegBytes = Buffer.from(
  '/9j/4AAQSkZJRgABAQAAAQABAAD/2wBDAAYEBQYFBAYGBQYHBwYIChAKCgkJChQODwwQFxQYGBcUFhYaHSUfGhsjHBYWICwgIyYnKSopGR8tMC0oMCUoKSj/2wBDAQcHBwoIChMKChMoGhYaKCgoKCgoKCgoKCgoKCgoKCgoKCgoKCgoKCgoKCgoKCgoKCgoKCgoKCgoKCgoKCgoKCgoKCj/wAARCAABAAEDASIAAhEBAxEB/8QAFQABAQAAAAAAAAAAAAAAAAAAAAf/xAAUEAEAAAAAAAAAAAAAAAAAAAAA/8QAFQEBAQAAAAAAAAAAAAAAAAAABgj/xAAUEQEAAAAAAAAAAAAAAAAAAAAA/9oADAMBAAIRAxEAPwCdABykX//Z',
  'base64',
)
const alphaPngBytes = Buffer.from(
  'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII=',
  'base64',
)

test('public API exposes PdfDocument and declarative helpers without the legacy builder', () => {
  const publicApi = pdfCrab as Record<string, unknown>

  equal(typeof createPdf, 'function')
  equal(typeof createPdfAsync, 'function')
  equal(typeof PdfDocument, 'function')
  equal(publicApi.PdfDocumentBuilder, undefined)
})

test('PdfDocument uses A4 defaults, cursor flow, styles, and Uint8Array output', () => {
  const document = new PdfDocument()
  document.fillColor('#111111').fontSize(14).text('Hello from pdf-crab-js').moveDown().rect(20, 50, 40, 20).fill()

  const pdf = document.finish()

  assertPdfBuffer(pdf)
  match(Buffer.from(pdf).toString('latin1'), /Hello from pdf-crab-js/)
  equal(document.x, 20)
  ok(Math.abs(document.y - 31.852) < 0.01)
})

test('PdfDocument supports paths, links, pages, and async finish', async () => {
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

  const pdf = await document.finishAsync()

  assertPdfBuffer(pdf)
  const body = Buffer.from(pdf).toString('latin1')
  match(body, /https:\/\/example\.com/)
  match(body, /second page/)
})

test('PdfDocument rejects unpainted paths and invalid state transitions', () => {
  const document = new PdfDocument({ size: [100, 100], unit: 'pt', margin: 10 })
  document.moveTo(10, 10).lineTo(20, 20)

  throws(() => document.finish(), /unpainted path/)
  document.discardPath().finish()
  throws(() => document.text('after finish'), /already finished/)
})

test('PdfDocument embeds PNG/JPEG-compatible image bytes, fits images, and supports Node paths', () => {
  const document = new PdfDocument({ unit: 'mm' })
  document
    .text('before')
    .image(imageBytes, { fit: [60, 40], align: 'center' })
    .image(imagePath.pathname, { width: 20 })

  const pdf = document.finish()
  const body = Buffer.from(pdf).toString('latin1')

  assertPdfBuffer(pdf)
  match(body, /\/Subtype \/Image/)
})

test('images preserve JPEG encoding and PNG transparency through a soft mask', () => {
  const jpegPdf = new PdfDocument({ unit: 'pt', size: [100, 100], margin: 0 }).image(tinyJpegBytes).finish()
  const jpegBody = Buffer.from(jpegPdf).toString('latin1')
  match(jpegBody, /\/Filter \/DCTDecode/)

  const pngPdf = new PdfDocument({ unit: 'pt', size: [100, 100], margin: 0 }).image(alphaPngBytes).finish()
  const pngBody = Buffer.from(pngPdf).toString('latin1')
  match(pngBody, /\/SMask \d+ 0 R/)
})

test('createPdf supports strict page sizes, top-left elements, images, and metadata', () => {
  const pdf = createPdf({
    title: 'Rich PDF',
    unit: 'pt',
    metadata: { author: 'pdf-crab-js', keywords: ['pdf', 'image'] },
    pages: [
      {
        size: [300, 300],
        elements: [
          { type: 'rect', x: 32, y: 80, width: 180, height: 96, fill: '#f3f4f6', stroke: '#111827', strokeWidth: 2 },
          { type: 'line', x1: 32, y1: 208, x2: 212, y2: 208, stroke: '#2563eb', strokeWidth: 1.5 },
          { type: 'text', text: 'Mixed elements', x: 40, y: 32, font: 'HelveticaBold', fontSize: 16, fill: '#111827' },
          { type: 'image', source: imageBytes, x: 20, y: 200, fit: [60, 60], align: 'center', valign: 'center' },
        ],
      },
    ],
  })

  assertPdfBuffer(pdf)
  const body = Buffer.from(pdf).toString('latin1')
  match(body, /Rich PDF/)
  match(body, /Mixed elements/)
  match(body, /\/Subtype \/Image/)
})

test('createPdfAsync returns a PDF Uint8Array', async () => {
  const pdf = await createPdfAsync({
    pages: [{ size: 'A4', elements: [{ type: 'text', text: 'Async PDF', x: 20, y: 20 }] }],
  })

  assertPdfBuffer(pdf)
  match(Buffer.from(pdf).toString('latin1'), /Async PDF/)
})

test('createPdf validates pages, sizes, element types, and image bytes', () => {
  throws(() => createPdf({ pages: [] as never }), /pages must contain at least one page/)
  throws(() => createPdf({ pages: [{ size: [0, 100] }] }), /page size width/)
  throws(
    () =>
      createPdf({
        pages: [{ size: 'A4', elements: [{ type: 'circle' } as never] }],
      }),
    /type must be one of/,
  )
  throws(
    () =>
      createPdf({
        pages: [{ size: 'A4', elements: [{ type: 'image', source: new Uint8Array([1, 2, 3]), x: 0, y: 0 } as never] }],
      }),
    /unsupported image format|image format could not be detected/,
  )
})
