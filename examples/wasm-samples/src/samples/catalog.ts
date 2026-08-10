import { renderPdf, type PdfDocumentInput, type PdfElementInput, type PdfPageInput } from 'pdf-crab-js/browser.js'

import type { RenderedExample, SampleContext } from './types.js'

function card(
  left: number,
  top: number,
  width: number,
  title: string,
  body: string,
  accent: string,
): PdfElementInput[] {
  return [
    { fill: '#f8f7fa', height: 52, stroke: '#d8d5de', strokeWidth: 0.6, type: 'rect', width, x: left, y: top },
    { fill: accent, height: 4, type: 'rect', width, x: left, y: top },
    { fill: '#231b1d', font: 'Tuffy', fontSize: 12, text: title, type: 'text', x: left + 6, y: top + 13 },
    {
      fill: '#66616e',
      font: 'Tuffy',
      fontSize: 8,
      height: 23,
      lineHeight: 10,
      overflow: 'ellipsis',
      text: body,
      type: 'text',
      width: width - 12,
      x: left + 6,
      y: top + 25,
    },
  ]
}

export function buildCatalog(context: SampleContext): RenderedExample {
  const { accent, company, font, image, recipient } = context
  if (!image) {
    throw new TypeError('The catalog sample requires image bytes.')
  }

  const cards = [
    ['Invoice engine', 'Tables, totals, links, and embedded fonts through a fluent API.'],
    ['Report builder', 'Landscape pages, automatic pagination, and repeated table headers.'],
    ['Browser WASM', 'A zero-config browser entry without SharedArrayBuffer, COOP, or COEP.'],
    ['Lazy output', 'Serialization starts only when bytes, Blob, stream, or iteration is requested.'],
    ['Typed tables', 'Key or function columns, formatters, and auto, fixed, or star widths.'],
    ['Real metrics', 'Line breaking and measurement use metrics from the selected font.'],
  ] as const
  const catalogElements: PdfElementInput[] = [
    { fill: '#231b1d', height: 42, type: 'rect', width: 297, x: 0, y: 0 },
    { fill: '#ffffff', font: 'Tuffy', fontSize: 21, text: 'Capability catalog', type: 'text', x: 16, y: 17 },
    {
      fill: '#cec9e6',
      font: 'Tuffy',
      fontSize: 8,
      text: `Prepared by ${company} for ${recipient}`,
      type: 'text',
      x: 16,
      y: 29,
    },
  ]

  for (const [index, [title, body]] of cards.entries()) {
    const column = index % 3
    const row = Math.floor(index / 3)
    catalogElements.push(...card(16 + column * 91, 55 + row * 64, 83, title, body, accent))
  }

  const pages: PdfPageInput[] = [
    {
      elements: [
        { fill: '#231b1d', height: 297, type: 'rect', width: 210, x: 0, y: 0 },
        { fill: accent, height: 297, type: 'rect', width: 7, x: 0, y: 0 },
        { source: image, type: 'image', width: 74, x: 118, y: 22 },
        { fill: '#b9b3d6', font: 'Tuffy', fontSize: 9, text: 'PDF-CRAB-JS / 1.0', type: 'text', x: 20, y: 32 },
        {
          fill: '#ffffff',
          font: 'Tuffy',
          fontSize: 31,
          height: 38,
          lineHeight: 33,
          text: 'Structured\ndocuments',
          type: 'text',
          width: 150,
          x: 20,
          y: 76,
        },
        {
          fill: '#d6d1e5',
          font: 'Tuffy',
          fontSize: 11,
          height: 50,
          lineHeight: 15,
          text: 'A focused API for invoices, reports, tables, and incremental streaming.',
          type: 'text',
          width: 128,
          x: 20,
          y: 154,
        },
        { stroke: accent, strokeWidth: 2, type: 'line', x1: 20, x2: 78, y1: 222, y2: 222 },
        { fill: '#b9b3c7', font: 'Tuffy', fontSize: 8, text: company, type: 'text', x: 20, y: 238 },
        { fill: '#b9b3c7', font: 'Tuffy', fontSize: 8, text: 'Browser WASM / no upload', type: 'text', x: 20, y: 249 },
      ],
      size: 'A4',
    },
    { elements: catalogElements, layout: 'landscape', size: 'A4' },
  ]

  const input: PdfDocumentInput = {
    fonts: [{ family: 'Tuffy', source: font }],
    metadata: {
      author: company,
      subject: 'Declarative pdf-crab-js API catalog',
      title: 'pdf-crab-js catalog',
    },
    pages,
    title: 'pdf-crab-js catalog',
    unit: 'mm',
  }

  return {
    filename: 'pdf-crab-catalog.pdf',
    output: renderPdf(input),
    title: 'Declarative catalog',
  }
}
