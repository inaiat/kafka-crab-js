# pdf-crab-js 1.0 Guide

`pdf-crab-js` generates structured PDFs with a Rust renderer and a typed TypeScript API. It runs
as a native Node.js addon or through the explicit browser/WASM entrypoint. Use it for invoices,
reports, statements, labels, images, and paginated tables without running a browser.

Use `html-to-pdf-crab-js` instead when HTML and CSS are the source of truth.

## Installation

The native entrypoint requires Node.js 24.

```bash
npm install pdf-crab-js
```

## Choose an API

The two public APIs share the same renderer and lazy `PdfOutput` contract:

- `renderPdf(input)` is declarative and works well for typed templates and explicit pages.
- `PdfDocument` is fluent and adds cursor-based text, typed tables, paths, images, and links.

### Fluent document

```ts
import { writeFile } from 'node:fs/promises'
import { PdfDocument } from 'pdf-crab-js'

type InvoiceRow = {
  description: string
  quantity: number
  total: number
}

const document = new PdfDocument({
  title: 'Invoice 42',
  unit: 'mm',
  size: 'A4',
  margin: 20,
})

document
  .font('HelveticaBold')
  .fontSize(20)
  .text('Invoice #42')
  .moveDown()
  .font('Helvetica')
  .fontSize(10)
  .table<InvoiceRow>({
    columns: [
      { key: 'description', header: 'Description', width: '*' },
      { key: 'quantity', header: 'Qty', width: 22, align: 'right' },
      {
        key: 'total',
        header: 'Total',
        width: 32,
        align: 'right',
        formatter: (value) => `$ ${Number(value).toFixed(2)}`,
      },
    ],
    rows: [{ description: 'Pro plan', quantity: 1, total: 42 }],
    repeatHeader: true,
    stripe: '#f8fafc',
  })

await writeFile('invoice.pdf', await document.render().bytes())
```

### Declarative document

```ts
import { renderPdf, type PdfDocumentInput } from 'pdf-crab-js'

const report = {
  title: 'Report',
  unit: 'mm',
  pages: [
    {
      size: 'A4',
      elements: [
        { type: 'rect', x: 20, y: 20, width: 170, height: 30, fill: '#eff6ff' },
        {
          type: 'text',
          text: 'Declarative report',
          x: 28,
          y: 30,
          font: 'HelveticaBold',
          fontSize: 18,
        },
      ],
    },
  ],
} satisfies PdfDocumentInput

const bytes = await renderPdf(report).bytes()
```

## Output and streaming

`renderPdf(input)` and `document.render()` return a lazy, single-use `PdfOutput`:

```ts
interface PdfOutput extends AsyncIterable<Uint8Array> {
  readonly used: boolean
  bytes(options?: { signal?: AbortSignal }): Promise<Uint8Array>
  blob(options?: { signal?: AbortSignal }): Promise<Blob>
  stream(options?: { chunkSize?: number; signal?: AbortSignal }): ReadableStream<Uint8Array>
}
```

Choose exactly one consumption method. A second attempt fails with `PDF_OUTPUT_USED`.

```ts
import { createWriteStream } from 'node:fs'
import { Readable } from 'node:stream'
import { pipeline } from 'node:stream/promises'

const output = document.render()
await pipeline(Readable.from(output.stream()), createWriteStream('report.pdf'))
```

The stream is pull-driven and supports cancellation through `AbortSignal`. Use `bytes()` for small
HTTP responses and `stream()` for large documents.

## Pages and text

The fluent defaults are A4 portrait, `mm`, and physical 20 mm margins. Coordinates use a top-left
origin. Dimensions follow the document unit, while font sizes, line heights, and stroke widths use
PDF points.

Text without `x` and `y` flows from the cursor, wraps to the available width, and paginates.
Absolute placement requires both coordinates. Alignment and bounded height require `width`.

Automatic page breaks preserve the current page's effective size, layout, and margins.

## Fonts and images

Built-in Times, Helvetica/Arial, Courier, Symbol, and ZapfDingbats variants are available. Register
a TTF or OTF font for additional Latin glyph coverage:

```ts
document
  .registerFont('Report', new URL('./Report.ttf', import.meta.url), { fallback: 'Helvetica' })
  .font('Report')
  .text('Ação, café e coração')
```

Custom fonts are subset and embedded with `ToUnicode`. Missing glyphs fail with
`PDF_MISSING_GLYPH` instead of silently corrupting text.

PNG and JPEG images accept bytes in every runtime. The Node.js entrypoint also accepts file paths.

## Browser and WASM

Browser applications must use the explicit entrypoint and pass image/font bytes:

```ts
import { renderPdf } from 'pdf-crab-js/browser'

const blob = await renderPdf({
  pages: [{ elements: [{ type: 'text', text: 'Browser PDF' }] }],
}).blob()
```

The default build is single-threaded and requires no `SharedArrayBuffer`, COOP, or COEP. Isolated
deployments can opt into `pdf-crab-js/browser/threaded` for the threaded build.

Run the [WASM sample studio](../../examples/wasm-samples/README.md) for live structured-PDF and
HTML-to-PDF previews with the TypeScript source displayed next to each sample.

## Errors

Public renderer and validation failures use `PdfError { code, path, cause }`:

```ts
import { PdfError } from 'pdf-crab-js'

try {
  await output.bytes()
} catch (error) {
  if (error instanceof PdfError) {
    console.error(error.code, error.path, error.message)
  }
}
```

The stable codes are `PDF_OUTPUT_USED`, `PDF_DOCUMENT_FINISHED`, `PDF_INVALID_ARGUMENT`,
`PDF_UNSUPPORTED_FORMAT`, `PDF_FONT_NOT_FOUND`, `PDF_MISSING_GLYPH`, `PDF_LAYOUT_ERROR`, and
`PDF_ABORTED`.

## 1.0 scope and references

Version 1.0 focuses on structured documents. Forms, encryption, PDF/A, accessibility tags,
outlines, SVG paths/input, color fonts, and a PDFKit compatibility adapter are outside this release.

- [Complete API reference](../../packages/pdf-crab-js/README.md)
- [Migration from 0.3 and the draft API](../../packages/pdf-crab-js/MIGRATION.md)
- [Runnable Node.js and browser examples](../../examples/pdf-crab-js/README.md)
- [Benchmark methodology and full results](../../benchmarks/pdf/README.md)
