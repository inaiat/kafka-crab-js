# pdf-crab-js

Structured PDF generation for Node.js and browser/WASM. The 1.0 API is deliberately small and
typed for invoices, reports, tables, images, and other structured documents.

## Install

```bash
npm install pdf-crab-js
```

## One output contract

`renderPdf` and `PdfDocument.render()` return a lazy, one-shot `PdfOutput`. It is shared by Node
and browser, and its native serializer is pull-driven:

```ts
import { writeFile } from 'node:fs/promises'
import { renderPdf } from 'pdf-crab-js'

const output = renderPdf({
  title: 'Invoice',
  unit: 'mm',
  pages: [{ elements: [{ type: 'text', text: 'Invoice #42' }] }],
})

await writeFile('invoice.pdf', await output.bytes())
```

`bytes()` and `blob()` are asynchronous. `stream({ chunkSize, signal })` returns a Web
`ReadableStream`; `for await (const chunk of output)` is available in both runtimes. An output can
only be consumed once. Reuse fails with `PdfError` code `PDF_OUTPUT_USED`; cancellation releases
the native serializer. A stream emits the PDF header on its first pull, before layout starts, and
does not start native work until the next pull. Page content streams and embedded font programs are
Flate-compressed; the serializer releases a page buffer after emitting it instead of retaining a
second full-PDF buffer. The native boundary is deliberately pull-driven (`nextChunk()`), rather
than retaining a JavaScript callback; this follows the NAPI-RS callback lifetime guidance in
[Functions and Callbacks](https://napi.rs/blog/function-and-callbacks) and keeps backpressure
identical in Node and WASM.

## Fluent documents

```ts
import { PdfDocument } from 'pdf-crab-js'

const document = new PdfDocument({ size: 'A4', unit: 'mm', margin: 20 })
document
  .font('HelveticaBold')
  .fontSize(18)
  .text('Invoice')
  .font('Helvetica')
  .fontSize(11)
  .text('Generated with pdf-crab-js')
  .table({
    columns: [
      { key: 'item', header: 'Item', width: '*' },
      { key: 'total', header: 'Total', width: 30, align: 'right' },
    ],
    rows: [{ item: 'Subscription', total: 42 }],
  })

const output = document.render()
```

The first page is A4 portrait, uses `mm`, and has physical 20 mm margins by default. `addPage()`
inherits the document defaults; automatic page breaks inherit the effective size, orientation, and
margins of the current page. Coordinates use a top-left origin. Font sizes, line heights, and
stroke widths remain PDF points. Every mutating method fails with `PDF_DOCUMENT_FINISHED` after
`render()`.

Text without `x`/`y` flows from the cursor, wraps to the useful width, preserves paragraphs, and
automatically paginates. Absolute placement requires both coordinates; alignment and a height
require a width. Use `overflow: 'clip' | 'ellipsis' | 'paginate'` for bounded text boxes.
Wrapping, alignment, justification, auto table widths, and ellipsis use font metrics rather than
character-count estimates.

## Fonts and Unicode

```ts
const document = new PdfDocument()
document
  .registerFont('Invoice', new URL('./Invoice.ttf', import.meta.url), { fallback: 'Helvetica' })
  .font('Invoice')
  .text('Ação, café e coração')

const declarative = renderPdf({
  fonts: [{ family: 'Invoice', source: fontBytes, fallback: 'Helvetica' }],
  pages: [{ elements: [{ type: 'text', text: 'Relatório final', font: 'Invoice' }] }],
})
```

`registerFont(family, source, { weight, style, fallback })` validates TTF/OTF inputs. Node accepts
bytes, paths, and file URLs; browsers accept `Uint8Array`/`ArrayBuffer`. Custom fonts are shaped,
subset, embedded as CID fonts, and receive a `ToUnicode` map for reliable extraction. Standard
Latin fonts use AFM/WinAnsi metrics and encoding. Fallback is explicit and applied only to missing
glyph runs; otherwise rendering fails with `PDF_MISSING_GLYPH` instead of corrupting text.

The 1.0 release covers Latin text, including Portuguese accents. Arabic/RTL and other bidi-heavy
scripts remain intentionally outside this delivery until end-to-end shaping, ordering, extraction,
and visual fixtures can be guaranteed together.

## Tables

`table<Row>()` accepts typed keys or value functions, fixed/`auto`/`*` widths, formatters,
alignment, cell styles, padding, borders, backgrounds, stripes, repeated headers,
and `rowSplit: 'avoid' | 'split'`. The cursor advances after the table, so another paragraph or
table continues naturally.

## Browser

```ts
import { renderPdf } from 'pdf-crab-js/browser'

const blob = await renderPdf({ pages: [{ elements: [{ type: 'text', text: 'WASM' }] }] }).blob()
```

The default browser build is single-threaded and needs no `SharedArrayBuffer`, COOP, or COEP.
Node-only image paths are rejected in the browser; pass `Uint8Array` or `ArrayBuffer`. A threaded
WASM build is available as `pdf-crab-js/browser/threaded` for isolated deployments while keeping
the same API; that optional subpath requires the usual cross-origin isolation setup.

## Errors and scope

Failures use `PdfError { code, path, cause }`. Invalid arguments are rejected at the nearest API
call. 1.0 focuses on structured PDFs; forms, encryption, PDF/A, accessibility tags, outlines,
SVG paths, color fonts, and a PDFKit compatibility adapter are outside this contract.

## Migration

See [MIGRATION.md](./MIGRATION.md) for the breaking 0.3/draft migration. There are no legacy
`createPdf*`, `finish*`, or provisional stream aliases in the public core.
