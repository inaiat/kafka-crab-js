# pdf-crab-js

Structured PDF generation for Node.js and WebAssembly, built with Rust, NAPI-RS, and
`pdf-writer`. Version 1.0 provides a fluent `PdfDocument` API with PDFKit-like ergonomics and a
strict declarative `createPdf` API.

Use this package for invoices, receipts, statements, labels, reports, tables, exports, and other
documents that can be expressed as pages, coordinates, text, shapes, links, and raster images. Use
[`html-to-pdf-crab-js`](../html-to-pdf-crab-js/README.md) when HTML/CSS layout is the source format.

## Install

```bash
npm install pdf-crab-js
```

The native package requires Node.js `>=22`. Browser/WASM deployments require
`SharedArrayBuffer` and cross-origin isolation.

## Fluent API

```ts
import { writeFileSync } from 'node:fs'
import { PdfDocument } from 'pdf-crab-js'

const document = new PdfDocument({
  size: 'A4',
  unit: 'mm',
  margin: 20,
})

document
  .font('HelveticaBold')
  .fontSize(18)
  .fillColor('#0f172a')
  .text('Invoice')
  .moveDown()
  .font('Helvetica')
  .fontSize(11)
  .text('Generated with pdf-crab-js')
  .strokeColor('#2563eb')
  .lineWidth(1)
  .moveTo(20, 55)
  .lineTo(190, 55)
  .stroke()

writeFileSync('invoice.pdf', document.finish())
```

The first page is created automatically. Defaults are A4 portrait, 20 mm margins, Helvetica 12,
black fill/stroke, and a 1 pt line width. Text without explicit coordinates uses the cursor and
automatically starts a new page when it reaches the bottom margin.

All public coordinates use a top-left origin and the configured `mm` or `pt` unit. PDF output is a
complete `Uint8Array`; in Node.js the returned value is also a `Buffer` without an extra copy.
Font sizes, line heights, and line widths follow PDF points, as in PDFKit; cursor movement converts
those metrics into the configured coordinate unit.

## Images

PNG and JPEG images are supported in both Node.js and browser/WASM builds:

```ts
import { readFileSync, writeFileSync } from 'node:fs'
import { PdfDocument } from 'pdf-crab-js'

const document = new PdfDocument({ unit: 'mm' })
document.image(readFileSync('logo.png'), { width: 48 })
document.image('photo.jpg', { fit: [170, 80], align: 'center', valign: 'center' })
writeFileSync('images.pdf', document.finish())
```

Image bytes can be `Uint8Array`, `ArrayBuffer`, or a Node.js `Buffer`. File paths are a Node.js-only
convenience; browser callers must pass bytes. With no dimensions, images use one point per pixel.
Supplying only one dimension preserves the aspect ratio. `fit: [width, height]` contains the image
while preserving the aspect ratio. PNG alpha is emitted through a PDF soft mask. GIF, WebP, SVG,
data URLs, HTTP URLs, and `Blob` inputs are not part of the 1.0 API.

## Declarative API

`createPdf` is useful when all pages and elements are known up front:

```ts
import { createPdf } from 'pdf-crab-js'

const pdf = createPdf({
  title: 'Report',
  unit: 'mm',
  pages: [
    {
      size: 'A4',
      elements: [
        { type: 'rect', x: 20, y: 20, width: 170, height: 30, fill: '#f8fafc' },
        { type: 'text', text: 'Top-left coordinates', x: 28, y: 30, fontSize: 16 },
        { type: 'image', source: logoBytes, x: 20, y: 70, fit: [60, 40] },
      ],
    },
  ],
})
```

`pages` is required and must contain at least one page. A page accepts `size: 'A3' | 'A4' |
'LETTER' | [width, height]` and optional `layout: 'portrait' | 'landscape'`. Declarative elements
are strict discriminated unions: `text`, `textBox`, `line`, `rect`, `polygon`, `path`, and `image`.
`createPdfAsync` has the same input and returns `Promise<Uint8Array>`.

The text engine intentionally uses approximate built-in-font metrics in 1.0. Custom fonts, tables,
Bezier curves, transforms, forms, accessibility tags, and PDF streaming are future features.

## Browser/WASM

```ts
import { createPdf } from 'pdf-crab-js/browser'

const pdf = createPdf({
  pages: [{ size: 'A4', elements: [{ type: 'text', text: 'WASM', x: 20, y: 20 }] }],
})
```

Browser imports reject image file paths with a clear error. Pass `Uint8Array` or `ArrayBuffer`
instead. The browser example lives in `examples/pdf-crab-js/wasm/`.

## Migration from 0.x

Version 1.0 intentionally removes `PdfDocumentBuilder` and the old bottom-left coordinate model.

| 0.x                            | 1.0                                                  |
| ------------------------------ | ---------------------------------------------------- |
| `PdfDocumentBuilder`           | `new PdfDocument(options)`                           |
| `startPage` / `appendElements` | `addPage` and fluent drawing methods                 |
| Bottom-left `y` coordinates    | Top-left `y` coordinates                             |
| Page `width` / `height`        | `size` and optional `layout`                         |
| `Buffer` contract              | `Uint8Array` contract (`Buffer` still works in Node) |
| No image element               | PNG/JPEG bytes and Node file paths                   |

For a direct coordinate migration, use `newY = pageHeight - oldY` for points/lines and
`newY = pageHeight - oldY - height` for rectangles, links, and images. Text should use its new top
edge instead of its old baseline.

## Development

```bash
pnpm --filter pdf-crab-js build
pnpm --filter pdf-crab-js test
pnpm --filter pdf-crab-js build:wasm
pnpm --filter pdf-crab-js test:wasm
```

The structured PDF benchmark compares `createPdf`, the fluent `PdfDocument` facade, PDFKit,
`html-to-pdf-crab-js`, and Gotenberg. It also includes a PDFKit dependency and image-capable
workloads under `benchmarks/pdf/`.
