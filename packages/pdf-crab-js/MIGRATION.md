# Migrating to pdf-crab-js 1.0

The 1.0 contract is intentionally breaking. It removes the provisional builder and stream helper
aliases instead of carrying ambiguous behavior into the stable API.

## Entry points and output

| 0.3/draft               | 1.0                                            |
| ----------------------- | ---------------------------------------------- |
| `createPdf(input)`      | `renderPdf(input).bytes()`                     |
| `createPdfAsync(input)` | `await renderPdf(input).bytes()`               |
| `createPdfStream*`      | `renderPdf(input).stream()` or async iteration |
| `document.finish*()`    | `await document.render().bytes()`              |
| `document.stream()`     | `document.render().stream()`                   |

`PdfOutput` is lazy and single-use. A second consumption raises `PdfError` with code
`PDF_OUTPUT_USED`; `AbortSignal` cancellation releases the native serializer.

## Fluent API

```ts
const document = new PdfDocument({ unit: 'mm', size: 'A4', margin: 20 })
document.text('Hello').moveDown().rect(20, 40, 60, 20).fill()
const pdf = await document.render().bytes()
```

`PdfDocumentBuilder`, `startPage`, `appendElements`, and `appendAnnotations` are no longer public.
Use `addPage`, fluent drawing methods, `link`, and `render`.

## Pages and coordinates

Defaults are A4 portrait, `mm`, and physical 20 mm margins. `addPage()` and automatic page breaks
inherit the effective page options. The declarative `pages` array is readonly at the type level;
runtime validation requires at least one page. Coordinates are top-left in the configured unit.

## Text and tables

`text` and `textBox` are one concept. Text without `x`/`y` flows, wraps, and paginates; absolute
placement requires both coordinates. Alignment and bounded height require `width`. Replace manual
table rectangles with `document.table({ columns, rows })`; headers repeat by default and the cursor
advances after the table.

Custom fonts now use `document.registerFont(family, source, { fallback })`, or the `fonts` array in
`renderPdf`. They are subset and embedded with `ToUnicode`; a missing glyph without an explicit
usable fallback raises `PDF_MISSING_GLYPH`. Arabic/RTL is not part of this release.

## Browser and images

Import `pdf-crab-js/browser` explicitly in browser code. The default WASM build is zero-config
single-threaded; pass image and font bytes (`Uint8Array`/`ArrayBuffer`) instead of Node file paths.
Use `pdf-crab-js/browser/threaded` only in an environment configured with cross-origin isolation.

For a direct migration from the old bottom-left coordinates, use `newY = pageHeight - oldY` for
points/lines and `newY = pageHeight - oldY - height` for rectangles, links, and images. Text now
receives its top edge rather than its old baseline.
