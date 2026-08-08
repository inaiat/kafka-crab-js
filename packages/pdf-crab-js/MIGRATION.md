# Migrating to pdf-crab-js 1.0

The 1.0 release is intentionally breaking. The public builder and bottom-left coordinate model
were removed in favor of a fluent document API and a single top-left coordinate system.

## Builder to document

```ts
const document = new PdfDocument({ unit: 'mm', size: 'A4', margin: 20 })
document.text('Hello').moveDown().rect(20, 40, 60, 20).fill()
const pdf = document.finish()
```

`PdfDocumentBuilder`, `startPage`, `appendElements`, and `appendAnnotations` are no longer public.
Use `addPage`, fluent drawing methods, and `link` instead.

## Coordinates

The old bottom-left coordinates must be inverted. For a page height `H`:

- points and line endpoints: `newY = H - oldY`;
- rectangles, links, and images: `newY = H - oldY - height`;
- text: pass the top edge of the text instead of the old baseline.

## Images and output

Images are now `Uint8Array`, `ArrayBuffer`, or Node `Buffer` values. Node callers may also pass a
file path. Browser callers must provide bytes. `finish()` and `createPdf()` return `Uint8Array`
values; Node still exposes the same bytes as a `Buffer` subtype.
