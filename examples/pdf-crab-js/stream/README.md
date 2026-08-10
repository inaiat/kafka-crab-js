# pdf-crab-js stream examples

These examples show the portable `AsyncIterable<Uint8Array>` stream API and its Node.js adapter:

- [Node.js source](../src/node/stream.ts)
- [Browser/WASM source](../src/browser/stream.ts)

## Node.js

Run from the repository root:

```bash
pnpm --filter pdf-crab-js-examples example:stream
```

The script writes two files to `examples/pdf-crab-js/output/`:

- `pdf-crab-js-stream-declarative-example.pdf` uses `renderPdf(...).stream()`.
- `pdf-crab-js-stream-document-example.pdf` uses `document.render().stream()` and `Readable.from`.

Both streams are pull-driven: the native serializer emits the header and PDF objects on demand,
and `chunkSize` controls how large each transport chunk can be.

## Browser/WASM

Build the examples and open `/stream/` in the Vite preview:

```bash
pnpm --filter pdf-crab-js-examples browser:build
pnpm --filter pdf-crab-js-examples browser:preview
```

The browser example consumes `renderPdf(...).stream()` directly, counts the chunks, and creates a
downloadable PDF from the resulting byte sequence. The default browser build is zero-config and
does not require cross-origin isolation.
