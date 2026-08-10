# Crab PDF Studio

An interactive browser lab for both PDF APIs in this monorepo:

- `pdf-crab-js` 1.0: a structured invoice, multi-page report, and declarative catalog.
- `html-to-pdf-crab-js`: a live HTML/CSS editor and threadless WASM renderer.

The structured examples exercise embedded TTF fonts, typed tables, shapes, links, images,
landscape pages, automatic pagination, and consumption through either `bytes()` or `stream()`.
Each scenario shows the actual TypeScript module used to create its PDF. Vite imports those files
with `?raw`, so the displayed source and executed source cannot silently drift apart.

## Local Vite Plus server

From the monorepo root:

```bash
pnpm install
pnpm --filter crab-js-pdf-studio dev
```

This compiles both WASM bindings and starts Vite at `http://127.0.0.1:5174`. The default browser
entries use threadless WASM and do not require COOP/COEP headers. The optional
`html-to-pdf-crab-js/browser/threaded` and `pdf-crab-js/browser/threaded` entries remain available
for isolated deployments.

To reuse bindings that are already compiled:

```bash
pnpm --filter crab-js-pdf-studio exec vite --host 127.0.0.1 --port 5174
```

## Quality and production build

```bash
pnpm --filter crab-js-pdf-studio check
pnpm --filter crab-js-pdf-studio build
pnpm --filter crab-js-pdf-studio preview
```

The build writes `dist/`. For a Netlify site connected to this repository, use:

- Build command: `pnpm --filter crab-js-pdf-studio build`
- Publish directory: `examples/wasm-samples/dist`

For Netlify Drop, upload only `dist/`.

`assets/Tuffy.ttf` is distributed under the license in `assets/Tuffy-LICENSE.txt`.
