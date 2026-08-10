import { PdfDocument, renderPdf } from '../../browser.js'

const bytes = new Uint8Array([1, 2, 3])

renderPdf({
  fonts: [{ family: 'BrowserFont', source: bytes }],
  pages: [{ elements: [{ type: 'image', source: bytes, x: 0, y: 0 }] }],
})

new PdfDocument().image(bytes)

renderPdf({
  pages: [
    {
      elements: [
        {
          type: 'image',
          // @ts-expect-error Browser declarative inputs accept bytes, not file paths.
          source: './node-only.png',
          x: 0,
          y: 0,
        },
      ],
    },
  ],
})

// @ts-expect-error Browser fluent inputs accept bytes, not file paths.
new PdfDocument().image('./node-only.png')
