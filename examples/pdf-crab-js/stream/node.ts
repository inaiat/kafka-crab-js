import { createWriteStream, mkdirSync } from 'node:fs'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import { pipeline } from 'node:stream/promises'
import { Readable } from 'node:stream'

import { PdfDocument, renderPdf, type PdfDocumentInput } from 'pdf-crab-js'

const currentDirectory = path.dirname(fileURLToPath(import.meta.url))
const outputDirectory = path.join(currentDirectory, '..', 'output')

const input: PdfDocumentInput = {
  title: 'pdf-crab-js stream example',
  unit: 'mm' as const,
  pages: [
    {
      size: 'A4' as const,
      elements: [
        { type: 'rect' as const, x: 20, y: 20, width: 170, height: 42, fill: '#eff6ff', stroke: '#2563eb' },
        { type: 'text' as const, text: 'Declarative stream', x: 30, y: 32, font: 'HelveticaBold', fontSize: 18 },
        {
          type: 'text' as const,
          text: 'renderPdf yields Uint8Array chunks.',
          x: 30,
          y: 44,
          fontSize: 10,
          fill: '#334155',
        },
      ],
    },
  ],
}

const document = new PdfDocument({
  title: 'pdf-crab-js fluent stream example',
  unit: 'mm',
  size: 'A4',
  margin: 20,
})
document
  .fillColor('#f0fdf4')
  .strokeColor('#16a34a')
  .rect(20, 20, 170, 42)
  .fillAndStroke()
  .font('HelveticaBold')
  .fontSize(18)
  .fillColor('#14532d')
  .text('Fluent document stream', { x: 30, y: 32 })
  .font('Helvetica')
  .fontSize(10)
  .fillColor('#166534')
  .text('Readable.from(output.stream()) writes chunks with backpressure.', { x: 30, y: 44 })

async function writePdf(outputPath: string, chunks: AsyncIterable<Uint8Array>): Promise<void> {
  await pipeline(Readable.from(chunks), createWriteStream(outputPath))
  console.log(`Generated ${outputPath}`)
}

mkdirSync(outputDirectory, { recursive: true })

await writePdf(
  path.join(outputDirectory, 'pdf-crab-js-stream-declarative-example.pdf'),
  renderPdf(input).stream({ chunkSize: 16 * 1024 }),
)
await writePdf(
  path.join(outputDirectory, 'pdf-crab-js-stream-document-example.pdf'),
  document.render().stream({ chunkSize: 16 * 1024 }),
)
