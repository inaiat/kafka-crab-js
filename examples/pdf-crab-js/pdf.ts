import { mkdirSync, readFileSync, writeFileSync } from 'node:fs'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

import { createPdf } from 'pdf-crab-js'

const currentDirectory = path.dirname(fileURLToPath(import.meta.url))
const outputDirectory = path.join(currentDirectory, 'output')
const outputPath = path.join(outputDirectory, 'pdf-crab-js-example.pdf')
const previewImage = readFileSync(path.join(currentDirectory, 'screenshots/pdf-crab-js-example.pdf.png'))

const pdf = createPdf({
  title: 'pdf-crab-js example',
  unit: 'mm',
  pages: [
    {
      size: 'A4',
      elements: [
        {
          type: 'rect',
          x: 18,
          y: 18,
          width: 174,
          height: 34,
          fill: '#f8fafc',
          stroke: '#0f172a',
          strokeWidth: 1,
        },
        {
          type: 'text',
          text: 'pdf-crab-js',
          x: 26,
          y: 27,
          font: 'HelveticaBold',
          fontSize: 18,
          fill: '#0f172a',
        },
        {
          type: 'text',
          text: 'Generated with the PdfDocument-style declarative API',
          x: 26,
          y: 39,
          fontSize: 10,
          fill: '#334155',
        },
        {
          type: 'line',
          x1: 18,
          y1: 62,
          x2: 192,
          y2: 62,
          stroke: '#2563eb',
          strokeWidth: 1.5,
        },
        {
          type: 'rect',
          x: 24,
          y: 78,
          width: 72,
          height: 36,
          stroke: '#16a34a',
          strokeWidth: 1,
        },
        {
          type: 'text',
          text: 'Text, lines, rectangles, and images',
          x: 104,
          y: 87,
          fontSize: 12,
          fill: '#111827',
        },
        {
          type: 'text',
          text: 'Coordinates use a top-left origin.',
          x: 104,
          y: 98,
          fontSize: 9,
          fill: '#475569',
        },
        {
          type: 'image',
          source: previewImage,
          x: 24,
          y: 132,
          fit: [72, 48],
          align: 'center',
          valign: 'center',
        },
      ],
    },
  ],
})

mkdirSync(outputDirectory, { recursive: true })
writeFileSync(outputPath, pdf)

console.log(`Generated ${outputPath}`)
console.log(`Size: ${pdf.byteLength.toLocaleString()} bytes`)
