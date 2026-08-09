import { mkdirSync, readFileSync, writeFileSync } from 'node:fs'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

import { PdfDocument } from 'pdf-crab-js'

const exampleDirectory = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../..')
const outputDirectory = path.join(exampleDirectory, 'output')
const outputPath = path.join(outputDirectory, 'pdf-crab-js-image-example.pdf')
const image = readFileSync(path.join(exampleDirectory, 'assets/pdf-crab-js-image-source.png'))

const document = new PdfDocument({
  metadata: {
    creator: 'pdf-crab-js examples',
    subject: 'Embedding a PNG in a PDF',
    title: 'pdf-crab-js image example',
  },
  title: 'pdf-crab-js image example',
  unit: 'mm',
  size: 'A4',
  margin: 18,
})

document
  .fillColor('#0f172a')
  .rect(18, 18, 174, 42)
  .fill()
  .font('HelveticaBold')
  .fontSize(18)
  .fillColor('#ffffff')
  .text('Image embedding', 26, 29)
  .font('Helvetica')
  .fontSize(9)
  .fillColor('#bfdbfe')
  .text('A PNG embedded directly from a Node.js Buffer.', 26, 43)
  .fontSize(10)
  .fillColor('#334155')
  .text('Use fit, align, and valign to place an image without changing its aspect ratio.', 18, 72, {
    width: 174,
  })
  .fillColor('#f8fafc')
  .rect(18, 94, 174, 98)
  .fill()
  .image(image, {
    x: 18,
    y: 94,
    fit: [174, 98],
    align: 'center',
    valign: 'center',
  })
  .strokeColor('#cbd5e1')
  .lineWidth(0.4)
  .rect(18, 94, 174, 98)
  .stroke()
  .fontSize(8)
  .fillColor('#64748b')
  .text('1672 x 941 PNG - fitted into a 174 x 98 mm box', 18, 199, { align: 'center', width: 174 })
  .fillColor('#0f172a')
  .rect(18, 218, 174, 48)
  .fill()
  .font('Courier')
  .fontSize(7.5)
  .fillColor('#e2e8f0')
  .text(
    "const image = readFileSync('image.png')\n\ndocument.image(image, {\n  fit: [174, 98],\n  align: 'center',\n  valign: 'center',\n})",
    26,
    226,
    { lineHeight: 10, width: 158 },
  )
  .font('Helvetica')
  .fontSize(7.5)
  .fillColor('#64748b')
  .text('Generated with pdf-crab-js', 18, 278, { align: 'center', width: 174 })

mkdirSync(outputDirectory, { recursive: true })
const pdf = await document.render().bytes()
writeFileSync(outputPath, pdf)

console.log(`Generated ${outputPath}`)
console.log(`Size: ${pdf.byteLength.toLocaleString()} bytes`)
