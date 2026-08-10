import { PdfDocument } from 'pdf-crab-js/browser.js'

import { type RenderedExample, type SampleContext, usd } from './types.js'

type InvoiceRow = Record<string, unknown> & {
  description: string
  quantity: number
  rate: number
  total: number
}

function createRows(count: number): InvoiceRow[] {
  const products = [
    'Structured PDF generation',
    'Batch document processing',
    'Custom report template',
    'WebAssembly integration',
    'Typography review',
    'Deployment support',
  ]

  return Array.from({ length: count }, (_, index) => {
    const quantity = (index % 4) + 1
    const rate = 89 + ((index * 37) % 320)
    return {
      description: `${products[index % products.length]} #${String(index + 1).padStart(2, '0')}`,
      quantity,
      rate,
      total: quantity * rate,
    }
  })
}

function drawBrandMark(document: PdfDocument, accent: string): void {
  document
    .save()
    .fillColor(accent)
    .strokeColor(accent)
    .lineWidth(0.8)
    .moveTo(18, 20)
    .lineTo(25, 14)
    .lineTo(32, 20)
    .lineTo(25, 26)
    .closePath()
    .fillAndStroke()
    .restore()
}

export function buildInvoice(context: SampleContext): RenderedExample {
  const { accent, company, font, recipient, rowCount } = context
  const rows = createRows(rowCount)
  const subtotal = rows.reduce((sum, row) => sum + row.total, 0)
  const tax = subtotal * 0.08
  const document = new PdfDocument({
    margin: 18,
    metadata: {
      author: company,
      keywords: ['invoice', 'pdf-crab-js', 'wasm'],
      subject: 'Complete pdf-crab-js 1.0 API example',
      title: `Invoice - ${company}`,
    },
    size: 'A4',
    title: `Invoice - ${company}`,
    unit: 'mm',
  })

  document.registerFont('Tuffy', font).font('Tuffy')
  document.rect(0, 0, 210, 48).fillColor('#231b1d').fill()
  drawBrandMark(document, accent)
  document
    .fontSize(9)
    .fillColor('#d7d3e9')
    .text(company.toUpperCase(), 38, 17)
    .fontSize(22)
    .fillColor('#ffffff')
    .text('INVOICE', 142, 18)
    .fontSize(9)
    .fillColor('#6d6878')
    .text('BILL TO', 18, 61)
    .fontSize(15)
    .fillColor('#231b1d')
    .text(recipient, 18, 70)
    .fontSize(9)
    .fillColor('#6d6878')
    .text('ISSUED', 124, 61)
    .fillColor('#231b1d')
    .text(new Intl.DateTimeFormat('en-US').format(new Date()), 124, 70)
    .fillColor('#6d6878')
    .text('DUE', 160, 61)
    .fillColor('#231b1d')
    .text('Net 15', 160, 70)

  document.table<InvoiceRow>({
    border: '#d7d4dc',
    columns: [
      { font: 'Tuffy', header: '#', value: (_row, index) => index + 1, width: 14 },
      { font: 'Tuffy', header: 'Description', key: 'description', width: '*' },
      { align: 'right', font: 'Tuffy', header: 'Qty.', key: 'quantity', width: 18 },
      {
        align: 'right',
        font: 'Tuffy',
        formatter: (value) => usd(Number(value)),
        header: 'Rate',
        key: 'rate',
        width: 30,
      },
      {
        align: 'right',
        font: 'Tuffy',
        formatter: (value) => usd(Number(value)),
        header: 'Total',
        key: 'total',
        width: 34,
      },
    ],
    headerHeight: 9,
    padding: 2.4,
    repeatHeader: true,
    rowHeight: 8.5,
    rows,
    rowSplit: 'avoid',
    stripe: '#f8f1f3',
    width: 174,
    x: 18,
    y: 88,
  })

  document
    .font('Tuffy')
    .fontSize(9)
    .fillColor('#6d6878')
    .moveDown(1)
    .text(`Subtotal: ${usd(subtotal)}`, { align: 'right', width: 174 })
    .text(`Tax (8%): ${usd(tax)}`, { align: 'right', width: 174 })
    .fontSize(13)
    .fillColor(accent)
    .text(`Total: ${usd(subtotal + tax)}`, { align: 'right', width: 174 })
    .fontSize(8)
    .fillColor('#6d6878')
    .moveDown(1.4)
    .text('Payment by bank transfer. Thank you for building better products with us.', {
      align: 'justify',
      width: 174,
    })

  const linkTop = document.y + 3
  document
    .fillColor(accent)
    .text('github.com/flash-tecnologia/crab-js', 18, linkTop)
    .link('https://github.com/flash-tecnologia/crab-js', { height: 6, width: 74, x: 18, y: linkTop })

  return {
    filename: 'pdf-crab-invoice.pdf',
    output: document.render(),
    title: 'Structured invoice',
  }
}
