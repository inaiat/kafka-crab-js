import { PdfDocument } from 'pdf-crab-js/browser.js'

import { type RenderedExample, type SampleContext, usd } from './types.js'

type ReportRow = Record<string, unknown> & {
  account: string
  owner: string
  region: string
  revenue: number
  status: string
  variance: number
}

function createRows(count: number): ReportRow[] {
  const companies = ['Aurora', 'Beacon', 'Cedar', 'Drift', 'Evergreen', 'Foundry', 'Golden', 'Horizon']
  const owners = ['Alice', 'Bruno', 'Chloe', 'Diana', 'Eli']
  const regions = ['Southeast', 'South', 'Northeast', 'Central']
  const statuses = ['Healthy', 'Attention', 'Expansion']

  return Array.from({ length: count }, (_, index) => ({
    account: `${companies[index % companies.length]} ${String(index + 1).padStart(2, '0')}`,
    owner: owners[index % owners.length] ?? 'Crab team',
    region: regions[index % regions.length] ?? 'Global',
    revenue: 18_500 + ((index * 13_711) % 92_000),
    status: statuses[index % statuses.length] ?? 'Healthy',
    variance: ((index * 7) % 31) - 8,
  }))
}

export function buildReport(context: SampleContext): RenderedExample {
  const { accent, company, font, recipient, rowCount } = context
  const rows = createRows(rowCount)
  const totalRevenue = rows.reduce((sum, row) => sum + row.revenue, 0)
  const averageVariance = rows.reduce((sum, row) => sum + row.variance, 0) / rows.length
  const document = new PdfDocument({
    layout: 'landscape',
    margin: 14,
    metadata: { author: company, subject: 'Revenue operations report', title: `${company} report` },
    size: 'A4',
    title: `${company} report`,
    unit: 'mm',
  })

  document.registerFont('Tuffy', font).font('Tuffy')
  document.rect(0, 0, 297, 43).fillColor('#231b1d').fill()
  document
    .fontSize(8)
    .fillColor('#b9b3d6')
    .text('REVENUE OPERATIONS / 2026', 14, 13)
    .fontSize(21)
    .fillColor('#ffffff')
    .text(company, 14, 23)
    .fontSize(9)
    .fillColor('#d8d4e8')
    .text(`Prepared for ${recipient}`, 214, 24)

  const metrics = [
    ['Revenue monitored', usd(totalRevenue)],
    ['Average variance', `${averageVariance.toFixed(1)}%`],
    ['Active accounts', rows.length.toLocaleString('en-US')],
  ] as const

  for (const [index, [label, value]] of metrics.entries()) {
    const left = 14 + index * 91.5
    document
      .rect(left, 50, 86, 25)
      .fillColor(index === 0 ? '#f0edff' : '#f7f6f3')
      .strokeColor('#dedad4')
      .fillAndStroke()
    document
      .fontSize(7.5)
      .fillColor('#77727e')
      .text(label.toUpperCase(), left + 5, 57)
      .fontSize(14)
      .fillColor(index === 0 ? accent : '#231b1d')
      .text(value, left + 5, 65)
  }

  document.table<ReportRow>({
    border: '#d9d6de',
    columns: [
      { font: 'Tuffy', header: '#', value: (_row, index) => index + 1, width: 14 },
      { font: 'Tuffy', header: 'Account', key: 'account', width: '*' },
      { font: 'Tuffy', header: 'Region', key: 'region', width: 34 },
      { font: 'Tuffy', header: 'Owner', key: 'owner', width: 28 },
      {
        align: 'right',
        font: 'Tuffy',
        formatter: (value) => usd(Number(value)),
        header: 'Revenue',
        key: 'revenue',
        width: 38,
      },
      {
        align: 'right',
        font: 'Tuffy',
        formatter: (value) => `${Number(value) > 0 ? '+' : ''}${String(value)}%`,
        header: 'Var.',
        key: 'variance',
        width: 22,
      },
      { font: 'Tuffy', header: 'Status', key: 'status', width: 29 },
    ],
    headerHeight: 8.5,
    padding: 2.2,
    repeatHeader: true,
    rowHeight: 8,
    rows,
    rowSplit: 'avoid',
    stripe: '#f8f7fa',
    width: 269,
    x: 14,
    y: 84,
  })

  document
    .font('Tuffy')
    .fontSize(8)
    .fillColor('#696572')
    .moveDown(1)
    .text('Repeated headers, mixed column widths, and natural cursor continuation after the table.', {
      align: 'right',
      width: 269,
    })

  return {
    filename: 'pdf-crab-report.pdf',
    output: document.render(),
    title: 'Multi-page report',
  }
}
