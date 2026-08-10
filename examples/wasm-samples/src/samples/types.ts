import type { PdfOutput } from 'pdf-crab-js/browser.js'

export type SampleContext = {
  accent: string
  company: string
  font: Uint8Array
  image?: Uint8Array
  recipient: string
  rowCount: number
}

export type RenderedExample = {
  filename: string
  output: PdfOutput
  title: string
}

export function usd(value: number): string {
  return new Intl.NumberFormat('en-US', {
    currency: 'USD',
    maximumFractionDigits: 2,
    style: 'currency',
  })
    .format(value)
    .replaceAll('\u00A0', ' ')
    .replaceAll('\u202F', ' ')
}
