# PDF Benchmark

This package compares PDF generation paths with the same generated table dataset:

- `Node + pdf-crab`: local native `pdf-crab-js` generation.
- `Node + pdf-crab (document)`: fluent `PdfDocument` facade over the same native renderer.
- `Node + PDFKit`: local JavaScript PDF generation with `pdfkit`.
- `Node + html-to-pdf-crab`: local native `html-to-pdf-crab-js` HTML-to-PDF generation.
- `Node + Gotenberg`: Node.js multipart upload to Gotenberg's Chromium HTML-to-PDF endpoint.

## 10-page Snapshot

Local 10-page benchmark, fastest to slowest by execution time (Gotenberg excluded because it requires the external
service):

| Order | Language                | Mode       | Execution time |       Throughput |
| ----- | ----------------------- | ---------- | -------------: | ---------------: |
| 1     | Node + pdf-crab         | local      |       3.323 ms | 3008.970 pages/s |
| 2     | Node + pdf-crab         | document   |       4.478 ms | 2232.994 pages/s |
| 3     | Node + PDFKit           | local      |      16.969 ms |  589.318 pages/s |
| 4     | Node + html-to-pdf-crab | local-html |      40.758 ms |  245.348 pages/s |

Interpretation:

- `pdf-crab-js` is the fastest path when the document can be represented as structured PDF pages
  and elements.
- PDFKit provides a pure JavaScript structured-PDF baseline without a native binding.
- `html-to-pdf-crab-js` is the easy HTML/CSS path. It avoids Chromium/Gotenberg while still doing
  HTML layout work.
- Gotenberg is useful when Chromium compatibility is required, but it adds a service boundary and
  browser conversion overhead.

## Setup

Start Gotenberg locally when running the Gotenberg scenario:

```bash
docker run --rm -p 3000:3000 gotenberg/gotenberg:8
```

## Run

```bash
pnpm --filter pdf-benchmark benchmark
```

The benchmark defaults to a 10-page PDF with 10 table rows per page. Useful knobs:

- `PDF_BENCHMARK_PAGES=5120` changes the page count.
- `PDF_BENCHMARK_RUNS=10` changes measured runs per scenario.
- `PDF_BENCHMARK_WARMUP=3` changes warmup runs per scenario.
- `PDF_BENCHMARK_ONLY=pdf-crab`, `PDF_BENCHMARK_ONLY=pdf-crab-document`, `PDF_BENCHMARK_ONLY=pdfkit`,
  `PDF_BENCHMARK_ONLY=html-to-pdf-crab-js`, `PDF_BENCHMARK_ONLY=pdf-crab-image`,
  `PDF_BENCHMARK_ONLY=pdf-crab-document-image`, `PDF_BENCHMARK_ONLY=pdfkit-image`, or
  `PDF_BENCHMARK_ONLY=gotenberg-node` selects scenarios. Multiple ids can be separated by commas.
- `PDF_BENCHMARK_GOTENBERG_URL=http://localhost:3000` changes the Gotenberg base URL.
- `PDF_BENCHMARK_WRITE=1` writes generated PDFs to `benchmarks/pdf/output/`.
- `PDF_BENCHMARK_COLORS=0` disables terminal colors.

For a large run similar to the captured benchmark table style:

```bash
PDF_BENCHMARK_PAGES=5120 PDF_BENCHMARK_RUNS=1 PDF_BENCHMARK_WARMUP=0 pnpm --filter pdf-benchmark benchmark
```

The image scenarios embed the same PNG on every page and isolate image decode/XObject overhead:

```bash
PDF_BENCHMARK_ONLY=pdf-crab-image,pdf-crab-document-image,pdfkit-image \
  PDF_BENCHMARK_RUNS=5 PDF_BENCHMARK_WARMUP=1 pnpm --filter pdf-benchmark benchmark
```
