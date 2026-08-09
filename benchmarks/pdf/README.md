# PDF Benchmark

This package compares PDF generation paths with the same generated table dataset. The runner does not produce one
global leaderboard: it prints a section per comparable workload.

- `Declarative/manual drawing`: `pdf-crab-js` page elements versus PDFKit drawing primitives.
- `High-level table API`: `PdfDocument.table()` versus PDFKit's `document.table()`.
- `Image workload`: the same table with one PNG embedded on every page.
- `HTML/CSS conversion`: `html-to-pdf-crab-js`, reported separately because it solves a different problem.
- `Remote HTML conversion`: Gotenberg is opt-in because it requires an external service.

Modes are explicit about the public output contract: `*-buffered` collects bytes, while `*-stream` consumes chunks as
they are produced. The displayed `p50` and `p95` are calculated from measured runs (with warmups excluded). Throughput
uses p50. TTFB is shown only for stream modes; it means time to the first output chunk, not time to a complete page.

## 5,120-page Reference Run

This is a one-run reference (`PDF_BENCHMARK_RUNS=1`, no warmup), so p50 and p95 are equal. It is intentionally split by
workload instead of ranking unrelated APIs together.

### Declarative/manual drawing — buffered

| Rank | Implementation  | Mode                 |     p50 |     p95 | TTFB p50 | Peak RSS |  PDF size |   Throughput |
| ---: | --------------- | -------------------- | ------: | ------: | -------: | -------: | --------: | -----------: |
|    1 | Node + pdf-crab | declarative-buffered | 1.782 s | 1.782 s |        — | 351.8 MB |  8.262 MB | 2873 pages/s |
|    2 | Node + PDFKit   | manual-buffered      | 2.380 s | 2.380 s |        — | 582.3 MB | 10.005 MB | 2151 pages/s |

### Declarative/manual drawing — stream

| Rank | Implementation  | Mode               |     p50 |     p95 | TTFB p50 | Peak RSS |  PDF size |   Throughput |
| ---: | --------------- | ------------------ | ------: | ------: | -------: | -------: | --------: | -----------: |
|    1 | Node + pdf-crab | declarative-stream | 1.838 s | 1.838 s | 3.516 ms | 375.0 MB |  8.262 MB | 2785 pages/s |
|    2 | Node + PDFKit   | manual-stream      | 2.297 s | 2.297 s | 0.378 ms | 592.9 MB | 10.005 MB | 2229 pages/s |

### High-level table API — buffered

| Rank | Implementation  | Mode                  |     p50 |     p95 | TTFB p50 | Peak RSS |  PDF size |   Throughput |
| ---: | --------------- | --------------------- | ------: | ------: | -------: | -------: | --------: | -----------: |
|    1 | Node + pdf-crab | fluent-table-buffered | 4.662 s | 4.662 s |        — | 363.5 MB | 14.654 MB | 1098 pages/s |
|    2 | Node + PDFKit   | table-buffered        | 8.852 s | 8.852 s |        — | 509.7 MB | 16.334 MB |  578 pages/s |

### High-level table API — stream

| Rank | Implementation  | Mode                |     p50 |     p95 | TTFB p50 | Peak RSS |  PDF size |   Throughput |
| ---: | --------------- | ------------------- | ------: | ------: | -------: | -------: | --------: | -----------: |
|    1 | Node + pdf-crab | fluent-table-stream | 4.615 s | 4.615 s |  4.156 s | 339.5 MB | 14.654 MB | 1109 pages/s |
|    2 | Node + PDFKit   | table-stream        | 9.202 s | 9.202 s | 0.568 ms | 493.3 MB | 16.334 MB |  556 pages/s |

The stream tables consume PDFKit's native Node stream and pdf-crab's `AsyncIterable` with the same chunk-level checks.
The fluent stream's TTFB includes the high-level table construction that happens before `render()` returns, while
PDFKit starts emitting while its drawing calls are still running. The declarative stream can emit its PDF header early
because its layout input is already available to the native renderer. TTFB is an output-chunk measurement, not a
page-completion measurement.

The declarative artifact in this run was 8.262 MB with a roughly 378 MB peak RSS, compared with the previous
implementation's 60.251 MB artifact and roughly 1.24 GB peak RSS.

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
- `PDF_BENCHMARK_MEMORY_SAMPLE_MS=50` controls the in-process RSS sampling interval.
- `PDF_BENCHMARK_ONLY=pdf-crab`, `PDF_BENCHMARK_ONLY=pdf-crab-document`,
  `PDF_BENCHMARK_ONLY=pdf-crab-stream`, `PDF_BENCHMARK_ONLY=pdf-crab-document-stream`,
  `PDF_BENCHMARK_ONLY=pdfkit`, `PDF_BENCHMARK_ONLY=pdfkit-stream`, `PDF_BENCHMARK_ONLY=pdfkit-table`,
  `PDF_BENCHMARK_ONLY=pdfkit-table-stream`,
  `PDF_BENCHMARK_ONLY=html-to-pdf-crab-js`, `PDF_BENCHMARK_ONLY=pdf-crab-image`,
  `PDF_BENCHMARK_ONLY=pdf-crab-document-image`, `PDF_BENCHMARK_ONLY=pdfkit-image`, or
  `PDF_BENCHMARK_ONLY=gotenberg-node` selects scenarios. Multiple ids can be separated by commas. Gotenberg is
  available only when explicitly selected.
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

The pdf-crab stream scenarios consume every `Uint8Array` chunk, validate the header/trailer incrementally, and
optionally write directly to `benchmarks/pdf/output/`. They measure end-to-end time-to-first-byte and peak RSS without
retaining a second full-PDF buffer. Set `PDF_BENCHMARK_WRITE=1` only when an artifact is needed; file output is
streamed as it is produced.
