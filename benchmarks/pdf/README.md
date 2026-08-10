# PDF Benchmark

This package compares PDF generation paths with the same generated table dataset. The runner does not produce one
global leaderboard: it prints a section per comparable workload.

- `Declarative/manual drawing`: `pdf-crab-js` page elements versus PDFKit drawing primitives.
- `High-level table API`: `PdfDocument.table()` versus PDFKit's `document.table()`.
- `Image workload`: the same table with one PNG embedded on every page.
- `HTML/CSS conversion`: optional `html-to-pdf-crab-js`, reported separately because it solves a different problem.
- `Remote HTML conversion`: optional Gotenberg, which requires an external service.

Modes are explicit about the public output contract: `*-buffered` collects bytes, while `*-stream` consumes chunks as
they are produced. The displayed `p50` and `p95` are calculated from measured runs (with warmups excluded). Throughput
uses p50.

## 1,000-page reference run

This reference uses 10 rows per page, 3 warmups, and 10 measured runs on Node.js 24.19.0/Darwin arm64. Throughput is
calculated from p50. Results are environment-dependent; reproduce them on the target system before using them for
capacity planning.

### Latency and throughput

| Workload           | Output   |   pdf-crab p50 / p95 |     PDFKit p50 / p95 | pdf-crab throughput | PDFKit throughput |   Speedup |
| ------------------ | -------- | -------------------: | -------------------: | ------------------: | ----------------: | --------: |
| Declarative/manual | Buffered | 261.112 / 262.760 ms | 322.669 / 331.236 ms |   3,829.772 pages/s | 3,099.153 pages/s | **1.24x** |
| Declarative/manual | Stream   | 270.564 / 272.054 ms | 323.439 / 331.698 ms |   3,695.985 pages/s | 3,091.771 pages/s | **1.20x** |
| High-level table   | Buffered | 658.476 / 669.829 ms |      1.268 / 1.312 s |   1,518.659 pages/s |   788.529 pages/s | **1.93x** |
| High-level table   | Stream   | 656.514 / 690.399 ms |      1.281 / 1.295 s |   1,523.197 pages/s |   780.357 pages/s | **1.95x** |

### Peak RSS and artifact size

| Workload           | Output   | pdf-crab peak RSS | PDFKit peak RSS |  Less RAM | pdf-crab PDF | PDFKit PDF | Smaller PDF |
| ------------------ | -------- | ----------------: | --------------: | --------: | -----------: | ---------: | ----------: |
| Declarative/manual | Buffered |        231.219 MB |      407.797 MB | **43.3%** |     1.611 MB |   1.952 MB |   **17.5%** |
| Declarative/manual | Stream   |        228.484 MB |      328.516 MB | **30.4%** |     1.611 MB |   1.952 MB |   **17.5%** |
| High-level table   | Buffered |        232.047 MB |      423.016 MB | **45.1%** |     2.859 MB |   3.187 MB |   **10.3%** |
| High-level table   | Stream   |        223.969 MB |      372.188 MB | **39.8%** |     2.859 MB |   3.187 MB |   **10.3%** |

At 1,000 pages, pdf-crab delivered 20-24% more throughput for manual/declarative drawing and roughly 1.9x the
throughput for high-level tables. It used 30-45% less peak RSS and generated PDFs that were 10-17% smaller in this run.
The stream scenarios consume every chunk from PDFKit's native Node stream and pdf-crab's `AsyncIterable`.

## Setup

Start Gotenberg locally before enabling its scenario:

```bash
podman run --rm -p 3000:3000 gotenberg/gotenberg:8
```

Docker can be used instead by replacing `podman` with `docker`.

## Run

```bash
pnpm --filter pdf-benchmark benchmark
```

The benchmark defaults to a 10-page PDF with 10 table rows per page. HTML conversion and Gotenberg are disabled by
default. Useful knobs:

- `PDF_BENCHMARK_PAGES=5120` changes the page count.
- `PDF_BENCHMARK_RUNS=10` changes measured runs per scenario.
- `PDF_BENCHMARK_WARMUP=3` changes warmup runs per scenario.
- `PDF_BENCHMARK_MEMORY_SAMPLE_MS=50` controls the in-process RSS sampling interval.
- `PDF_BENCHMARK_ENABLE_HTML=1` adds `html-to-pdf-crab-js` to the default scenarios.
- `PDF_BENCHMARK_ENABLE_GOTENBERG=1` adds Gotenberg to the default scenarios.
- `PDF_BENCHMARK_ONLY=pdf-crab`, `PDF_BENCHMARK_ONLY=pdf-crab-document`,
  `PDF_BENCHMARK_ONLY=pdf-crab-stream`, `PDF_BENCHMARK_ONLY=pdf-crab-document-stream`,
  `PDF_BENCHMARK_ONLY=pdfkit`, `PDF_BENCHMARK_ONLY=pdfkit-stream`, `PDF_BENCHMARK_ONLY=pdfkit-table`,
  `PDF_BENCHMARK_ONLY=pdfkit-table-stream`,
  `PDF_BENCHMARK_ONLY=html-to-pdf-crab-js`, `PDF_BENCHMARK_ONLY=pdf-crab-image`,
  `PDF_BENCHMARK_ONLY=pdf-crab-document-image`, `PDF_BENCHMARK_ONLY=pdfkit-image`, or
  `PDF_BENCHMARK_ONLY=gotenberg-node` selects an exact scenario list. Multiple ids can be separated by commas. An
  explicit list takes precedence over the enable flags.
- `PDF_BENCHMARK_GOTENBERG_URL=http://localhost:3000` changes the Gotenberg base URL.
- `PDF_BENCHMARK_WRITE=1` writes generated PDFs to `benchmarks/pdf/output/`.
- `PDF_BENCHMARK_COLORS=0` disables terminal colors.

Enable one or both optional HTML conversion scenarios while retaining the default comparisons:

```bash
PDF_BENCHMARK_ENABLE_HTML=1 pnpm --filter pdf-benchmark benchmark

PDF_BENCHMARK_ENABLE_GOTENBERG=1 pnpm --filter pdf-benchmark benchmark

PDF_BENCHMARK_ENABLE_HTML=1 PDF_BENCHMARK_ENABLE_GOTENBERG=1 \
  pnpm --filter pdf-benchmark benchmark
```

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
optionally write directly to `benchmarks/pdf/output/`. They measure end-to-end duration and peak RSS without retaining a
second full-PDF buffer. Set `PDF_BENCHMARK_WRITE=1` only when an artifact is needed; file output is streamed as it is
produced.
