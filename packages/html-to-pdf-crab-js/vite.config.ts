import { defineConfig } from 'vite-plus'
import { sharedFmtConfig, sharedLintConfig, sharedTestLintRules } from '../../vite.shared.mjs'

const napiGeneratedFiles = [
  'browser.js',
  'index.js',
  'index.d.ts',
  'index.d.cts',
  '*.wasi.d.cts',
  '*.wasi.cjs',
  '*.wasi-browser.js',
  '*.wasip1.d.cts',
  '*.wasip1.cjs',
  '*.wasip1-browser.js',
  '*.wasip1-deferred.js',
  '*.wasip1-deferred.d.ts',
  'wasi-worker*.mjs',
]

const pdfLintIgnorePatterns = [...(sharedLintConfig?.ignorePatterns ?? []), ...napiGeneratedFiles, 'npm/**']

export default defineConfig({
  fmt: {
    ...sharedFmtConfig,
  },
  lint: {
    ...sharedLintConfig,
    ignorePatterns: pdfLintIgnorePatterns,
    overrides: [
      {
        files: ['js-tests/**'],
        rules: sharedTestLintRules,
      },
    ],
  },
})
