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
