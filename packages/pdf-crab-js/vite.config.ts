import { defineConfig } from 'vite-plus'
import { sharedFmtConfig, sharedLintConfig, sharedTestLintRules } from '../../vite.shared.mjs'

const napiGeneratedFiles = [
  'browser.js',
  'browser-threaded.js',
  'index.js',
  'index.d.ts',
  'index.d.cts',
  '*.wasi.d.cts',
  '*.wasi.cjs',
  '*.wasi-browser.js',
  '*.wasip1-*',
  '*.wasip1.cjs',
  '*.wasip1.d.cts',
  'wasi-worker*.mjs',
  'browser.d.ts',
  'js-binding.*',
  'dist/**',
]

const pdfLintIgnorePatterns = [...(sharedLintConfig?.ignorePatterns ?? []), ...napiGeneratedFiles, 'npm/**', 'js-tests/types/**']

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
      {
        files: ['js-src/**'],
        rules: {
          'id-length': 'off',
          curly: 'off',
          'no-nested-ternary': 'off',
          'unicorn/no-nested-ternary': 'off',
          'unicorn/prefer-export-from': 'off',
          'unicorn/prefer-node-protocol': 'off',
          'typescript/no-non-null-assertion': 'off',
          'typescript/no-unnecessary-type-assertion': 'off',
          'typescript/no-unsafe-type-assertion': 'off',
          'typescript/no-redundant-type-constituents': 'off',
          'typescript/promise-function-async': 'off',
        },
      },
    ],
  },
})
