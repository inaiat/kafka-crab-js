import path from 'node:path'
import { fileURLToPath } from 'node:url'

import { defineConfig } from 'vite-plus'

import { sharedFmtConfig, sharedLintConfig } from '../../vite.shared.mjs'

const currentDirectory = path.dirname(fileURLToPath(import.meta.url))
const workspaceRoot = path.resolve(currentDirectory, '../..')

export default defineConfig({
  build: {
    target: 'esnext',
  },
  fmt: {
    ...sharedFmtConfig,
  },
  lint: {
    ...sharedLintConfig,
    overrides: [
      {
        files: ['src/**/*.ts'],
        rules: {
          'id-length': 'off',
          'max-lines': 'off',
          'max-statements': 'off',
          'no-await-in-loop': 'off',
          'no-nested-ternary': 'off',
          'unicorn/no-nested-ternary': 'off',
          'unicorn/prefer-node-protocol': 'off',
        },
      },
    ],
  },
  optimizeDeps: {
    exclude: [
      'html-to-pdf-crab-js',
      'html-to-pdf-crab-js/browser.js',
      'html-to-pdf-crab-js-wasm32-wasip1',
      'pdf-crab-js',
      'pdf-crab-js/browser.js',
    ],
  },
  resolve: {
    alias: {
      'html-to-pdf-crab-js-wasm32-wasip1': path.resolve(
        currentDirectory,
        '../../packages/html-to-pdf-crab-js/html-to-pdf-crab-js.wasip1-browser.js',
      ),
    },
  },
  server: {
    fs: {
      allow: [workspaceRoot],
    },
    open: true,
  },
})
