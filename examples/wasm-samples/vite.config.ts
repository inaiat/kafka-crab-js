import { copyFileSync, mkdirSync } from 'node:fs'
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
      'html-to-pdf-crab-js-wasm32-wasi',
      'pdf-crab-js',
      'pdf-crab-js/browser.js',
    ],
  },
  plugins: [
    {
      closeBundle() {
        const outputDirectory = path.resolve(currentDirectory, 'dist')
        mkdirSync(outputDirectory, { recursive: true })
        copyFileSync(path.resolve(currentDirectory, '_headers'), path.join(outputDirectory, '_headers'))
      },
      name: 'copy-netlify-headers',
    },
  ],
  resolve: {
    alias: {
      'html-to-pdf-crab-js-wasm32-wasi': path.resolve(
        currentDirectory,
        '../../packages/html-to-pdf-crab-js/html-to-pdf-crab-js.wasi-browser.js',
      ),
    },
  },
  server: {
    fs: {
      allow: [workspaceRoot],
    },
    headers: {
      'Cross-Origin-Embedder-Policy': 'require-corp',
      'Cross-Origin-Opener-Policy': 'same-origin',
    },
    open: true,
  },
})
