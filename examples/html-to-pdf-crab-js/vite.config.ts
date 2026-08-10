import { defineConfig } from 'vite-plus'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import { sharedFmtConfig, sharedLintConfig } from '../../vite.shared.mjs'

const currentDirectory = path.dirname(fileURLToPath(import.meta.url))
const workspaceRoot = path.resolve(currentDirectory, '../..')

export default defineConfig({
  build: {
    rollupOptions: {
      input: path.resolve(currentDirectory, 'wasm/index.html'),
    },
    target: 'esnext',
  },
  fmt: {
    ...sharedFmtConfig,
  },
  lint: {
    ...sharedLintConfig,
    overrides: [
      {
        files: ['**/*.ts'],
        rules: {
          'id-length': 'off',
          'no-console': 'off',
        },
      },
      {
        files: ['wasm/browser.ts'],
        rules: {
          'unicorn/prefer-node-protocol': 'off',
        },
      },
    ],
  },
  optimizeDeps: {
    exclude: ['html-to-pdf-crab-js', 'html-to-pdf-crab-js/browser.js', 'html-to-pdf-crab-js-wasm32-wasip1'],
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
    open: '/wasm/',
  },
})
