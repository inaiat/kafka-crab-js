import { defineConfig } from 'vite-plus'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import { sharedFmtConfig, sharedLintConfig } from '../../vite.shared.mjs'

const currentDirectory = path.dirname(fileURLToPath(import.meta.url))
const workspaceRoot = path.resolve(currentDirectory, '../..')

export default defineConfig({
  build: {
    rollupOptions: {
      input: {
        stream: path.resolve(currentDirectory, 'stream/index.html'),
        wasm: path.resolve(currentDirectory, 'wasm/index.html'),
      },
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
        files: ['src/browser/*.ts'],
        rules: {
          'unicorn/prefer-node-protocol': 'off',
        },
      },
    ],
  },
  optimizeDeps: {
    exclude: ['pdf-crab-js', 'pdf-crab-js/browser.js'],
  },
  server: {
    fs: {
      allow: [workspaceRoot],
    },
    open: '/wasm/',
  },
})
