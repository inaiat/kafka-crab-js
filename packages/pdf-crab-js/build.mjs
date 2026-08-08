import { readFile, unlink, writeFile } from 'node:fs/promises'

import { NapiCli } from '@napi-rs/cli'
import { dirname } from 'node:path'
import { fileURLToPath } from 'node:url'
import { build as pack } from 'vite-plus/pack'

const napi = new NapiCli()
const cwd = dirname(fileURLToPath(import.meta.url))

const argValue = (name) => {
  const prefix = `${name}=`
  const inline = process.argv.find((arg) => arg.startsWith(prefix))
  if (inline) {
    return inline.slice(prefix.length)
  }

  const index = process.argv.indexOf(name)
  return index === -1 ? undefined : process.argv[index + 1]
}

const hasFlag = (...names) => names.some((name) => process.argv.includes(name))

const removeIfPresent = async (fileName) => {
  try {
    await unlink(new URL(`./${fileName}`, import.meta.url))
  } catch (error) {
    if (!error || typeof error !== 'object' || !('code' in error) || error.code !== 'ENOENT') {
      throw error
    }
  }
}

const useAsyncBrowserWasmInstantiation = async () => {
  const path = new URL('./pdf-crab-js.wasi-browser.js', import.meta.url)
  let content

  try {
    content = await readFile(path, 'utf8')
  } catch (error) {
    if (error && typeof error === 'object' && 'code' in error && error.code === 'ENOENT') {
      return
    }

    throw error
  }

  if (!content.includes('__emnapiInstantiateNapiModuleSync')) {
    return
  }

  const rewritten = content
    .replaceAll(
      'instantiateNapiModuleSync as __emnapiInstantiateNapiModuleSync',
      'instantiateNapiModule as __emnapiInstantiateNapiModule',
    )
    .replaceAll('__emnapiInstantiateNapiModuleSync(', 'await __emnapiInstantiateNapiModule(')

  if (rewritten.includes('__emnapiInstantiateNapiModuleSync')) {
    throw new Error(`Failed to rewrite ${path} to async WASM instantiation`)
  }

  await writeFile(path, rewritten)
}

const writeBrowserEntrypoint = async (isWasi) => {
  const content = isWasi
    ? "export * from './dist/browser.js'\n"
    : `import * as binding from './pdf-crab-js.wasi-browser.js'

import { configurePdfRuntime, createPdf, createPdfAsync, PdfDocument } from './dist/api.js'

configurePdfRuntime({
  binding,
  resolveImageSource(source) {
    if (typeof source === 'string') {
      throw new TypeError('image file paths are only supported in the Node.js entrypoint; pass Uint8Array or ArrayBuffer')
    }
    if (source instanceof ArrayBuffer) return new Uint8Array(source)
    return new Uint8Array(source.buffer, source.byteOffset, source.byteLength)
  },
})

export { PdfDocument, createPdf, createPdfAsync }
`
  await writeFile(new URL('./browser.js', import.meta.url), content)
}

const build = async () => {
  const target = argValue('--target')
  const isWasi = target?.startsWith('wasm32-wasi') ?? false

  if (isWasi) {
    const result = await napi.build({
      constEnum: false,
      crossCompile: hasFlag('-x', '--cross-compile'),
      dts: 'index.d.cts',
      esm: true,
      platform: true,
      release: !hasFlag('--debug'),
      target,
      jsBinding: 'index.js',
    })
    await result.task
  } else {
    await removeIfPresent('index.js')
    await removeIfPresent('index.d.cts')

    const commonOptions = {
      constEnum: false,
      crossCompile: hasFlag('-x', '--cross-compile'),
      dts: 'js-binding.d.ts',
      platform: true,
      release: !hasFlag('--debug'),
      target,
    }

    let esmBinding
    for (const binding of [
      { jsBinding: 'js-binding.js', esm: true },
      { jsBinding: 'js-binding.cjs', noDtsHeader: true },
    ]) {
      const result = await napi.build({ ...commonOptions, ...binding })
      await result.task
      if (binding.esm) {
        esmBinding = await readFile(new URL('./js-binding.js', import.meta.url), 'utf8')
      }
    }
    if (esmBinding) {
      await writeFile(new URL('./js-binding.js', import.meta.url), esmBinding)
    }
  }

  await pack({
    checks: { legacyCjs: false },
    cwd,
    deps: { neverBundle: [/js-binding\.(?:js|cjs)$/, /^pdf-crab-js-wasm32-wasi$/] },
    dts: true,
    entry: isWasi ? 'js-src/**/*.ts' : ['js-src/index.ts', 'js-src/api.ts'],
    fixedExtension: false,
    format: isWasi ? ['esm'] : ['esm', 'cjs'],
    platform: 'node',
    report: false,
    sourcemap: true,
    target: isWasi ? 'esnext' : 'node24',
  })

  await useAsyncBrowserWasmInstantiation()
  await writeBrowserEntrypoint(isWasi)
}

build().catch((error) => {
  console.error('Build failed:', error)
  process.exit(1)
})
