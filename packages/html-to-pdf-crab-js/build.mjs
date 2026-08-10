import { readFile, writeFile } from 'node:fs/promises'

import { NapiCli } from '@napi-rs/cli'

const napi = new NapiCli()

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

const useAsyncBrowserWasmInstantiation = async (fileName) => {
  const path = new URL(`./${fileName}`, import.meta.url)
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

const preserveHighLevelBrowserEntry = async (fileName) => {
  // NAPI-RS uses the metadata export list to replace browser.js with a raw flavor re-export.
  // Keep this package's high-level browser facade instead.
  const path = new URL(`./${fileName}`, import.meta.url)
  let content

  try {
    content = await readFile(path, 'utf8')
  } catch (error) {
    if (error && typeof error === 'object' && 'code' in error && error.code === 'ENOENT') return
    throw error
  }

  const lineEnd = content.indexOf('\n')
  const metadataPrefix = '// napi-rs-artifact-metadata:'
  if (lineEnd === -1 || !content.startsWith(metadataPrefix)) return

  const metadata = JSON.parse(content.slice(metadataPrefix.length, lineEnd))
  if (!Array.isArray(metadata.exports)) return

  delete metadata.exports
  await writeFile(path, `${metadataPrefix}${JSON.stringify(metadata)}\n${content.slice(lineEnd + 1)}`)
}

const writeBrowserEntrypoints = async () => {
  await writeFile(new URL('./browser.js', import.meta.url), "export * from './html-to-pdf-crab-js.wasip1-browser.js'\n")
  await writeFile(
    new URL('./browser-threaded.js', import.meta.url),
    "export * from './html-to-pdf-crab-js.wasi-browser.js'\n",
  )
}

const build = async () => {
  const target = argValue('--target')
  const isWasi = target?.startsWith('wasm32-wasi') ?? false
  const isThreadedWasi = target?.endsWith('-threads') ?? false
  const commonOptions = {
    constEnum: false,
    crossCompile: hasFlag('-x', '--cross-compile'),
    dts: isWasi ? 'index.d.cts' : 'index.d.ts',
    platform: true,
    release: !hasFlag('--debug'),
    target,
  }

  const result = await napi.build({
    ...commonOptions,
    esm: true,
    jsBinding: 'index.js',
  })
  await result.task

  if (isWasi) {
    await preserveHighLevelBrowserEntry(
      isThreadedWasi ? 'html-to-pdf-crab-js.wasi.cjs' : 'html-to-pdf-crab-js.wasip1.cjs',
    )
    await useAsyncBrowserWasmInstantiation(
      isThreadedWasi ? 'html-to-pdf-crab-js.wasi-browser.js' : 'html-to-pdf-crab-js.wasip1-browser.js',
    )
    await writeBrowserEntrypoints()
  }
}

build().catch((error) => {
  console.error('Build failed:', error)
  process.exit(1)
})
