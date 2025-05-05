import * as esbuild from 'esbuild'

const commonConfig = {
  entryPoints: ['./js-src/index.ts'],
  platform: 'node',
  bundle: true,
  sourcemap: true,
  external: ['../js-binding.js', '../js-binding.d.ts', '../js-binding.cjs'],
  outdir: './dist',
}

await esbuild.build({
  ...commonConfig,
  entryPoints: ['./js-src/index.ts'],
  format: 'esm',
})

await esbuild.build({
  ...commonConfig,
  format: 'cjs',
  outExtension: { '.js': '.cjs' },
  plugins: [
    {
      name: 'js-binding-path-replacer',
      setup(build) {
        // Replace js-binding.js imports with js-binding.cjs
        build.onResolve({ filter: /\.\.\/js-binding\.js$/ }, args => {
          return {
            path: '../js-binding.cjs',
            external: true,
          }
        })
      },
    },
  ],
})
