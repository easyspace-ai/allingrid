import typescript from 'rollup-plugin-typescript2'
import terser from '@rollup/plugin-terser'
import alias from '@rollup/plugin-alias'
import { readFileSync } from 'fs'
import { resolve, dirname } from 'path'
import { fileURLToPath } from 'url'

const __filename = fileURLToPath(import.meta.url)
const __dirname = dirname(__filename)

const pkg = JSON.parse(readFileSync('./package.json', 'utf8'))

export default [
  // ES Module (Browser-compatible)
  {
    input: 'src/index.ts',
    output: {
      file: 'dist/luckdb.es.mjs',
      format: 'es',
      sourcemap: true
    },
    plugins: [
      alias({
        entries: [
          { find: /^node:events$/, replacement: resolve(__dirname, 'src/utils/EventEmitter.ts') },
          { find: /^events$/, replacement: resolve(__dirname, 'src/utils/EventEmitter.ts') }
        ]
      }),
      typescript({
        tsconfig: './tsconfig.json',
        useTsconfigDeclarationDir: true
      }),
      terser({
        compress: {
          drop_console: false, // Keep console for debugging in browser
          drop_debugger: true
        }
      })
    ],
    external: [
      'sharedb',
      'reconnecting-websocket',
      '@teamwork/websocket-json-stream'
    ]
  },
  // CommonJS
  {
    input: 'src/index.ts',
    output: {
      file: 'dist/luckdb.cjs.js',
      format: 'cjs',
      sourcemap: true,
      exports: 'named'
    },
    plugins: [
      typescript({
        tsconfig: './tsconfig.json',
        useTsconfigDeclarationDir: true
      }),
      terser({
        compress: {
          drop_console: true,
          drop_debugger: true
        }
      })
    ],
    external: [
      'sharedb',
      'reconnecting-websocket',
      '@teamwork/websocket-json-stream'
    ]
  },
  // UMD (bundled with dependencies)
  {
    input: 'src/index.ts',
    output: {
      file: 'dist/luckdb.umd.js',
      format: 'umd',
      name: 'LuckDB',
      sourcemap: true,
      globals: {
        'sharedb': 'ShareDB',
        'reconnecting-websocket': 'ReconnectingWebSocket',
        '@teamwork/websocket-json-stream': 'WebSocketJSONStream'
      }
    },
    plugins: [
      alias({
        entries: [
          { find: /^node:events$/, replacement: resolve(__dirname, 'src/utils/EventEmitter.ts') },
          { find: /^events$/, replacement: resolve(__dirname, 'src/utils/EventEmitter.ts') }
        ]
      }),
      typescript({
        tsconfig: './tsconfig.json',
        useTsconfigDeclarationDir: true
      }),
      terser({
        compress: {
          drop_console: false,
          drop_debugger: true
        }
      })
    ]
  },
  // IIFE (bundled with dependencies)
  {
    input: 'src/index.ts',
    output: {
      file: 'dist/luckdb.iife.js',
      format: 'iife',
      name: 'LuckDB',
      sourcemap: true
    },
    plugins: [
      alias({
        entries: [
          { find: /^node:events$/, replacement: resolve(__dirname, 'src/utils/EventEmitter.ts') },
          { find: /^events$/, replacement: resolve(__dirname, 'src/utils/EventEmitter.ts') }
        ]
      }),
      typescript({
        tsconfig: './tsconfig.json',
        useTsconfigDeclarationDir: true
      }),
      terser({
        compress: {
          drop_console: false,
          drop_debugger: true
        }
      })
    ]
  }
]
