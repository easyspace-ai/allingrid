import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import path from 'path'

// https://vite.dev/config/
export default defineConfig({
  plugins: [react()],
  resolve: {
    alias: {
      "@": path.resolve(__dirname, "./src"),
      // 使用 SDK 构建版本（dist）
      "luckdb-sdk": path.resolve(__dirname, "../sdk/dist/luckdb.es.mjs"),
    },
  },
})
