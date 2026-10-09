import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// Dev server: http://localhost:5175. /api -> platform API; /backend -> existing
// pipeline backend (read-only use). The Docker image serves the same build via
// nginx on :5174 with identical routes (see nginx.conf).
export default defineConfig({
  plugins: [react()],
  server: {
    port: 5175,
    proxy: {
      '/api': { target: process.env.PLATFORM_API_URL ?? 'http://localhost:8001', changeOrigin: true },
      '/backend': {
        target: process.env.BACKEND_URL ?? 'http://localhost:8000',
        changeOrigin: true,
        rewrite: (p) => p.replace(/^\/backend/, ''),
      },
    },
  },
})
