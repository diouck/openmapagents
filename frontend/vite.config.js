import { defineConfig } from "vite"
import react from "@vitejs/plugin-react"
import { resolve } from "path"

export default defineConfig({
  plugins: [react()],
  // Force le runtime JSX automatique (react/jsx-runtime) au build comme en dev.
  // Évite « React is not defined » si le plugin n'applique pas l'auto-runtime
  // (ex. désalignement de versions vite/plugin-react).
  esbuild: {
    jsx: "automatic",
    jsxImportSource: "react",
  },
  build: {
    chunkSizeWarningLimit: 1500,
    rollupOptions: {
      input: {
        main: resolve(__dirname, "index.html"),   // landing page → /
        app:  resolve(__dirname, "app.html"),      // React app   → /app.html
      },
      output: {
        // Regroupe deck.gl + luma.gl + loaders.gl + math.gl dans un seul chunk
        // (corrige la dépendance circulaire entre chunks signalée par Rollup et
        //  le risque d'ordre d'exécution cassé). Chargé en lazy via deck3d.js.
        manualChunks(id) {
          if (id.includes("node_modules")) {
            if (/[\\/](@deck\.gl|@luma\.gl|@loaders\.gl|@math\.gl|deck\.gl)[\\/]/.test(id)) {
              return "deckgl";
            }
            if (id.includes("@mkkellogg/gaussian-splats-3d") || /[\\/]three[\\/]/.test(id)) {
              return "splats";
            }
          }
        },
      },
    }
  },
  server: {
    port: 5173,
    proxy: {
      "/api": {
        target: "http://127.0.0.1:8000",
        changeOrigin: true,
        secure: false,
        timeout: 120000,
        proxyTimeout: 120000,
      }
    }
  }
})
