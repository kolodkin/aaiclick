import { defineConfig } from "vitest/config";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";

export default defineConfig({
  plugins: [react(), tailwindcss()],
  // shortcut: aaiclick builds and serves no widget bundle yet, so a dashboard's
  // <qv-*> tags stay unrendered; serve /widgets.js to adopt them (future.md).
  define: { __WIDGETS_URL__: JSON.stringify("/widgets.js") },
  resolve: {
    alias: { "@qv/core": "/src/queryview-core/index.ts" },
  },
  build: {
    outDir: "aaiclick/server/static",
    emptyOutDir: true,
  },
  server: {
    proxy: {
      "/api": "http://localhost:8000",
      "/mcp": "http://localhost:8000",
    },
  },
  test: {
    environment: "node",
    include: ["src/**/*.test.{ts,tsx}"],
  },
});
