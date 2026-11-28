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
    coverage: {
      provider: "v8",
      include: ["src/**"],
      reportsDirectory: "tmp/vitest-coverage",
      reporter: ["text-summary", "html"],
    },
    projects: [
      {
        extends: true,
        test: {
          name: "unit",
          environment: "node",
          include: ["src/**/*.test.{ts,tsx}"],
          exclude: ["src/**/*.browser.test.{ts,tsx}"],
        },
      },
      {
        // Component tests: render in real Chromium through Playwright.
        extends: true,
        test: {
          name: "browser",
          include: ["src/**/*.browser.test.{ts,tsx}"],
          browser: {
            enabled: true,
            provider: "playwright",
            headless: true,
            instances: [{ browser: "chromium" }],
            // Failure screenshots; the default lands beside the test, under src/.
            screenshotDirectory: "tmp/vitest-screenshots",
          },
        },
      },
    ],
  },
});
