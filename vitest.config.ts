import { defineConfig } from "vitest/config";

export default defineConfig({
  resolve: {
    alias: { "@qv/core": "/src/queryview-core/index.ts" },
  },
  test: {
    environment: "node",
    include: ["src/**/*.test.{ts,tsx}"],
  },
});
