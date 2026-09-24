import { cloudflareTest } from "@cloudflare/vitest-pool-workers";
import { defineConfig } from "vite-plus";

export default defineConfig({
  plugins: [
    cloudflareTest({
      main: "./src/worker.ts",
      wrangler: { configPath: "./wrangler.jsonc" },
    }),
  ],
  test: { cache: false, silent: "passed-only" },
});
