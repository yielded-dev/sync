import { cloudflare } from "@cloudflare/vite-plugin";
import { cloudflareTest } from "@cloudflare/vitest-pool-workers";
import react from "@vitejs/plugin-react";
import { defineConfig } from "vite-plus";

export default defineConfig(({ mode }) => ({
  plugins:
    mode === "test"
      ? [
          cloudflareTest({
            main: "./src/worker.ts",
            wrangler: { configPath: "./wrangler.jsonc" },
          }),
        ]
      : [...react(), ...cloudflare()],
  server: { host: "127.0.0.1" },
  test: { cache: false, silent: "passed-only" },
}));
