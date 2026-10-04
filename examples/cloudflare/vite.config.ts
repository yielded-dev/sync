import { defineConfig } from "vite-plus";

export default defineConfig({
  run: {
    tasks: {
      build: {
        command: "wrangler deploy --dry-run --outdir dist",
        input: [
          { auto: true },
          "*",
          // Excluding the workspace root (".") would drop every file input.
          { pattern: "!examples/cloudflare", base: "workspace" },
          "src/**",
          "!.wrangler",
          "!.wrangler/**",
          "!dist",
          "!dist/**",
          { pattern: "bun.lock", base: "workspace" },
          { pattern: "!**/node_modules", base: "workspace" },
          { pattern: "!**/node_modules/.vite*", base: "workspace" },
          { pattern: "!**/node_modules/.vite*/**", base: "workspace" },
        ],
        output: ["dist/**"],
      },
    },
  },
});
