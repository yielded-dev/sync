import { cloudflareTest } from "@cloudflare/vitest-pool-workers";
import { defineConfig } from "vite-plus";

export default defineConfig({
  plugins: [
    cloudflareTest({
      main: "./test/fixtures/worker.ts",
      wrangler: { configPath: "../../examples/cloudflare/wrangler.jsonc" },
      miniflare: {
        durableObjects: { PUBLICATION: { className: "PublicationObject", useSQLite: true } },
      },
    }),
  ],
  pack: {
    entry: ["src/index.ts"],
    dts: { tsconfig: "tsconfig.build.json" },
    format: ["esm"],
    sourcemap: true,
  },
  test: { cache: false, silent: "passed-only" },
});
