import { cloudflareTest } from "@cloudflare/vitest-plugin";
import { defineConfig } from "vite-plus";

export default defineConfig({
  plugins: [
    cloudflareTest({
      main: "./test/fixtures/worker.ts",
      wrangler: { configPath: "./test/wrangler.jsonc" },
      miniflare: {
        durableObjects: { PUBLICATION: { className: "PublicationObject", useSQLite: true } },
      },
    }),
  ],
  pack: {
    entry: {
      index: "src/index.ts",
      EffectCfServices: "src/EffectCfServices.ts",
    },
    dts: { tsconfig: "tsconfig.build.json" },
    format: ["esm"],
    sourcemap: true,
  },
  test: {
    cache: false,
    // workerd cannot read Vitest's module cache on the host filesystem.
    fsModuleCache: false,
    silent: "passed-only",
  },
});
