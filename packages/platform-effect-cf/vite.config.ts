import { cloudflareTest } from "@cloudflare/vitest-plugin";
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
    deps: {
      // tsdown <0.23 compatibility: resolve external dependency subpaths.
      // Remove to preserve subpath imports as written (the new default).
      // https://tsdown.dev/options/dependencies#deps-resolvedepsubpath
      resolveDepSubpath: true,
    },
    entry: ["src/index.ts"],
    dts: { tsconfig: "tsconfig.build.json" },
    format: ["esm"],
    sourcemap: true,
  },
  test: {
    // Vitest v4 compatibility: preserve mock call history.
    // Remove after tests no longer rely on calls from setup or earlier tests.
    // https://viteplus.dev/guide/vitest-v5#remove-unneeded-compatibility-settings
    // https://vitest.dev/guide/migration/#clearmocks-is-enabled-by-default
    clearMocks: false,
    cache: false,
    silent: "passed-only",
  },
});
