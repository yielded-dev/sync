import { defineConfig } from "vite-plus";

export default defineConfig({
  pack: {
    entry: ["src/index.ts"],
    dts: { tsconfig: "tsconfig.build.json" },
    format: ["esm"],
    sourcemap: true,
  },
  test: { cache: false, silent: "passed-only" },
});
