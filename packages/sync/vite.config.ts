import { defineConfig } from "vite-plus";

export default defineConfig({
  pack: {
    entry: ["src/index.ts", "src/server.ts", "src/client.ts", "src/atom.ts"],
    dts: true,
    format: ["esm"],
    sourcemap: true,
  },
  test: { cache: false, silent: "passed-only" },
});
