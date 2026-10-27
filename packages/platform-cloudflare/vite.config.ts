import { defineConfig } from "vite-plus";

export default defineConfig({
  pack: {
    entry: {
      index: "src/index.ts",
      Cloudflare: "src/Cloudflare.ts",
      DurableObject: "src/DurableObject.ts",
      SqliteStorage: "src/SqliteStorage.ts",
    },
    dts: { tsconfig: "tsconfig.build.json" },
    format: ["esm"],
    sourcemap: true,
  },
});
