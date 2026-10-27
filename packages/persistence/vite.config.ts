import { defineConfig } from "vite-plus";

export default defineConfig({
  pack: {
    entry: {
      "index": "src/index.ts",
      "Persistence": "src/Persistence.ts",
    },
    dts: { tsconfig: "tsconfig.build.json" },
    format: ["esm"],
    sourcemap: true,
  },
});
