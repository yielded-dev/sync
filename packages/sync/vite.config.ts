import { defineConfig } from "vite-plus";

export default defineConfig({
  pack: {
    entry: {
      index: "src/index.ts",
      server: "src/server.ts",
      client: "src/client.ts",
      atom: "src/atom.ts",
      Action: "src/Action.ts",
      Model: "src/Model.ts",
      Plugin: "src/Plugin.ts",
      Source: "src/Source.ts",
      RegistrationError: "src/RegistrationError.ts",
      "server/Server": "src/server/Server.ts",
      "server/SourceStorage": "src/server/SourceStorage.ts",
      "server/Host": "src/server/Host.ts",
      "server/Rpc": "src/server/Rpc.ts",
      "client/Client": "src/client/Client.ts",
      "client/CurrentActor": "src/client/CurrentActor.ts",
      "client/Model": "src/client/Model.ts",
      "client/ReplicaPersistence": "src/client/ReplicaPersistence.ts",
      "client/Transport": "src/client/Transport.ts",
      "client/View": "src/client/View.ts",
      "atom/SourceAtom": "src/atom/SourceAtom.ts",
    },
    dts: { tsconfig: "tsconfig.build.json" },
    format: ["esm"],
    sourcemap: true,
  },
  test: {
    cache: false,
    silent: "passed-only",
  },
});
