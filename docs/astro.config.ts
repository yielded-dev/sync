import starlight from "@astrojs/starlight";
import yieldedTheme from "@yielded/starlight-theme";
import { defineConfig } from "astro/config";
import starlightLinksValidator from "starlight-links-validator";

import syncPackage from "../packages/sync/package.json" with { type: "json" };

export default defineConfig({
  site: "https://yielded.dev",
  base: "/sync",
  integrations: [
    starlight({
      title: "Yielded Sync",
      description: "Effect-native realtime synchronization with exact retries and scoped clients.",
      favicon: "/favicon.svg",
      plugins: [
        yieldedTheme({ library: "sync", replacements: { SYNC_VERSION: syncPackage.version } }),
        starlightLinksValidator(),
      ],
      sidebar: [
        {
          label: "Start",
          items: [
            { label: "Getting started", slug: "guide/getting-started" },
            { label: "Packages and hosts", slug: "reference/packages" },
          ],
        },
        {
          label: "Build",
          items: [
            { label: "How Sync works", slug: "guide/concepts" },
            { label: "Consumer example", slug: "api/consumer" },
            { label: "Client and Atom", slug: "client" },
          ],
        },
        {
          label: "Reference",
          items: [{ label: "Beta release", slug: "release" }],
        },
      ],
    }),
  ],
});
