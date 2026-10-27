import starlight from "@astrojs/starlight";
import yieldedTheme from "@yielded/starlight-theme";
import { defineConfig } from "astro/config";
import starlightLinksValidator from "starlight-links-validator";

export default defineConfig({
  site: "https://yielded.dev",
  base: "/sync",
  integrations: [
    starlight({
      title: "Yielded Sync",
      description: "Effect-native realtime synchronization with exact retries and scoped clients.",
      favicon: "/favicon.svg",
      plugins: [yieldedTheme({ library: "sync" }), starlightLinksValidator()],
      sidebar: [
        {
          label: "Start",
          items: [
            { label: "Why Sync?", slug: "guide/why-sync" },
            { label: "Getting started", slug: "guide/getting-started" },
          ],
        },
        {
          label: "Build",
          items: [
            { label: "How Sync works", slug: "guide/concepts" },
            { label: "Server actions", slug: "api/consumer" },
            { label: "Clients and React", slug: "client" },
          ],
        },
        {
          label: "Reference",
          items: [
            { label: "Packages and hosts", slug: "reference/packages" },
            { label: "Releases and upgrades", slug: "release" },
          ],
        },
      ],
    }),
  ],
});
