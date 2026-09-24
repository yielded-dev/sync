import { defineConfig } from "vitepress";

import syncPackage from "../../packages/sync/package.json" with { type: "json" };

export default defineConfig({
  title: "Yielded Sync",
  description: "Effect-native realtime synchronization with exact retries and scoped clients.",
  lang: "en-US",
  base: "/sync/",
  cleanUrls: true,
  sitemap: { hostname: "https://yielded.dev/sync/" },
  markdown: {
    config(md) {
      md.core.ruler.before("normalize", "sync-version", (state) => {
        state.src = state.src.replaceAll("{{SYNC_VERSION}}", syncPackage.version);
      });
    },
  },
  head: [["link", { rel: "icon", type: "image/svg+xml", href: "/sync/favicon.svg" }]],
  srcExclude: ["PUBLIC_API.md", "TOOLCHAIN.md", "slide-deck-pilot.md"],
  themeConfig: {
    siteTitle: "Yielded Sync",
    nav: [
      { text: "Get started", link: "/guide/getting-started" },
      { text: "Reference", link: "/reference/packages" },
      { text: "Auth", link: "https://yielded.dev/auth/" },
    ],
    sidebar: [
      {
        text: "Start",
        items: [
          { text: "Getting started", link: "/guide/getting-started" },
          { text: "Packages and hosts", link: "/reference/packages" },
        ],
      },
      {
        text: "Build",
        items: [
          { text: "How Sync works", link: "/guide/concepts" },
          { text: "Consumer example", link: "/api/consumer" },
          { text: "Client and Atom", link: "/client" },
        ],
      },
      {
        text: "Reference",
        items: [{ text: "Beta release", link: "/RELEASE" }],
      },
    ],
    socialLinks: [{ icon: "github", link: "https://github.com/yielded-dev/sync" }],
    search: { provider: "local" },
    outline: { level: [2, 3], label: "On this page" },
    docFooter: { prev: "Previous", next: "Continue" },
    editLink: { pattern: "https://github.com/yielded-dev/sync/edit/main/docs/:path" },
    externalLinkIcon: true,
  },
});
