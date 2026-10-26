import * as api from "./browser.ts";

declare global {
  interface Window {
    persistence: typeof api;
  }
}

window.persistence = api;
