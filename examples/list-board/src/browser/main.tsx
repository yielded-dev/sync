import { RegistryProvider } from "@effect/atom-react";
import { StrictMode } from "react";
import { createRoot } from "react-dom/client";

import { App } from "./App.tsx";

import "./styles.css";

const root = document.getElementById("root");

if (root === null) throw new Error("The board root element is missing");

createRoot(root).render(
  <StrictMode>
    <RegistryProvider>
      <App />
    </RegistryProvider>
  </StrictMode>,
);
