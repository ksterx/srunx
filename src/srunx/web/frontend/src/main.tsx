import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { BrowserRouter } from "react-router-dom";
import { App } from "./App.tsx";
import { installAuthFetch } from "./lib/auth.ts";
import { installSelectionFetch, refreshSelection } from "./lib/selection.ts";
import "./styles/globals.css";

// Attach the selected cluster/project and the bearer token (if any) to
// /api/* requests before the app mounts.
installSelectionFetch();
installAuthFetch();
void refreshSelection();

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <BrowserRouter>
      <App />
    </BrowserRouter>
  </StrictMode>,
);
