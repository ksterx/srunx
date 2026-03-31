import { lazy, Suspense, useState } from "react";
import { Outlet } from "react-router-dom";
import { Sidebar } from "./Sidebar.tsx";

const FileExplorer = lazy(() =>
  import("./FileExplorer.tsx").then((m) => ({ default: m.FileExplorer })),
);

export function Layout() {
  const [explorerOpen, setExplorerOpen] = useState(false);

  return (
    <div style={{ display: "flex", width: "100%", height: "100%" }}>
      <Sidebar
        explorerOpen={explorerOpen}
        onToggleExplorer={() => setExplorerOpen((v) => !v)}
        onNavigate={() => setExplorerOpen(false)}
      />
      {explorerOpen ? (
        <Suspense
          fallback={
            <div
              style={{
                flex: 1,
                display: "flex",
                alignItems: "center",
                justifyContent: "center",
                color: "var(--text-muted)",
                fontSize: "0.8rem",
              }}
            >
              Loading Explorer...
            </div>
          }
        >
          <FileExplorer />
        </Suspense>
      ) : (
        <main
          className="grid-bg"
          style={{
            flex: 1,
            overflow: "auto",
            padding: "var(--sp-6)",
            position: "relative",
          }}
        >
          <Outlet />
        </main>
      )}
    </div>
  );
}
