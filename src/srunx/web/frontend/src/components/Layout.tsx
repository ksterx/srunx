import { useState } from "react";
import { Outlet } from "react-router-dom";
import { AnimatePresence } from "framer-motion";
import { Sidebar } from "./Sidebar.tsx";
import { FileExplorer } from "./FileExplorer.tsx";

export function Layout() {
  const [explorerOpen, setExplorerOpen] = useState(false);

  return (
    <div style={{ display: "flex", width: "100%", height: "100%" }}>
      <Sidebar
        explorerOpen={explorerOpen}
        onToggleExplorer={() => setExplorerOpen((v) => !v)}
      />
      <AnimatePresence>
        {explorerOpen && (
          <FileExplorer onClose={() => setExplorerOpen(false)} />
        )}
      </AnimatePresence>
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
    </div>
  );
}
