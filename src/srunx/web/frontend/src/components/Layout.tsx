import { useLocation } from "react-router-dom";
import { Outlet } from "react-router-dom";
import { Sidebar } from "./Sidebar.tsx";
import { useSelection } from "../lib/selection.ts";

export function Layout() {
  const { pathname } = useLocation();
  const isExplorer = pathname === "/explorer";
  const { cluster, project, ready } = useSelection();

  return (
    <div style={{ display: "flex", width: "100%", height: "100%" }}>
      <Sidebar />
      <main
        className={isExplorer ? undefined : "grid-bg"}
        style={{
          flex: 1,
          overflow: "auto",
          padding: isExplorer ? 0 : "var(--sp-6)",
          position: "relative",
        }}
      >
        {/* Remount pages when the selection changes so they refetch. */}
        {ready && <Outlet key={`${cluster ?? ""}|${project ?? ""}`} />}
      </main>
    </div>
  );
}
