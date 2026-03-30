import { Outlet } from "react-router-dom";
import { Sidebar } from "./Sidebar.tsx";

export function Layout() {
  return (
    <div style={{ display: "flex", width: "100%", height: "100%" }}>
      <Sidebar />
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
