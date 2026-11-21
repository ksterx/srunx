import { motion } from "framer-motion";
import { useSearchParams } from "react-router-dom";
import { FolderOpen, Server, Bell, Variable, Sliders } from "lucide-react";
import { GeneralTab } from "./settings/GeneralTab.tsx";
import { ClustersTab } from "./settings/ClustersTab.tsx";
import { NotificationsTab } from "./settings/NotificationsTab.tsx";
import { EnvironmentTab } from "./settings/EnvironmentTab.tsx";
import { ProjectTab } from "./settings/ProjectTab.tsx";

const EASE = [0.16, 1, 0.3, 1] as [number, number, number, number];

const stagger = (i: number) => ({
  initial: { opacity: 0, y: 12 },
  animate: { opacity: 1, y: 0 },
  transition: { delay: i * 0.06, duration: 0.4, ease: EASE },
});

/* ── Tab definitions ─────────────────────────── */

type TabId =
  | "general"
  | "clusters"
  | "notifications"
  | "environment"
  | "project";

type TabDef = {
  id: TabId;
  label: string;
  icon: React.ReactNode;
};

const TABS: TabDef[] = [
  { id: "general", label: "General", icon: <Sliders size={14} /> },
  { id: "clusters", label: "Clusters", icon: <Server size={14} /> },
  { id: "notifications", label: "Notifications", icon: <Bell size={14} /> },
  { id: "environment", label: "Environment", icon: <Variable size={14} /> },
  { id: "project", label: "Project", icon: <FolderOpen size={14} /> },
];

/* ── Main Settings component ─────────────────── */

export function Settings() {
  const [searchParams, setSearchParams] = useSearchParams();
  const requested = searchParams.get("tab");
  const activeTab: TabId = TABS.some((t) => t.id === requested)
    ? (requested as TabId)
    : "general";
  const setActiveTab = (id: TabId) =>
    setSearchParams(id === "general" ? {} : { tab: id }, { replace: true });

  return (
    <div
      style={{ display: "flex", flexDirection: "column", gap: "var(--sp-6)" }}
    >
      {/* Header */}
      <motion.div {...stagger(0)}>
        <h1 style={{ marginBottom: 4 }}>Settings</h1>
        <p style={{ color: "var(--text-muted)", fontSize: "0.85rem" }}>
          Manage clusters, layered defaults, project settings, and notifications
        </p>
      </motion.div>

      {/* Tab bar */}
      <motion.div
        {...stagger(1)}
        style={{
          display: "flex",
          gap: 2,
          borderBottom: "1px solid var(--border-subtle)",
          paddingBottom: 0,
        }}
      >
        {TABS.map((tab) => (
          <button
            key={tab.id}
            onClick={() => setActiveTab(tab.id)}
            style={{
              display: "flex",
              alignItems: "center",
              gap: "var(--sp-2)",
              padding: "var(--sp-3) var(--sp-4)",
              background: "transparent",
              border: "none",
              borderBottom:
                activeTab === tab.id
                  ? "2px solid var(--accent)"
                  : "2px solid transparent",
              color:
                activeTab === tab.id
                  ? "var(--text-primary)"
                  : "var(--text-muted)",
              fontFamily: "var(--font-display)",
              fontSize: "0.8rem",
              fontWeight: 500,
              letterSpacing: "0.04em",
              cursor: "pointer",
              transition: "all 150ms var(--ease-out)",
            }}
          >
            {tab.icon}
            {tab.label}
          </button>
        ))}
      </motion.div>

      {/* Tab content */}
      {activeTab === "general" && <GeneralTab />}
      {activeTab === "clusters" && <ClustersTab />}
      {activeTab === "notifications" && <NotificationsTab />}
      {activeTab === "environment" && <EnvironmentTab />}
      {activeTab === "project" && <ProjectTab />}
    </div>
  );
}
