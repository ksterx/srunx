import { NavLink } from "react-router-dom";
import {
  LayoutDashboard,
  Layers,
  GitFork,
  Cpu,
  Settings,
  Terminal,
  ChevronLeft,
  ChevronRight,
  FolderTree,
} from "lucide-react";
import { useState } from "react";
import { motion, AnimatePresence } from "framer-motion";

type NavItem = {
  to: string;
  icon: React.ReactNode;
  label: string;
};

const NAV_ITEMS: NavItem[] = [
  { to: "/", icon: <LayoutDashboard size={18} />, label: "Dashboard" },
  { to: "/jobs", icon: <Layers size={18} />, label: "Jobs" },
  { to: "/workflows", icon: <GitFork size={18} />, label: "Workflows" },
  { to: "/resources", icon: <Cpu size={18} />, label: "Resources" },
  { to: "/settings", icon: <Settings size={18} />, label: "Settings" },
];

type SidebarProps = {
  explorerOpen: boolean;
  onToggleExplorer: () => void;
  onNavigate: () => void;
};

export function Sidebar({
  explorerOpen,
  onToggleExplorer,
  onNavigate,
}: SidebarProps) {
  const [collapsed, setCollapsed] = useState(false);

  return (
    <motion.aside
      className="sidebar"
      animate={{ width: collapsed ? 64 : 240 }}
      transition={{ duration: 0.25, ease: [0.16, 1, 0.3, 1] }}
      style={{
        height: "100%",
        background: "var(--bg-surface)",
        borderRight: "1px solid var(--border-subtle)",
        display: "flex",
        flexDirection: "column",
        overflow: "hidden",
        flexShrink: 0,
        position: "relative",
      }}
    >
      {/* Logo */}
      <div
        style={{
          padding: collapsed ? "20px 0" : "20px 20px",
          display: "flex",
          alignItems: "center",
          justifyContent: collapsed ? "center" : "flex-start",
          gap: 10,
          borderBottom: "1px solid var(--border-ghost)",
          minHeight: 64,
        }}
      >
        <div
          style={{
            width: 28,
            height: 28,
            borderRadius: 6,
            background:
              "linear-gradient(135deg, var(--accent), var(--resource))",
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            flexShrink: 0,
          }}
        >
          <Terminal size={15} color="#fff" strokeWidth={2.5} />
        </div>
        <AnimatePresence>
          {!collapsed && (
            <motion.span
              initial={{ opacity: 0, x: -8 }}
              animate={{ opacity: 1, x: 0 }}
              exit={{ opacity: 0, x: -8 }}
              transition={{ duration: 0.15 }}
              style={{
                fontFamily: "var(--font-display)",
                fontWeight: 700,
                fontSize: "1.1rem",
                letterSpacing: "0.06em",
                color: "var(--text-primary)",
                whiteSpace: "nowrap",
              }}
            >
              srunx
            </motion.span>
          )}
        </AnimatePresence>
      </div>

      {/* Navigation */}
      <nav
        style={{
          flex: 1,
          padding: "12px 8px",
          display: "flex",
          flexDirection: "column",
          gap: 2,
        }}
      >
        {/* Explorer toggle */}
        <button
          onClick={onToggleExplorer}
          style={{
            display: "flex",
            alignItems: "center",
            gap: 12,
            padding: collapsed ? "10px 0" : "10px 12px",
            justifyContent: collapsed ? "center" : "flex-start",
            borderRadius: 6,
            textDecoration: "none",
            fontSize: "0.875rem",
            fontWeight: 500,
            fontFamily: "var(--font-body)",
            color: explorerOpen
              ? "var(--text-primary)"
              : "var(--text-secondary)",
            background: explorerOpen ? "var(--accent-dim)" : "transparent",
            borderLeft: explorerOpen
              ? "2px solid var(--accent)"
              : "2px solid transparent",
            border: "none",
            cursor: "pointer",
            transition: "all 150ms cubic-bezier(0.16,1,0.3,1)",
            width: "100%",
          }}
          onMouseEnter={(e) => {
            if (!explorerOpen)
              e.currentTarget.style.background = "var(--bg-hover)";
          }}
          onMouseLeave={(e) => {
            if (!explorerOpen) e.currentTarget.style.background = "transparent";
          }}
        >
          <span style={{ flexShrink: 0, display: "flex" }}>
            <FolderTree size={18} />
          </span>
          <AnimatePresence>
            {!collapsed && (
              <motion.span
                initial={{ opacity: 0, width: 0 }}
                animate={{ opacity: 1, width: "auto" }}
                exit={{ opacity: 0, width: 0 }}
                transition={{ duration: 0.15 }}
                style={{ whiteSpace: "nowrap", overflow: "hidden" }}
              >
                Explorer
              </motion.span>
            )}
          </AnimatePresence>
        </button>

        {/* Separator */}
        <div
          style={{
            height: 1,
            background: "var(--border-ghost)",
            margin: "4px 12px",
          }}
        />

        {NAV_ITEMS.map((item) => (
          <NavLink
            key={item.to}
            to={item.to}
            end={item.to === "/"}
            onClick={onNavigate}
            style={({ isActive }) => {
              const active = isActive && !explorerOpen;
              return {
                display: "flex",
                alignItems: "center",
                gap: 12,
                padding: collapsed ? "10px 0" : "10px 12px",
                justifyContent: collapsed ? "center" : "flex-start",
                borderRadius: 6,
                textDecoration: "none",
                fontSize: "0.875rem",
                fontWeight: 500,
                fontFamily: "var(--font-body)",
                color: active ? "var(--text-primary)" : "var(--text-secondary)",
                background: active ? "var(--accent-dim)" : "transparent",
                borderLeft: active
                  ? "2px solid var(--accent)"
                  : "2px solid transparent",
                transition: "all 150ms cubic-bezier(0.16,1,0.3,1)",
              };
            }}
          >
            <span style={{ flexShrink: 0, display: "flex" }}>{item.icon}</span>
            <AnimatePresence>
              {!collapsed && (
                <motion.span
                  initial={{ opacity: 0, width: 0 }}
                  animate={{ opacity: 1, width: "auto" }}
                  exit={{ opacity: 0, width: 0 }}
                  transition={{ duration: 0.15 }}
                  style={{ whiteSpace: "nowrap", overflow: "hidden" }}
                >
                  {item.label}
                </motion.span>
              )}
            </AnimatePresence>
          </NavLink>
        ))}
      </nav>

      {/* Collapse toggle */}
      <button
        onClick={() => setCollapsed((c) => !c)}
        style={{
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
          padding: "14px 0",
          borderTop: "1px solid var(--border-ghost)",
          background: "transparent",
          color: "var(--text-muted)",
          cursor: "pointer",
          border: "none",
          borderTopStyle: "solid",
          borderTopWidth: 1,
          borderTopColor: "var(--border-ghost)",
          transition: "color 150ms",
        }}
        onMouseEnter={(e) =>
          (e.currentTarget.style.color = "var(--text-secondary)")
        }
        onMouseLeave={(e) =>
          (e.currentTarget.style.color = "var(--text-muted)")
        }
      >
        {collapsed ? <ChevronRight size={16} /> : <ChevronLeft size={16} />}
      </button>
    </motion.aside>
  );
}
