import { NavLink } from "react-router-dom";
import {
  LayoutDashboard,
  Layers,
  GitFork,
  FileCode2,
  Cpu,
  Bell,
  Settings,
  ChevronLeft,
  ChevronRight,
  FolderTree,
  Server,
  Loader2,
  ChevronUp as ChevronUpIcon,
} from "lucide-react";
import { useCallback, useEffect, useRef, useState } from "react";
import { motion, AnimatePresence } from "framer-motion";
import { config as configApi } from "../lib/api.ts";
import type { SSHProfilesResponse } from "../lib/types.ts";

type NavItem = {
  to: string;
  icon: React.ReactNode;
  label: string;
};

const NAV_ITEMS: NavItem[] = [
  { to: "/", icon: <LayoutDashboard size={18} />, label: "Dashboard" },
  { to: "/jobs", icon: <Layers size={18} />, label: "Jobs" },
  { to: "/workflows", icon: <GitFork size={18} />, label: "Workflows" },
  { to: "/templates", icon: <FileCode2 size={18} />, label: "Templates" },
  { to: "/resources", icon: <Cpu size={18} />, label: "Resources" },
  { to: "/notifications", icon: <Bell size={18} />, label: "Notifications" },
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
  const [profileData, setProfileData] = useState<SSHProfilesResponse | null>(
    null,
  );
  const [profileOpen, setProfileOpen] = useState(false);
  const [connecting, setConnecting] = useState<string | null>(null);
  const [connectedProfile, setConnectedProfile] = useState<string | null>(null);

  const loadProfiles = useCallback(async () => {
    try {
      const [profiles, status] = await Promise.all([
        configApi.sshProfiles(),
        configApi.sshStatus(),
      ]);
      setProfileData(profiles);
      setConnectedProfile(status.profile_name);
    } catch {
      // silent
    }
  }, []);

  useEffect(() => {
    loadProfiles();
  }, [loadProfiles]);

  const handleSwitchProfile = async (name: string) => {
    setConnecting(name);
    try {
      const res = await configApi.connectSSHProfile(name);
      if (res.connected) {
        setConnectedProfile(name);
      }
    } catch {
      // silent
    } finally {
      setConnecting(null);
      setProfileOpen(false);
      await loadProfiles();
    }
  };

  // Close dropdown on outside click
  const profileRef = useRef<HTMLDivElement>(null);
  useEffect(() => {
    if (!profileOpen) return;
    const handler = (e: MouseEvent) => {
      if (
        profileRef.current &&
        !profileRef.current.contains(e.target as Node)
      ) {
        setProfileOpen(false);
      }
    };
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, [profileOpen]);

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
        <img
          src="/icon.svg"
          alt="srunx"
          width={28}
          height={28}
          style={{
            borderRadius: 6,
            flexShrink: 0,
            display: "block",
          }}
        />
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
            transition: "all var(--duration-fast) var(--ease-out)",
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
                transition: "all var(--duration-fast) var(--ease-out)",
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

      {/* Profile switcher */}
      {profileData && !collapsed && (
        <div
          ref={profileRef}
          style={{
            padding: "8px 12px",
            borderTop: "1px solid var(--border-ghost)",
            position: "relative",
          }}
        >
          <button
            onClick={() => setProfileOpen((v) => !v)}
            style={{
              width: "100%",
              display: "flex",
              alignItems: "center",
              gap: 8,
              padding: "8px 10px",
              borderRadius: 6,
              border: "1px solid var(--border-ghost)",
              background: "var(--bg-base)",
              color: "var(--text-secondary)",
              cursor: "pointer",
              fontSize: "0.75rem",
              fontFamily: "var(--font-mono)",
            }}
          >
            <Server
              size={13}
              style={{
                flexShrink: 0,
                color: connectedProfile
                  ? "var(--st-completed)"
                  : "var(--text-muted)",
              }}
            />
            <span
              style={{
                flex: 1,
                textAlign: "left",
                overflow: "hidden",
                textOverflow: "ellipsis",
                whiteSpace: "nowrap",
              }}
            >
              {connectedProfile ?? "No connection"}
            </span>
            <ChevronUpIcon
              size={12}
              style={{
                flexShrink: 0,
                transform: profileOpen ? "rotate(180deg)" : "rotate(0deg)",
                transition: "transform var(--duration-fast)",
              }}
            />
          </button>

          <AnimatePresence>
            {profileOpen && (
              <motion.div
                initial={{ opacity: 0, y: 4 }}
                animate={{ opacity: 1, y: 0 }}
                exit={{ opacity: 0, y: 4 }}
                transition={{ duration: 0.12 }}
                style={{
                  position: "absolute",
                  bottom: "100%",
                  left: 12,
                  right: 12,
                  marginBottom: 4,
                  background: "var(--bg-surface)",
                  border: "1px solid var(--border-subtle)",
                  borderRadius: 8,
                  boxShadow: "var(--shadow-panel)",
                  overflow: "hidden",
                  zIndex: 100,
                }}
              >
                {Object.keys(profileData.profiles).map((name) => {
                  const isCurrent = name === connectedProfile;
                  const isConnecting = connecting === name;
                  return (
                    <button
                      key={name}
                      onClick={() => {
                        if (!isCurrent && !isConnecting)
                          handleSwitchProfile(name);
                      }}
                      disabled={isConnecting}
                      style={{
                        width: "100%",
                        display: "flex",
                        alignItems: "center",
                        gap: 8,
                        padding: "8px 12px",
                        border: "none",
                        background: isCurrent
                          ? "var(--accent-dim)"
                          : "transparent",
                        color: isCurrent
                          ? "var(--accent)"
                          : "var(--text-secondary)",
                        cursor: isCurrent ? "default" : "pointer",
                        fontSize: "0.75rem",
                        fontFamily: "var(--font-mono)",
                        textAlign: "left",
                      }}
                    >
                      {isConnecting ? (
                        <Loader2 size={12} className="spin" />
                      ) : (
                        <Server size={12} />
                      )}
                      <span
                        style={{
                          flex: 1,
                          overflow: "hidden",
                          textOverflow: "ellipsis",
                          whiteSpace: "nowrap",
                        }}
                      >
                        {name}
                      </span>
                      {isCurrent && (
                        <span
                          style={{
                            fontSize: "0.65rem",
                            color: "var(--st-completed)",
                          }}
                        >
                          connected
                        </span>
                      )}
                    </button>
                  );
                })}
              </motion.div>
            )}
          </AnimatePresence>
        </div>
      )}

      {collapsed && profileData && (
        <div
          style={{
            padding: "8px 0",
            display: "flex",
            justifyContent: "center",
            borderTop: "1px solid var(--border-ghost)",
          }}
          title={connectedProfile ?? "No connection"}
        >
          <Server
            size={14}
            style={{
              color: connectedProfile
                ? "var(--st-completed)"
                : "var(--text-muted)",
            }}
          />
        </div>
      )}

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
          transition: "color var(--duration-fast)",
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
