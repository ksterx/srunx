import { NavLink } from "react-router-dom";
import {
  LayoutDashboard,
  Layers,
  GitFork,
  Grid3x3,
  FileCode2,
  Cpu,
  Bell,
  Settings,
  ChevronLeft,
  ChevronRight,
  FolderTree,
  Server,
  FolderOpen,
  ChevronUp as ChevronUpIcon,
} from "lucide-react";
import {
  useEffect,
  useLayoutEffect,
  useRef,
  useState,
} from "react";
import { createPortal } from "react-dom";
import { motion, AnimatePresence } from "framer-motion";
import {
  selectCluster,
  selectProject,
  useSelection,
} from "../lib/selection.ts";

type NavItem = {
  to: string;
  icon: React.ReactNode;
  label: string;
};

const NAV_ITEMS: NavItem[] = [
  { to: "/", icon: <LayoutDashboard size={18} />, label: "Dashboard" },
  { to: "/jobs", icon: <Layers size={18} />, label: "Jobs" },
  { to: "/workflows", icon: <GitFork size={18} />, label: "Workflows" },
  { to: "/sweep_runs", icon: <Grid3x3 size={18} />, label: "Sweeps" },
  { to: "/templates", icon: <FileCode2 size={18} />, label: "Templates" },
  { to: "/resources", icon: <Cpu size={18} />, label: "Resources" },
  { to: "/notifications", icon: <Bell size={18} />, label: "Notifications" },
  { to: "/settings", icon: <Settings size={18} />, label: "Settings" },
];

const COLLAPSED_STORAGE_KEY = "srunx.sidebar.collapsed";
const SIDEBAR_EXPANDED = 240;
const SIDEBAR_COLLAPSED = 64;
// Icon column center matches the collapsed sidebar's center (64 / 2 = 32).
// Nav icons are 18px wide, so padding-left = 32 - 9 = 23.
const NAV_PAD_LEFT = 23;
// Logo is 28px wide, so padding-left = 32 - 14 = 18.
const LOGO_PAD_LEFT = 18;

function readStoredCollapsed(): boolean {
  if (typeof window === "undefined") return false;
  try {
    return window.localStorage.getItem(COLLAPSED_STORAGE_KEY) === "1";
  } catch {
    return false;
  }
}

export function Sidebar() {
  const [collapsed, setCollapsed] = useState<boolean>(readStoredCollapsed);
  const selection = useSelection();
  // Persist collapsed state
  useEffect(() => {
    try {
      window.localStorage.setItem(COLLAPSED_STORAGE_KEY, collapsed ? "1" : "0");
    } catch {
      // quota / disabled — ignore
    }
  }, [collapsed]);

  // Keyboard shortcut: Cmd/Ctrl+B toggles the sidebar (VS Code convention)
  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if ((e.metaKey || e.ctrlKey) && !e.shiftKey && !e.altKey) {
        if (e.key === "b" || e.key === "B") {
          const target = e.target as HTMLElement | null;
          const tag = target?.tagName;
          if (
            tag === "INPUT" ||
            tag === "TEXTAREA" ||
            target?.isContentEditable
          ) {
            return;
          }
          e.preventDefault();
          setCollapsed((c) => !c);
        }
      }
    };
    window.addEventListener("keydown", handler);
    return () => window.removeEventListener("keydown", handler);
  }, []);

  return (
    <motion.aside
      className="sidebar"
      animate={{ width: collapsed ? SIDEBAR_COLLAPSED : SIDEBAR_EXPANDED }}
      transition={{ duration: 0.2, ease: [0.16, 1, 0.3, 1] }}
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
      {/* Logo row — always flush to NAV icon column */}
      <div
        style={{
          display: "flex",
          alignItems: "center",
          gap: 12,
          height: 64,
          paddingLeft: LOGO_PAD_LEFT,
          paddingRight: 12,
          borderBottom: "1px solid var(--border-ghost)",
          flexShrink: 0,
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
        <AnimatePresence initial={false}>
          {!collapsed && (
            <motion.span
              key="brand"
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              exit={{ opacity: 0 }}
              transition={{ duration: 0.12 }}
              style={{
                fontFamily: "var(--font-display)",
                fontWeight: 700,
                fontSize: "1.15rem",
                letterSpacing: "0.06em",
                color: "var(--text-primary)",
                whiteSpace: "nowrap",
                flex: 1,
                overflow: "hidden",
                minWidth: 0,
              }}
            >
              srunx
            </motion.span>
          )}
        </AnimatePresence>
        {/* Toggle button — visible in expanded state, top-right corner */}
        {!collapsed && (
          <button
            type="button"
            onClick={() => setCollapsed(true)}
            title="Collapse sidebar (⌘B)"
            aria-label="Collapse sidebar"
            style={{
              display: "flex",
              alignItems: "center",
              justifyContent: "center",
              width: 26,
              height: 26,
              border: "1px solid var(--border-ghost)",
              borderRadius: 6,
              background: "transparent",
              color: "var(--text-muted)",
              cursor: "pointer",
              flexShrink: 0,
              transition: "all var(--duration-fast)",
            }}
            onMouseEnter={(e) => {
              e.currentTarget.style.color = "var(--text-primary)";
              e.currentTarget.style.background = "var(--bg-hover)";
              e.currentTarget.style.borderColor = "var(--border-default)";
            }}
            onMouseLeave={(e) => {
              e.currentTarget.style.color = "var(--text-muted)";
              e.currentTarget.style.background = "transparent";
              e.currentTarget.style.borderColor = "var(--border-ghost)";
            }}
          >
            <ChevronLeft size={15} />
          </button>
        )}
      </div>

      {/* Expand-when-collapsed toggle — floats just below the logo */}
      {collapsed && (
        <button
          type="button"
          onClick={() => setCollapsed(false)}
          title="Expand sidebar (⌘B)"
          aria-label="Expand sidebar"
          style={{
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            height: 32,
            margin: "6px auto",
            width: 32,
            border: "1px solid var(--border-ghost)",
            borderRadius: 6,
            background: "transparent",
            color: "var(--text-muted)",
            cursor: "pointer",
            flexShrink: 0,
            transition: "all var(--duration-fast)",
          }}
          onMouseEnter={(e) => {
            e.currentTarget.style.color = "var(--text-primary)";
            e.currentTarget.style.background = "var(--bg-hover)";
            e.currentTarget.style.borderColor = "var(--border-default)";
          }}
          onMouseLeave={(e) => {
            e.currentTarget.style.color = "var(--text-muted)";
            e.currentTarget.style.background = "transparent";
            e.currentTarget.style.borderColor = "var(--border-ghost)";
          }}
        >
          <ChevronRight size={15} />
        </button>
      )}

      {/* Navigation */}
      <nav
        style={{
          flex: 1,
          padding: "8px 0",
          display: "flex",
          flexDirection: "column",
          gap: 1,
          overflowY: "auto",
          overflowX: "hidden",
        }}
      >
        <SidebarNavLink
          to="/explorer"
          icon={<FolderTree size={18} />}
          label="Explorer"
          collapsed={collapsed}
        />

        <div
          style={{
            height: 1,
            background: "var(--border-ghost)",
            margin: "6px 12px",
          }}
        />

        {NAV_ITEMS.map((item) => (
          <SidebarNavLink
            key={item.to}
            to={item.to}
            icon={item.icon}
            label={item.label}
            collapsed={collapsed}
            end={item.to === "/"}
          />
        ))}
      </nav>

      {/* Project / cluster selectors — available in both states */}
      {selection.ready && (
        <div
          style={{
            padding: "8px 0",
            borderTop: "1px solid var(--border-ghost)",
            flexShrink: 0,
          }}
        >
          {selection.projects.length > 0 && (
            <SelectionSwitcher
              kind="project"
              icon={<FolderOpen size={18} />}
              collapsed={collapsed}
              current={selection.project}
              placeholder="No project"
              items={selection.projects.map((p) => ({
                name: p.name,
                hint: p.cluster ?? undefined,
              }))}
              onSelect={selectProject}
            />
          )}
          <SelectionSwitcher
            kind="cluster"
            icon={<Server size={18} />}
            collapsed={collapsed}
            current={selection.cluster}
            placeholder="No cluster"
            items={selection.clusters.map((c) => ({
              name: c.name,
              hint:
                c.name === selection.defaultCluster ? "default" : c.transport,
            }))}
            onSelect={selectCluster}
          />
        </div>
      )}
    </motion.aside>
  );
}

type SidebarNavLinkProps = {
  to: string;
  icon: React.ReactNode;
  label: string;
  collapsed: boolean;
  end?: boolean;
};

function SidebarNavLink({
  to,
  icon,
  label,
  collapsed,
  end,
}: SidebarNavLinkProps) {
  return (
    <NavLink
      to={to}
      end={end}
      title={collapsed ? label : undefined}
      style={({ isActive }) => ({
        position: "relative",
        display: "flex",
        alignItems: "center",
        gap: 12,
        height: 38,
        paddingLeft: NAV_PAD_LEFT,
        paddingRight: 16,
        textDecoration: "none",
        fontSize: "0.92rem",
        fontWeight: 500,
        fontFamily: "var(--font-body)",
        color: isActive ? "var(--text-primary)" : "var(--text-secondary)",
        background: isActive ? "var(--accent-dim)" : "transparent",
        transition:
          "background var(--duration-fast), color var(--duration-fast)",
      })}
      onMouseEnter={(e) => {
        const el = e.currentTarget as HTMLAnchorElement;
        if (!el.classList.contains("active")) {
          el.style.background = "var(--bg-hover)";
        }
      }}
      onMouseLeave={(e) => {
        const el = e.currentTarget as HTMLAnchorElement;
        if (!el.classList.contains("active")) {
          el.style.background = "transparent";
        }
      }}
    >
      {({ isActive }: { isActive: boolean }) => (
        <>
          {isActive && (
            <span
              aria-hidden
              style={{
                position: "absolute",
                left: 0,
                top: 6,
                bottom: 6,
                width: 2,
                background: "var(--accent)",
                borderRadius: "0 2px 2px 0",
              }}
            />
          )}
          <span style={{ flexShrink: 0, display: "flex" }}>{icon}</span>
          <AnimatePresence initial={false}>
            {!collapsed && (
              <motion.span
                key="label"
                initial={{ opacity: 0 }}
                animate={{ opacity: 1 }}
                exit={{ opacity: 0 }}
                transition={{ duration: 0.12 }}
                style={{
                  whiteSpace: "nowrap",
                  overflow: "hidden",
                  minWidth: 0,
                }}
              >
                {label}
              </motion.span>
            )}
          </AnimatePresence>
        </>
      )}
    </NavLink>
  );
}

type SelectionItem = { name: string; hint?: string };

type SelectionSwitcherProps = {
  kind: "cluster" | "project";
  icon: React.ReactNode;
  collapsed: boolean;
  current: string | null;
  placeholder: string;
  items: SelectionItem[];
  onSelect: (name: string) => void;
};

function SelectionSwitcher({
  kind,
  icon,
  collapsed,
  current,
  placeholder,
  items,
  onSelect,
}: SelectionSwitcherProps) {
  const [open, setOpen] = useState(false);
  const btnRef = useRef<HTMLButtonElement>(null);

  // Close dropdown on outside click (checks both button and portal popover)
  useEffect(() => {
    if (!open) return;
    const handler = (e: MouseEvent) => {
      const target = e.target as HTMLElement | null;
      if (!target) return;
      const insideAnchor = btnRef.current?.contains(target) ?? false;
      const insidePopover = Boolean(
        target.closest?.(`[data-selection-popover="${kind}"]`),
      );
      if (!insideAnchor && !insidePopover) setOpen(false);
    };
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, [open, kind]);

  const label = current ?? placeholder;

  return (
    <>
      <button
        ref={btnRef}
        onClick={() => setOpen((v) => !v)}
        title={collapsed ? `${kind}: ${label}` : undefined}
        aria-label={`Select ${kind}`}
        data-testid={`${kind}-switcher`}
        style={{
          position: "relative",
          width: "100%",
          display: "flex",
          alignItems: "center",
          gap: 12,
          height: 38,
          paddingLeft: NAV_PAD_LEFT,
          paddingRight: collapsed ? NAV_PAD_LEFT : 12,
          justifyContent: "flex-start",
          border: "none",
          background: open ? "var(--bg-hover)" : "transparent",
          color: "var(--text-secondary)",
          cursor: "pointer",
          fontFamily: "var(--font-mono)",
          fontSize: "0.78rem",
          transition: "background var(--duration-fast)",
        }}
        onMouseEnter={(e) => {
          if (!open) e.currentTarget.style.background = "var(--bg-hover)";
        }}
        onMouseLeave={(e) => {
          if (!open) e.currentTarget.style.background = "transparent";
        }}
      >
        <span
          style={{
            flexShrink: 0,
            display: "flex",
            color: current ? "var(--st-completed)" : "var(--text-muted)",
          }}
        >
          {icon}
        </span>
        <AnimatePresence initial={false}>
          {!collapsed && (
            <motion.span
              key={`${kind}-label`}
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              exit={{ opacity: 0 }}
              transition={{ duration: 0.12 }}
              style={{
                flex: 1,
                textAlign: "left",
                overflow: "hidden",
                textOverflow: "ellipsis",
                whiteSpace: "nowrap",
                minWidth: 0,
              }}
            >
              {label}
            </motion.span>
          )}
        </AnimatePresence>
        {!collapsed && (
          <ChevronUpIcon
            size={12}
            style={{
              color: "var(--text-muted)",
              flexShrink: 0,
              transform: open ? "rotate(180deg)" : "rotate(0deg)",
              transition: "transform var(--duration-fast)",
            }}
          />
        )}
      </button>

      <SelectionPopover
        kind={kind}
        open={open}
        anchorRef={btnRef}
        collapsed={collapsed}
        items={items}
        current={current}
        onSelect={(name) => {
          setOpen(false);
          onSelect(name);
        }}
      />
    </>
  );
}

type SelectionPopoverProps = {
  kind: "cluster" | "project";
  open: boolean;
  anchorRef: React.RefObject<HTMLButtonElement | null>;
  collapsed: boolean;
  items: SelectionItem[];
  current: string | null;
  onSelect: (name: string) => void;
};

function SelectionPopover({
  kind,
  open,
  anchorRef,
  collapsed,
  items,
  current,
  onSelect,
}: SelectionPopoverProps) {
  const [pos, setPos] = useState<{
    top: number;
    left: number;
    width: number;
  } | null>(null);

  useLayoutEffect(() => {
    if (!open) return;
    const anchor = anchorRef.current;
    if (!anchor) return;

    const compute = () => {
      const r = anchor.getBoundingClientRect();
      const popoverWidth = collapsed ? 200 : r.width;
      // Approximate popover height (n rows × 38px + padding). Kept simple — the
      // browser clamps via viewport; exact height isn't needed for positioning.
      const estHeight = Math.min(48 + Math.max(items.length, 1) * 38, 320);
      if (collapsed) {
        // To the right of the button, bottom-aligned with it.
        setPos({
          top: r.bottom - estHeight,
          left: r.right + 6,
          width: popoverWidth,
        });
      } else {
        // Above the button, matching its width.
        setPos({
          top: r.top - estHeight - 4,
          left: r.left,
          width: popoverWidth,
        });
      }
    };

    compute();
    window.addEventListener("resize", compute);
    window.addEventListener("scroll", compute, true);
    return () => {
      window.removeEventListener("resize", compute);
      window.removeEventListener("scroll", compute, true);
    };
  }, [open, collapsed, items.length, anchorRef]);

  if (typeof document === "undefined") return null;

  return createPortal(
    <AnimatePresence>
      {open && pos && (
        <motion.div
          data-selection-popover={kind}
          initial={{ opacity: 0, y: 4 }}
          animate={{ opacity: 1, y: 0 }}
          exit={{ opacity: 0, y: 4 }}
          transition={{ duration: 0.12 }}
          style={{
            position: "fixed",
            top: pos.top,
            left: pos.left,
            width: pos.width,
            maxHeight: 320,
            overflowY: "auto",
            background: "var(--bg-surface)",
            border: "1px solid var(--border-subtle)",
            borderRadius: 8,
            boxShadow: "var(--shadow-dropdown)",
            zIndex: 1000,
          }}
        >
          <div
            style={{
              padding: "8px 14px 4px",
              fontSize: "0.65rem",
              fontFamily: "var(--font-display)",
              textTransform: "uppercase",
              letterSpacing: "0.08em",
              color: "var(--text-muted)",
            }}
          >
            {kind === "cluster" ? "Cluster" : "Project"}
          </div>
          {items.length === 0 && (
            <div
              style={{
                padding: "10px 14px",
                fontSize: "0.78rem",
                color: "var(--text-muted)",
              }}
            >
              {kind === "cluster"
                ? "No clusters — add one in Settings"
                : "No projects"}
            </div>
          )}
          {items.map(({ name, hint }) => {
            const isCurrent = name === current;
            return (
              <button
                key={name}
                onClick={() => {
                  if (!isCurrent) onSelect(name);
                }}
                style={{
                  width: "100%",
                  display: "flex",
                  alignItems: "center",
                  gap: 8,
                  padding: "10px 14px",
                  border: "none",
                  background: isCurrent ? "var(--accent-dim)" : "transparent",
                  color: isCurrent ? "var(--accent)" : "var(--text-secondary)",
                  cursor: isCurrent ? "default" : "pointer",
                  fontSize: "0.78rem",
                  fontFamily: "var(--font-mono)",
                  textAlign: "left",
                }}
              >
                {kind === "cluster" ? (
                  <Server size={13} />
                ) : (
                  <FolderOpen size={13} />
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
                {hint && (
                  <span
                    style={{
                      fontSize: "0.68rem",
                      color: "var(--text-muted)",
                    }}
                  >
                    {hint}
                  </span>
                )}
              </button>
            );
          })}
        </motion.div>
      )}
    </AnimatePresence>,
    document.body,
  );
}
