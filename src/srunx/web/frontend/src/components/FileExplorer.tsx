import { useCallback, useEffect, useRef, useState } from "react";
import { motion, AnimatePresence } from "framer-motion";
import {
  Folder,
  FolderOpen,
  File,
  FileCode,
  Link2,
  ChevronRight,
  ChevronDown,
  RefreshCw,
  Play,
  X,
  Loader2,
  Check,
  AlertTriangle,
} from "lucide-react";
import { files, jobs } from "../lib/api.ts";
import type { Mount, FileEntry, FileEntryType } from "../lib/types.ts";

/* ── Helpers ────────────────────────────────── */

const SBATCH_EXTENSIONS = [".sh", ".slurm", ".sbatch", ".bash"];

function isSbatchFile(name: string): boolean {
  return SBATCH_EXTENSIONS.some((ext) => name.endsWith(ext));
}

function getFileIcon(name: string, type: FileEntryType, isExpanded: boolean) {
  if (type === "directory")
    return isExpanded ? (
      <FolderOpen
        size={14}
        style={{ color: "var(--st-pending)", flexShrink: 0 }}
      />
    ) : (
      <Folder size={14} style={{ color: "var(--st-pending)", flexShrink: 0 }} />
    );
  if (type === "symlink")
    return (
      <Link2 size={14} style={{ color: "var(--text-muted)", flexShrink: 0 }} />
    );
  if (isSbatchFile(name))
    return (
      <FileCode
        size={14}
        style={{ color: "var(--st-running)", flexShrink: 0 }}
      />
    );
  return (
    <File size={14} style={{ color: "var(--text-muted)", flexShrink: 0 }} />
  );
}

/* ── Context Menu ───────────────────────────── */

type ContextMenuProps = {
  x: number;
  y: number;
  fileName: string;
  canSubmit: boolean;
  onSubmit: () => void;
  onClose: () => void;
};

function ContextMenu({
  x,
  y,
  fileName,
  canSubmit,
  onSubmit,
  onClose,
}: ContextMenuProps) {
  const ref = useRef<HTMLDivElement>(null);

  useEffect(() => {
    function handleClick(e: MouseEvent) {
      if (ref.current && !ref.current.contains(e.target as Node)) onClose();
    }
    function handleKey(e: KeyboardEvent) {
      if (e.key === "Escape") onClose();
    }
    document.addEventListener("mousedown", handleClick);
    document.addEventListener("keydown", handleKey);
    return () => {
      document.removeEventListener("mousedown", handleClick);
      document.removeEventListener("keydown", handleKey);
    };
  }, [onClose]);

  return (
    <div
      ref={ref}
      style={{
        position: "fixed",
        left: x,
        top: y,
        zIndex: 2000,
        background: "var(--bg-raised)",
        border: "1px solid var(--border-default)",
        borderRadius: "var(--radius-md)",
        boxShadow: "var(--shadow-dropdown)",
        padding: "var(--sp-1) 0",
        minWidth: 180,
      }}
    >
      <div
        style={{
          padding: "var(--sp-1) var(--sp-3)",
          fontSize: "0.65rem",
          color: "var(--text-muted)",
          fontFamily: "var(--font-mono)",
          overflow: "hidden",
          textOverflow: "ellipsis",
          whiteSpace: "nowrap",
          borderBottom: "1px solid var(--border-ghost)",
          marginBottom: 2,
        }}
      >
        {fileName}
      </div>
      {canSubmit ? (
        <button
          onClick={() => {
            onSubmit();
            onClose();
          }}
          style={{
            display: "flex",
            alignItems: "center",
            gap: 8,
            width: "100%",
            padding: "var(--sp-2) var(--sp-3)",
            background: "transparent",
            border: "none",
            color: "var(--st-running)",
            fontSize: "0.8rem",
            fontFamily: "var(--font-body)",
            cursor: "pointer",
            textAlign: "left",
          }}
          onMouseEnter={(e) =>
            (e.currentTarget.style.background = "var(--bg-hover)")
          }
          onMouseLeave={(e) =>
            (e.currentTarget.style.background = "transparent")
          }
        >
          <Play size={13} />
          Submit as sbatch
        </button>
      ) : (
        <div
          style={{
            padding: "var(--sp-2) var(--sp-3)",
            fontSize: "0.75rem",
            color: "var(--text-muted)",
          }}
        >
          Not a submittable script
        </div>
      )}
    </div>
  );
}

/* ── Submit Dialog ──────────────────────────── */

type SubmitDialogProps = {
  fileName: string;
  filePath: string;
  mountName: string;
  onClose: () => void;
};

function SubmitDialog({
  fileName,
  filePath,
  mountName,
  onClose,
}: SubmitDialogProps) {
  const [jobName, setJobName] = useState(fileName.replace(/\.[^.]+$/, ""));
  const [submitting, setSubmitting] = useState(false);
  const [result, setResult] = useState<{ job_id: number | null } | null>(null);
  const [error, setError] = useState<string | null>(null);

  async function handleSubmit() {
    try {
      setSubmitting(true);
      setError(null);
      const { content } = await files.read(mountName, filePath);
      const res = await jobs.submit(content, jobName, mountName);
      setResult(res);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Submission failed");
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <div
      style={{
        position: "fixed",
        inset: 0,
        background: "rgba(5, 8, 16, 0.7)",
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        zIndex: 1500,
      }}
      onClick={(e) => e.target === e.currentTarget && onClose()}
    >
      <motion.div
        initial={{ opacity: 0, scale: 0.95, y: 12 }}
        animate={{ opacity: 1, scale: 1, y: 0 }}
        exit={{ opacity: 0, scale: 0.95, y: 12 }}
        transition={{ duration: 0.2, ease: [0.16, 1, 0.3, 1] }}
        style={{
          background: "var(--bg-surface)",
          border: "1px solid var(--border-default)",
          borderRadius: "var(--radius-lg)",
          boxShadow: "var(--shadow-panel)",
          width: "100%",
          maxWidth: 420,
          overflow: "hidden",
        }}
      >
        {/* Header */}
        <div
          style={{
            display: "flex",
            alignItems: "center",
            justifyContent: "space-between",
            padding: "var(--sp-4) var(--sp-5)",
            borderBottom: "1px solid var(--border-ghost)",
          }}
        >
          <h3
            style={{
              fontFamily: "var(--font-display)",
              fontSize: "0.85rem",
              textTransform: "uppercase",
              letterSpacing: "0.08em",
              color: "var(--text-secondary)",
            }}
          >
            Submit Job
          </h3>
          <button
            onClick={onClose}
            style={{
              background: "none",
              border: "none",
              color: "var(--text-muted)",
              cursor: "pointer",
              padding: 4,
              borderRadius: "var(--radius-sm)",
              display: "flex",
            }}
          >
            <X size={16} />
          </button>
        </div>

        {/* Body */}
        <div
          style={{
            padding: "var(--sp-5)",
            display: "flex",
            flexDirection: "column",
            gap: "var(--sp-4)",
          }}
        >
          <div>
            <label
              style={{
                fontFamily: "var(--font-mono)",
                fontSize: "0.7rem",
                color: "var(--text-muted)",
                textTransform: "uppercase",
                letterSpacing: "0.06em",
                marginBottom: 4,
                display: "block",
              }}
            >
              Script
            </label>
            <div
              style={{
                fontFamily: "var(--font-mono)",
                fontSize: "0.8rem",
                color: "var(--text-secondary)",
                padding: "var(--sp-2) var(--sp-3)",
                background: "var(--bg-base)",
                borderRadius: "var(--radius-md)",
                border: "1px solid var(--border-subtle)",
                overflow: "hidden",
                textOverflow: "ellipsis",
                whiteSpace: "nowrap",
              }}
            >
              {filePath}
            </div>
          </div>

          <div>
            <label
              style={{
                fontFamily: "var(--font-mono)",
                fontSize: "0.7rem",
                color: "var(--text-muted)",
                textTransform: "uppercase",
                letterSpacing: "0.06em",
                marginBottom: 4,
                display: "block",
              }}
            >
              Job Name
            </label>
            <input
              className="input"
              style={{ width: "100%" }}
              value={jobName}
              onChange={(e) => setJobName(e.target.value)}
              disabled={submitting || !!result}
            />
          </div>

          {error && (
            <div
              style={{
                display: "flex",
                alignItems: "center",
                gap: "var(--sp-2)",
                fontSize: "0.8rem",
                color: "var(--st-failed)",
              }}
            >
              <AlertTriangle size={14} />
              {error}
            </div>
          )}

          {result && (
            <div
              style={{
                display: "flex",
                alignItems: "center",
                gap: "var(--sp-2)",
                padding: "var(--sp-2) var(--sp-3)",
                background: "var(--st-completed-dim)",
                borderRadius: "var(--radius-md)",
                fontSize: "0.8rem",
                color: "var(--st-completed)",
              }}
            >
              <Check size={14} />
              Job submitted — ID: {result.job_id}
            </div>
          )}
        </div>

        {/* Footer */}
        <div
          style={{
            display: "flex",
            justifyContent: "flex-end",
            gap: "var(--sp-2)",
            padding: "var(--sp-3) var(--sp-5)",
            borderTop: "1px solid var(--border-ghost)",
          }}
        >
          <button className="btn btn-ghost" onClick={onClose}>
            {result ? "Close" : "Cancel"}
          </button>
          {!result && (
            <button
              className="btn btn-primary"
              onClick={handleSubmit}
              disabled={submitting || !jobName.trim()}
              style={
                submitting || !jobName.trim()
                  ? { opacity: 0.5, cursor: "not-allowed" }
                  : {}
              }
            >
              {submitting ? (
                <>
                  <Loader2
                    size={14}
                    style={{ animation: "spin 1s linear infinite" }}
                  />
                  Submitting...
                </>
              ) : (
                <>
                  <Play size={14} />
                  Submit
                </>
              )}
            </button>
          )}
        </div>
      </motion.div>
    </div>
  );
}

/* ── Tree Node ──────────────────────────────── */

type TreeNodeProps = {
  entry: FileEntry;
  depth: number;
  parentPath: string;
  mountName: string;
  expandedDirs: Map<string, FileEntry[]>;
  onToggleDir: (fullPath: string) => void;
  loadingDir: string | null;
  onContextMenu: (
    e: React.MouseEvent,
    fullPath: string,
    entry: FileEntry,
  ) => void;
};

function TreeNode({
  entry,
  depth,
  parentPath,
  mountName,
  expandedDirs,
  onToggleDir,
  loadingDir,
  onContextMenu,
}: TreeNodeProps) {
  const fullPath = parentPath ? `${parentPath}/${entry.name}` : entry.name;
  const isDir = entry.type === "directory";
  const isSymlink = entry.type === "symlink";
  const isSymlinkDir =
    isSymlink &&
    entry.accessible !== false &&
    entry.target_kind === "directory";
  const isExpandable = isDir || isSymlinkDir;
  const isExpanded = expandedDirs.has(fullPath);
  const isInaccessible = isSymlink && entry.accessible === false;
  const isLoading = loadingDir === fullPath;

  function handleClick() {
    if (isInaccessible) return;
    if (isExpandable) onToggleDir(fullPath);
  }

  return (
    <div>
      <div
        onClick={handleClick}
        onContextMenu={(e) => {
          e.preventDefault();
          onContextMenu(e, fullPath, entry);
        }}
        style={{
          display: "flex",
          alignItems: "center",
          gap: 4,
          paddingLeft: depth * 12 + 8,
          paddingRight: 8,
          paddingTop: 2,
          paddingBottom: 2,
          height: 24,
          fontFamily: "var(--font-mono)",
          fontSize: "0.75rem",
          cursor: isInaccessible ? "default" : "pointer",
          opacity: isInaccessible ? 0.35 : 1,
          color: isSbatchFile(entry.name)
            ? "var(--text-primary)"
            : "var(--text-secondary)",
          transition: "background var(--duration-fast) var(--ease-out)",
          userSelect: "none",
        }}
        onMouseEnter={(e) => {
          if (!isInaccessible)
            e.currentTarget.style.background = "var(--bg-hover)";
        }}
        onMouseLeave={(e) => {
          e.currentTarget.style.background = "transparent";
        }}
      >
        {/* Expand arrow */}
        {isExpandable ? (
          <span
            style={{
              display: "flex",
              alignItems: "center",
              flexShrink: 0,
              width: 12,
            }}
          >
            {isLoading ? (
              <Loader2
                size={10}
                style={{
                  color: "var(--text-muted)",
                  animation: "spin 1s linear infinite",
                }}
              />
            ) : isExpanded ? (
              <ChevronDown size={10} />
            ) : (
              <ChevronRight size={10} />
            )}
          </span>
        ) : (
          <span style={{ width: 12, flexShrink: 0 }} />
        )}

        {/* Icon */}
        {getFileIcon(entry.name, entry.type, isExpanded)}

        {/* Name */}
        <span
          style={{
            overflow: "hidden",
            textOverflow: "ellipsis",
            whiteSpace: "nowrap",
            flex: 1,
          }}
        >
          {entry.name}
        </span>

        {/* Sbatch indicator */}
        {isSbatchFile(entry.name) && (
          <Play
            size={10}
            style={{ color: "var(--st-running)", opacity: 0.5, flexShrink: 0 }}
          />
        )}
      </div>

      {/* Children */}
      {isExpandable && isExpanded && expandedDirs.has(fullPath) && (
        <TreeChildren
          entries={expandedDirs.get(fullPath) ?? []}
          depth={depth + 1}
          parentPath={fullPath}
          mountName={mountName}
          expandedDirs={expandedDirs}
          onToggleDir={onToggleDir}
          loadingDir={loadingDir}
          onContextMenu={onContextMenu}
        />
      )}
    </div>
  );
}

type TreeChildrenProps = {
  entries: FileEntry[];
  depth: number;
  parentPath: string;
  mountName: string;
  expandedDirs: Map<string, FileEntry[]>;
  onToggleDir: (fullPath: string) => void;
  loadingDir: string | null;
  onContextMenu: (
    e: React.MouseEvent,
    fullPath: string,
    entry: FileEntry,
  ) => void;
};

function TreeChildren({
  entries,
  depth,
  parentPath,
  mountName,
  expandedDirs,
  onToggleDir,
  loadingDir,
  onContextMenu,
}: TreeChildrenProps) {
  const sorted = [...entries].sort((a, b) => {
    if (a.type === "directory" && b.type !== "directory") return -1;
    if (a.type !== "directory" && b.type === "directory") return 1;
    return a.name.localeCompare(b.name);
  });

  return (
    <>
      {sorted.map((entry) => (
        <TreeNode
          key={parentPath ? `${parentPath}/${entry.name}` : entry.name}
          entry={entry}
          depth={depth}
          parentPath={parentPath}
          mountName={mountName}
          expandedDirs={expandedDirs}
          onToggleDir={onToggleDir}
          loadingDir={loadingDir}
          onContextMenu={onContextMenu}
        />
      ))}
    </>
  );
}

/* ── Mount Section ──────────────────────────── */

type MountSectionProps = {
  mount: Mount;
  isOpen: boolean;
  onToggle: () => void;
  entries: FileEntry[];
  expandedDirs: Map<string, FileEntry[]>;
  onToggleDir: (fullPath: string) => void;
  loadingDir: string | null;
  onSync: (mountName: string) => void;
  syncing: boolean;
  syncSuccess: boolean;
  onContextMenu: (
    e: React.MouseEvent,
    fullPath: string,
    entry: FileEntry,
    mountName: string,
  ) => void;
};

function MountSection({
  mount,
  isOpen,
  onToggle,
  entries,
  expandedDirs,
  onToggleDir,
  loadingDir,
  onSync,
  syncing,
  syncSuccess,
  onContextMenu,
}: MountSectionProps) {
  return (
    <div>
      {/* Mount header */}
      <div
        onClick={onToggle}
        style={{
          display: "flex",
          alignItems: "center",
          gap: 6,
          padding: "6px 8px",
          background: "var(--bg-raised)",
          cursor: "pointer",
          userSelect: "none",
          borderBottom: "1px solid var(--border-ghost)",
        }}
        onMouseEnter={(e) =>
          (e.currentTarget.style.background = "var(--bg-overlay)")
        }
        onMouseLeave={(e) =>
          (e.currentTarget.style.background = "var(--bg-raised)")
        }
      >
        {isOpen ? <ChevronDown size={11} /> : <ChevronRight size={11} />}
        <Folder size={13} style={{ color: "var(--st-pending)" }} />
        <span
          style={{
            flex: 1,
            fontFamily: "var(--font-display)",
            fontSize: "0.7rem",
            fontWeight: 600,
            textTransform: "uppercase",
            letterSpacing: "0.06em",
            color: "var(--text-primary)",
            overflow: "hidden",
            textOverflow: "ellipsis",
            whiteSpace: "nowrap",
          }}
        >
          {mount.name}
        </span>
        <button
          onClick={(e) => {
            e.stopPropagation();
            onSync(mount.name);
          }}
          disabled={syncing}
          title="Sync to remote"
          style={{
            background: "none",
            border: "none",
            color: syncSuccess ? "var(--st-completed)" : "var(--text-muted)",
            cursor: syncing ? "not-allowed" : "pointer",
            padding: 2,
            borderRadius: "var(--radius-sm)",
            display: "flex",
            alignItems: "center",
          }}
          onMouseEnter={(e) => {
            if (!syncing) e.currentTarget.style.color = "var(--text-secondary)";
          }}
          onMouseLeave={(e) => {
            e.currentTarget.style.color = syncSuccess
              ? "var(--st-completed)"
              : "var(--text-muted)";
          }}
        >
          {syncing ? (
            <Loader2
              size={12}
              style={{ animation: "spin 1s linear infinite" }}
            />
          ) : syncSuccess ? (
            <Check size={12} />
          ) : (
            <RefreshCw size={12} />
          )}
        </button>
      </div>

      {/* Mount tree */}
      {isOpen && (
        <div style={{ paddingTop: 2, paddingBottom: 2 }}>
          {entries.length === 0 ? (
            <div
              style={{
                padding: "var(--sp-3) var(--sp-4)",
                fontSize: "0.7rem",
                color: "var(--text-muted)",
                fontStyle: "italic",
              }}
            >
              Empty directory
            </div>
          ) : (
            <TreeChildren
              entries={entries}
              depth={0}
              parentPath=""
              mountName={mount.name}
              expandedDirs={expandedDirs}
              onToggleDir={onToggleDir}
              loadingDir={loadingDir}
              onContextMenu={(e, fp, entry) =>
                onContextMenu(e, fp, entry, mount.name)
              }
            />
          )}
        </div>
      )}
    </div>
  );
}

/* ── FileExplorer (main) ────────────────────── */

type FileExplorerProps = {
  onClose: () => void;
};

export function FileExplorer({ onClose }: FileExplorerProps) {
  const [mounts, setMounts] = useState<Mount[]>([]);
  const [openMounts, setOpenMounts] = useState<Set<string>>(new Set());
  const [mountEntries, setMountEntries] = useState<Map<string, FileEntry[]>>(
    new Map(),
  );
  const [expandedDirs, setExpandedDirs] = useState<Map<string, FileEntry[]>>(
    new Map(),
  );
  const [loadingDir, setLoadingDir] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  // Sync state per mount
  const [syncingMount, setSyncingMount] = useState<string | null>(null);
  const [syncSuccessMount, setSyncSuccessMount] = useState<string | null>(null);

  // Context menu
  const [contextMenu, setContextMenu] = useState<{
    x: number;
    y: number;
    filePath: string;
    fileName: string;
    entry: FileEntry;
    mountName: string;
  } | null>(null);

  // Submit dialog
  const [submitTarget, setSubmitTarget] = useState<{
    filePath: string;
    fileName: string;
    mountName: string;
  } | null>(null);

  /* Load mounts on mount */
  useEffect(() => {
    let cancelled = false;
    async function load() {
      try {
        setLoading(true);
        const result = await files.mounts();
        if (cancelled) return;
        setMounts(result);
        // Auto-open first mount
        if (result.length > 0) {
          const first = result[0].name;
          setOpenMounts(new Set([first]));
          const browse = await files.browse(first, "");
          if (!cancelled) {
            setMountEntries((prev) => new Map(prev).set(first, browse.entries));
          }
        }
      } catch (err) {
        if (!cancelled)
          setError(err instanceof Error ? err.message : "Failed to load");
      } finally {
        if (!cancelled) setLoading(false);
      }
    }
    load();
    return () => {
      cancelled = true;
    };
  }, []);

  /* Toggle mount open/close */
  const handleToggleMount = useCallback(
    async (name: string) => {
      setOpenMounts((prev) => {
        const next = new Set(prev);
        if (next.has(name)) {
          next.delete(name);
        } else {
          next.add(name);
        }
        return next;
      });

      // Load root entries if not loaded yet
      if (!mountEntries.has(name)) {
        try {
          const result = await files.browse(name, "");
          setMountEntries((prev) => new Map(prev).set(name, result.entries));
        } catch (err) {
          setError(err instanceof Error ? err.message : "Failed to browse");
        }
      }
    },
    [mountEntries],
  );

  /* Toggle directory expansion - scoped by mount */
  const handleToggleDir = useCallback(
    async (mountName: string, fullPath: string) => {
      const key = `${mountName}:${fullPath}`;
      if (expandedDirs.has(key)) {
        setExpandedDirs((prev) => {
          const next = new Map(prev);
          next.delete(key);
          return next;
        });
        return;
      }

      try {
        setLoadingDir(key);
        const result = await files.browse(mountName, fullPath);
        setExpandedDirs((prev) => new Map(prev).set(key, result.entries));
      } catch (err) {
        setError(err instanceof Error ? err.message : "Failed to browse");
      } finally {
        setLoadingDir(null);
      }
    },
    [expandedDirs],
  );

  /* Sync handler */
  async function handleSync(mountName: string) {
    try {
      setSyncingMount(mountName);
      setSyncSuccessMount(null);
      await files.sync(mountName);
      setSyncSuccessMount(mountName);
      setTimeout(() => setSyncSuccessMount(null), 3000);
    } catch {
      setError("Sync failed");
    } finally {
      setSyncingMount(null);
    }
  }

  /* Context menu handler */
  function handleContextMenu(
    e: React.MouseEvent,
    filePath: string,
    entry: FileEntry,
    mountName: string,
  ) {
    e.preventDefault();
    setContextMenu({
      x: e.clientX,
      y: e.clientY,
      filePath,
      fileName: entry.name,
      entry,
      mountName,
    });
  }

  /* Build scoped expandedDirs for a specific mount */
  function getScopedExpandedDirs(mountName: string): Map<string, FileEntry[]> {
    const prefix = `${mountName}:`;
    const scoped = new Map<string, FileEntry[]>();
    for (const [key, value] of expandedDirs) {
      if (key.startsWith(prefix)) {
        scoped.set(key.slice(prefix.length), value);
      }
    }
    return scoped;
  }

  return (
    <>
      <motion.div
        initial={{ width: 0, opacity: 0 }}
        animate={{ width: 260, opacity: 1 }}
        exit={{ width: 0, opacity: 0 }}
        transition={{ duration: 0.25, ease: [0.16, 1, 0.3, 1] }}
        style={{
          height: "100%",
          background: "var(--bg-surface)",
          borderRight: "1px solid var(--border-subtle)",
          display: "flex",
          flexDirection: "column",
          overflow: "hidden",
          flexShrink: 0,
        }}
      >
        {/* Header */}
        <div
          style={{
            display: "flex",
            alignItems: "center",
            justifyContent: "space-between",
            padding: "10px 12px",
            borderBottom: "1px solid var(--border-ghost)",
            minHeight: 40,
          }}
        >
          <span
            style={{
              fontFamily: "var(--font-display)",
              fontSize: "0.7rem",
              fontWeight: 600,
              textTransform: "uppercase",
              letterSpacing: "0.08em",
              color: "var(--text-secondary)",
            }}
          >
            Explorer
          </span>
          <button
            onClick={onClose}
            style={{
              background: "none",
              border: "none",
              color: "var(--text-muted)",
              cursor: "pointer",
              padding: 2,
              borderRadius: "var(--radius-sm)",
              display: "flex",
            }}
            onMouseEnter={(e) =>
              (e.currentTarget.style.color = "var(--text-secondary)")
            }
            onMouseLeave={(e) =>
              (e.currentTarget.style.color = "var(--text-muted)")
            }
          >
            <X size={14} />
          </button>
        </div>

        {/* Body */}
        <div style={{ flex: 1, overflow: "auto" }}>
          {loading ? (
            <div
              style={{
                display: "flex",
                alignItems: "center",
                justifyContent: "center",
                padding: "var(--sp-8)",
                color: "var(--text-muted)",
                gap: 8,
              }}
            >
              <Loader2
                size={14}
                style={{ animation: "spin 1s linear infinite" }}
              />
              <span style={{ fontSize: "0.75rem" }}>Loading...</span>
            </div>
          ) : error && mounts.length === 0 ? (
            <div
              style={{
                padding: "var(--sp-4)",
                color: "var(--st-failed)",
                fontSize: "0.75rem",
                textAlign: "center",
              }}
            >
              {error}
            </div>
          ) : mounts.length === 0 ? (
            <div
              style={{
                padding: "var(--sp-4)",
                color: "var(--text-muted)",
                fontSize: "0.75rem",
                textAlign: "center",
              }}
            >
              No mounts configured.
              <br />
              <span style={{ fontSize: "0.65rem" }}>
                Add mounts via SSH profile settings.
              </span>
            </div>
          ) : (
            mounts.map((mount) => (
              <MountSection
                key={mount.name}
                mount={mount}
                isOpen={openMounts.has(mount.name)}
                onToggle={() => handleToggleMount(mount.name)}
                entries={mountEntries.get(mount.name) ?? []}
                expandedDirs={getScopedExpandedDirs(mount.name)}
                onToggleDir={(fullPath) =>
                  handleToggleDir(mount.name, fullPath)
                }
                loadingDir={
                  loadingDir?.startsWith(`${mount.name}:`)
                    ? loadingDir.slice(mount.name.length + 1)
                    : null
                }
                onSync={handleSync}
                syncing={syncingMount === mount.name}
                syncSuccess={syncSuccessMount === mount.name}
                onContextMenu={handleContextMenu}
              />
            ))
          )}

          {error && mounts.length > 0 && (
            <div
              style={{
                padding: "var(--sp-2) var(--sp-3)",
                fontSize: "0.7rem",
                color: "var(--st-failed)",
                borderTop: "1px solid var(--border-ghost)",
              }}
            >
              {error}
            </div>
          )}
        </div>
      </motion.div>

      {/* Context menu */}
      {contextMenu && (
        <ContextMenu
          x={contextMenu.x}
          y={contextMenu.y}
          fileName={contextMenu.fileName}
          canSubmit={isSbatchFile(contextMenu.fileName)}
          onSubmit={() =>
            setSubmitTarget({
              filePath: contextMenu.filePath,
              fileName: contextMenu.fileName,
              mountName: contextMenu.mountName,
            })
          }
          onClose={() => setContextMenu(null)}
        />
      )}

      {/* Submit dialog */}
      <AnimatePresence>
        {submitTarget && (
          <SubmitDialog
            fileName={submitTarget.fileName}
            filePath={submitTarget.filePath}
            mountName={submitTarget.mountName}
            onClose={() => setSubmitTarget(null)}
          />
        )}
      </AnimatePresence>

      <style>{`
        @keyframes spin {
          from { transform: rotate(0deg); }
          to { transform: rotate(360deg); }
        }
      `}</style>
    </>
  );
}
