import { useCallback, useEffect, useState } from "react";
import { motion, AnimatePresence } from "framer-motion";
import {
  Folder,
  File,
  Link2,
  ChevronRight,
  ChevronDown,
  X,
  RefreshCw,
  AlertTriangle,
  Check,
  Loader2,
} from "lucide-react";
import { files } from "../lib/api.ts";
import type { Mount, FileEntry, FileEntryType } from "../lib/types.ts";

/* ── Types ───────────────────────────────────── */

type FileBrowserProps = {
  /** Whether to select files or directories */
  mode: "file" | "directory";
  /** Current value (remote path) */
  value: string;
  /** Called with the selected remote path */
  onSelect: (remotePath: string) => void;
  /** Close the browser */
  onClose: () => void;
  /** Optional: current work_dir for relative path calculation */
  workDir?: string | null;
};

type SelectedEntry = {
  name: string;
  type: FileEntryType;
  remotePath: string;
};

/* ── Styles ──────────────────────────────────── */

const overlayStyle: React.CSSProperties = {
  position: "fixed",
  inset: 0,
  background: "rgba(5, 8, 16, 0.7)",
  display: "flex",
  alignItems: "center",
  justifyContent: "center",
  zIndex: 1000,
};

const panelStyle: React.CSSProperties = {
  background: "var(--bg-surface)",
  border: "1px solid var(--border-default)",
  borderRadius: "var(--radius-lg)",
  boxShadow: "var(--shadow-panel)",
  width: "100%",
  maxWidth: 600,
  maxHeight: "70vh",
  display: "flex",
  flexDirection: "column",
  overflow: "hidden",
};

const headerStyle: React.CSSProperties = {
  display: "flex",
  alignItems: "center",
  justifyContent: "space-between",
  padding: "var(--sp-4) var(--sp-5)",
  borderBottom: "1px solid var(--border-ghost)",
};

const bodyStyle: React.CSSProperties = {
  flex: 1,
  overflow: "auto",
  padding: "var(--sp-4) var(--sp-5)",
  display: "flex",
  flexDirection: "column",
  gap: "var(--sp-4)",
};

const footerStyle: React.CSSProperties = {
  display: "flex",
  alignItems: "center",
  justifyContent: "space-between",
  padding: "var(--sp-3) var(--sp-5)",
  borderTop: "1px solid var(--border-ghost)",
  gap: "var(--sp-3)",
};

const treeContainerStyle: React.CSSProperties = {
  background: "var(--bg-base)",
  border: "1px solid var(--border-subtle)",
  borderRadius: "var(--radius-md)",
  overflow: "auto",
  maxHeight: 320,
  minHeight: 160,
};

const syncBannerStyle: React.CSSProperties = {
  display: "flex",
  alignItems: "center",
  gap: "var(--sp-2)",
  padding: "var(--sp-2) var(--sp-3)",
  background: "var(--st-pending-dim)",
  borderRadius: "var(--radius-md)",
  fontSize: "0.75rem",
  color: "var(--st-pending)",
};

const iconBtnStyle: React.CSSProperties = {
  background: "none",
  border: "none",
  color: "var(--text-muted)",
  cursor: "pointer",
  padding: 4,
  borderRadius: "var(--radius-sm)",
  display: "flex",
  alignItems: "center",
  justifyContent: "center",
};

const labelStyle: React.CSSProperties = {
  fontFamily: "var(--font-mono)",
  fontSize: "0.7rem",
  color: "var(--text-muted)",
  textTransform: "uppercase",
  letterSpacing: "0.06em",
  marginBottom: 4,
};

/* ── DirectoryTree ───────────────────────────── */

type DirectoryTreeProps = {
  entries: FileEntry[];
  depth: number;
  parentPath: string;
  mountName: string;
  remotePrefix: string;
  expandedDirs: Map<string, FileEntry[]>;
  onToggleDir: (fullPath: string) => void;
  selectedEntry: SelectedEntry | null;
  onSelectEntry: (entry: SelectedEntry) => void;
  selectMode: "file" | "directory";
  loadingDir: string | null;
};

function DirectoryTree({
  entries,
  depth,
  parentPath,
  mountName,
  remotePrefix,
  expandedDirs,
  onToggleDir,
  selectedEntry,
  onSelectEntry,
  selectMode,
  loadingDir,
}: DirectoryTreeProps) {
  const sorted = [...entries].sort((a, b) => {
    if (a.type === "directory" && b.type !== "directory") return -1;
    if (a.type !== "directory" && b.type === "directory") return 1;
    return a.name.localeCompare(b.name);
  });

  return (
    <>
      {sorted.map((entry) => {
        const fullPath = parentPath
          ? `${parentPath}/${entry.name}`
          : entry.name;
        const remotePath = `${remotePrefix}/${fullPath}`;
        const isExpanded = expandedDirs.has(fullPath);
        const isDir = entry.type === "directory";
        const isSymlink = entry.type === "symlink";
        const isSymlinkDir =
          isSymlink &&
          entry.accessible !== false &&
          entry.target_kind === "directory";
        const isExpandable = isDir || isSymlinkDir;
        const isInaccessible = isSymlink && entry.accessible === false;
        const isLoading = loadingDir === fullPath;

        const effectiveKind: "file" | "directory" = isDir
          ? "directory"
          : isSymlinkDir
            ? "directory"
            : "file";
        const isSelected = selectedEntry?.remotePath === remotePath;
        const isSelectable =
          !isInaccessible &&
          (selectMode === "file"
            ? effectiveKind === "file"
            : effectiveKind === "directory");

        function handleClick() {
          if (isInaccessible) return;
          if (isExpandable) {
            onToggleDir(fullPath);
            if (selectMode === "directory") {
              onSelectEntry({
                name: entry.name,
                type: entry.type,
                remotePath,
              });
            }
          } else {
            if (isSelectable) {
              onSelectEntry({
                name: entry.name,
                type: entry.type,
                remotePath,
              });
            }
          }
        }

        return (
          <div key={fullPath}>
            <div
              onClick={handleClick}
              style={{
                display: "flex",
                alignItems: "center",
                gap: 6,
                paddingLeft: depth * 16 + 8,
                paddingRight: 8,
                paddingTop: 4,
                paddingBottom: 4,
                fontFamily: "var(--font-mono)",
                fontSize: "0.8rem",
                cursor: isInaccessible ? "default" : "pointer",
                opacity: isInaccessible ? 0.35 : 1,
                background: isSelected ? "var(--accent-dim)" : "transparent",
                color: isSelected ? "var(--accent)" : "var(--text-primary)",
                transition: "background var(--duration-fast) var(--ease-out)",
                userSelect: "none",
              }}
              onMouseEnter={(e) => {
                if (!isSelected && !isInaccessible) {
                  e.currentTarget.style.background = "var(--bg-hover)";
                }
              }}
              onMouseLeave={(e) => {
                if (!isSelected) {
                  e.currentTarget.style.background = "transparent";
                }
              }}
            >
              {isExpandable && (
                <span
                  style={{
                    display: "flex",
                    alignItems: "center",
                    flexShrink: 0,
                    width: 14,
                  }}
                >
                  {isLoading ? (
                    <Loader2
                      size={12}
                      style={{
                        color: "var(--text-muted)",
                        animation: "spin 1s linear infinite",
                      }}
                    />
                  ) : isExpanded ? (
                    <ChevronDown size={12} />
                  ) : (
                    <ChevronRight size={12} />
                  )}
                </span>
              )}
              {!isExpandable && <span style={{ width: 14, flexShrink: 0 }} />}
              {isDir ? (
                <Folder
                  size={14}
                  style={{ color: "var(--st-pending)", flexShrink: 0 }}
                />
              ) : isSymlink ? (
                <Link2
                  size={14}
                  style={{ color: "var(--text-muted)", flexShrink: 0 }}
                />
              ) : (
                <File
                  size={14}
                  style={{ color: "var(--text-muted)", flexShrink: 0 }}
                />
              )}
              <span
                style={{
                  overflow: "hidden",
                  textOverflow: "ellipsis",
                  whiteSpace: "nowrap",
                }}
              >
                {entry.name}
              </span>
            </div>

            {isExpandable && isExpanded && expandedDirs.has(fullPath) && (
              <DirectoryTree
                entries={expandedDirs.get(fullPath) ?? []}
                depth={depth + 1}
                parentPath={fullPath}
                mountName={mountName}
                remotePrefix={remotePrefix}
                expandedDirs={expandedDirs}
                onToggleDir={onToggleDir}
                selectedEntry={selectedEntry}
                onSelectEntry={onSelectEntry}
                selectMode={selectMode}
                loadingDir={loadingDir}
              />
            )}
          </div>
        );
      })}
    </>
  );
}

/* ── FileBrowser ─────────────────────────────── */

export function FileBrowser({
  mode,
  value: _value,
  onSelect,
  onClose,
  workDir: _workDir,
}: FileBrowserProps) {
  const [mounts, setMounts] = useState<Mount[]>([]);
  const [selectedMount, setSelectedMount] = useState<string | null>(null);
  const [expandedDirs, setExpandedDirs] = useState<Map<string, FileEntry[]>>(
    new Map(),
  );
  const [rootEntries, setRootEntries] = useState<FileEntry[]>([]);
  const [selectedEntry, setSelectedEntry] = useState<SelectedEntry | null>(
    null,
  );
  const [syncing, setSyncing] = useState(false);
  const [syncError, setSyncError] = useState<string | null>(null);
  const [syncSuccess, setSyncSuccess] = useState(false);
  const [loading, setLoading] = useState(true);
  const [loadingDir, setLoadingDir] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  const currentMount = mounts.find((m) => m.name === selectedMount);

  /* Load mounts on open */
  useEffect(() => {
    let cancelled = false;
    async function load() {
      try {
        setLoading(true);
        const result = await files.mounts();
        if (cancelled) return;
        setMounts(result);
        if (result.length > 0) {
          setSelectedMount(result[0].name);
        }
      } catch (err) {
        if (!cancelled) {
          setError(
            err instanceof Error ? err.message : "Failed to load mounts",
          );
        }
      } finally {
        if (!cancelled) setLoading(false);
      }
    }
    load();
    return () => {
      cancelled = true;
    };
  }, []);

  /* Load root entries when mount changes */
  useEffect(() => {
    if (!selectedMount) return;
    let cancelled = false;
    async function loadRoot() {
      try {
        setExpandedDirs(new Map());
        setRootEntries([]);
        setSelectedEntry(null);
        setError(null);
        const result = await files.browse(selectedMount!, "");
        if (cancelled) return;
        setRootEntries(result.entries);
      } catch (err) {
        if (!cancelled) {
          setError(
            err instanceof Error ? err.message : "Failed to browse directory",
          );
        }
      }
    }
    loadRoot();
    return () => {
      cancelled = true;
    };
  }, [selectedMount]);

  /* Toggle directory expansion */
  const handleToggleDir = useCallback(
    async (fullPath: string) => {
      if (!selectedMount) return;

      if (expandedDirs.has(fullPath)) {
        setExpandedDirs((prev) => {
          const next = new Map(prev);
          next.delete(fullPath);
          return next;
        });
        return;
      }

      try {
        setLoadingDir(fullPath);
        const result = await files.browse(selectedMount, fullPath);
        setExpandedDirs((prev) => {
          const next = new Map(prev);
          next.set(fullPath, result.entries);
          return next;
        });
      } catch (err) {
        setError(
          err instanceof Error ? err.message : "Failed to browse directory",
        );
      } finally {
        setLoadingDir(null);
      }
    },
    [selectedMount, expandedDirs],
  );

  /* Sync handler */
  async function handleSync() {
    if (!selectedMount) return;
    try {
      setSyncing(true);
      setSyncError(null);
      setSyncSuccess(false);
      await files.sync(selectedMount);
      setSyncSuccess(true);
      setTimeout(() => setSyncSuccess(false), 3000);
    } catch (err) {
      setSyncError(err instanceof Error ? err.message : "Sync failed");
    } finally {
      setSyncing(false);
    }
  }

  /* Confirm selection */
  function handleConfirm() {
    if (selectedEntry) {
      onSelect(selectedEntry.remotePath);
    }
  }

  /* Close on backdrop click */
  function handleBackdropClick(e: React.MouseEvent) {
    if (e.target === e.currentTarget) {
      onClose();
    }
  }

  return (
    <AnimatePresence>
      <div style={overlayStyle} onClick={handleBackdropClick}>
        <motion.div
          initial={{ opacity: 0, scale: 0.95, y: 12 }}
          animate={{ opacity: 1, scale: 1, y: 0 }}
          exit={{ opacity: 0, scale: 0.95, y: 12 }}
          transition={{ duration: 0.2, ease: [0.16, 1, 0.3, 1] }}
          style={panelStyle}
        >
          {/* Header */}
          <div style={headerStyle}>
            <h3
              style={{
                fontFamily: "var(--font-display)",
                fontSize: "0.85rem",
                textTransform: "uppercase",
                letterSpacing: "0.08em",
                color: "var(--text-secondary)",
              }}
            >
              {mode === "file" ? "Select File" : "Select Directory"}
            </h3>
            <button
              onClick={onClose}
              title="Close"
              style={iconBtnStyle}
              onMouseEnter={(e) => {
                e.currentTarget.style.background = "var(--bg-hover)";
              }}
              onMouseLeave={(e) => {
                e.currentTarget.style.background = "none";
              }}
            >
              <X size={16} />
            </button>
          </div>

          {/* Body */}
          <div style={bodyStyle}>
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
                  size={16}
                  style={{ animation: "spin 1s linear infinite" }}
                />
                <span style={{ fontSize: "0.8rem" }}>Loading mounts...</span>
              </div>
            ) : error && mounts.length === 0 ? (
              <div
                style={{
                  padding: "var(--sp-4)",
                  color: "var(--st-failed)",
                  fontSize: "0.8rem",
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
                  fontSize: "0.8rem",
                  textAlign: "center",
                }}
              >
                No mounts configured
              </div>
            ) : (
              <>
                {/* Mount selector */}
                <div style={{ display: "flex", flexDirection: "column" }}>
                  <label style={labelStyle}>Project</label>
                  <select
                    className="input"
                    value={selectedMount ?? ""}
                    onChange={(e) => setSelectedMount(e.target.value)}
                  >
                    {mounts.map((m) => (
                      <option key={m.name} value={m.name}>
                        {m.name}
                      </option>
                    ))}
                  </select>
                </div>

                {/* Remote prefix */}
                {currentMount && (
                  <div
                    style={{
                      fontFamily: "var(--font-mono)",
                      fontSize: "0.75rem",
                      color: "var(--text-secondary)",
                      padding: "var(--sp-1) 0",
                    }}
                  >
                    Remote: {currentMount.remote}
                  </div>
                )}

                {/* Sync banner */}
                <div style={syncBannerStyle}>
                  <AlertTriangle size={14} style={{ flexShrink: 0 }} />
                  <span style={{ flex: 1 }}>
                    Ensure local files are synced before running workflows
                  </span>
                  <button
                    className="btn btn-ghost"
                    onClick={handleSync}
                    disabled={syncing || !selectedMount}
                    style={{
                      padding: "2px 8px",
                      fontSize: "0.65rem",
                      gap: 4,
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
                    {syncing
                      ? "Syncing..."
                      : syncSuccess
                        ? "Synced"
                        : "Sync Now"}
                  </button>
                </div>

                {syncError && (
                  <div
                    style={{
                      fontSize: "0.75rem",
                      color: "var(--st-failed)",
                      padding: "var(--sp-1) 0",
                    }}
                  >
                    {syncError}
                  </div>
                )}

                {error && (
                  <div
                    style={{
                      fontSize: "0.75rem",
                      color: "var(--st-failed)",
                      padding: "var(--sp-1) 0",
                    }}
                  >
                    {error}
                  </div>
                )}

                {/* File tree */}
                <div style={treeContainerStyle}>
                  {rootEntries.length === 0 && !error ? (
                    <div
                      style={{
                        padding: "var(--sp-4)",
                        color: "var(--text-muted)",
                        fontSize: "0.8rem",
                        textAlign: "center",
                      }}
                    >
                      Empty directory
                    </div>
                  ) : (
                    <div style={{ padding: "var(--sp-2) 0" }}>
                      <DirectoryTree
                        entries={rootEntries}
                        depth={0}
                        parentPath=""
                        mountName={selectedMount ?? ""}
                        remotePrefix={currentMount?.remote ?? ""}
                        expandedDirs={expandedDirs}
                        onToggleDir={handleToggleDir}
                        selectedEntry={selectedEntry}
                        onSelectEntry={setSelectedEntry}
                        selectMode={mode}
                        loadingDir={loadingDir}
                      />
                    </div>
                  )}
                </div>
              </>
            )}
          </div>

          {/* Footer */}
          <div style={footerStyle}>
            <div
              style={{
                fontFamily: "var(--font-mono)",
                fontSize: "0.7rem",
                color: selectedEntry
                  ? "var(--text-secondary)"
                  : "var(--text-muted)",
                overflow: "hidden",
                textOverflow: "ellipsis",
                whiteSpace: "nowrap",
                flex: 1,
                minWidth: 0,
              }}
              title={selectedEntry?.remotePath ?? ""}
            >
              {selectedEntry ? selectedEntry.remotePath : "No selection"}
            </div>
            <div style={{ display: "flex", gap: "var(--sp-2)", flexShrink: 0 }}>
              <button className="btn btn-ghost" onClick={onClose}>
                Cancel
              </button>
              <button
                className="btn btn-primary"
                onClick={handleConfirm}
                disabled={!selectedEntry}
                style={
                  !selectedEntry ? { opacity: 0.5, cursor: "not-allowed" } : {}
                }
              >
                Select
              </button>
            </div>
          </div>
        </motion.div>
      </div>
    </AnimatePresence>
  );
}
