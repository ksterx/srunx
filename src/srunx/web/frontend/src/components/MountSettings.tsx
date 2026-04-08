import { useCallback, useEffect, useState } from "react";
import { motion, AnimatePresence } from "framer-motion";
import { X, Trash2, Plus, Loader2 } from "lucide-react";
import { files } from "../lib/api.ts";
import type { MountConfig } from "../lib/types.ts";

/* ── Props ───────────────────────────────────── */

type MountSettingsProps = {
  onClose: () => void;
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
  maxHeight: "80vh",
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
  justifyContent: "flex-end",
  padding: "var(--sp-3) var(--sp-5)",
  borderTop: "1px solid var(--border-ghost)",
};

const labelStyle: React.CSSProperties = {
  fontFamily: "var(--font-mono)",
  fontSize: "0.7rem",
  color: "var(--text-muted)",
  textTransform: "uppercase",
  letterSpacing: "0.06em",
  marginBottom: 4,
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

/* ── Component ───────────────────────────────── */

export function MountSettings({ onClose }: MountSettingsProps) {
  const [mounts, setMounts] = useState<MountConfig[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [deleting, setDeleting] = useState<string | null>(null);
  const [adding, setAdding] = useState(false);

  // Form state
  const [newName, setNewName] = useState("");
  const [newLocal, setNewLocal] = useState("");
  const [newRemote, setNewRemote] = useState("");
  const [newExcludes, setNewExcludes] = useState("");

  /* Load mounts */
  useEffect(() => {
    let cancelled = false;
    async function load() {
      try {
        setLoading(true);
        setError(null);
        const result = await files.mountsConfig();
        if (!cancelled) setMounts(result);
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

  /* Delete mount */
  const handleDelete = useCallback(async (name: string) => {
    try {
      setDeleting(name);
      setError(null);
      await files.removeMount(name);
      setMounts((prev) => prev.filter((m) => m.name !== name));
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to remove mount");
    } finally {
      setDeleting(null);
    }
  }, []);

  /* Add mount */
  const handleAdd = useCallback(async () => {
    const trimmedName = newName.trim();
    const trimmedLocal = newLocal.trim();
    const trimmedRemote = newRemote.trim();

    if (!trimmedName || !trimmedLocal || !trimmedRemote) {
      setError("All fields are required");
      return;
    }

    const excludePatterns = newExcludes
      .split(",")
      .map((s) => s.trim())
      .filter(Boolean);

    try {
      setAdding(true);
      setError(null);
      const created = await files.addMount({
        name: trimmedName,
        local: trimmedLocal,
        remote: trimmedRemote,
        exclude_patterns:
          excludePatterns.length > 0 ? excludePatterns : undefined,
      });
      setMounts((prev) => [...prev, created]);
      setNewName("");
      setNewLocal("");
      setNewRemote("");
      setNewExcludes("");
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to add mount");
    } finally {
      setAdding(false);
    }
  }, [newName, newLocal, newRemote, newExcludes]);

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
              Manage Mounts
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
            ) : (
              <>
                {/* Error display */}
                {error && (
                  <div
                    style={{
                      fontSize: "0.75rem",
                      color: "var(--st-failed)",
                      padding: "var(--sp-2) var(--sp-3)",
                      background: "var(--st-failed-dim)",
                      borderRadius: "var(--radius-md)",
                    }}
                  >
                    {error}
                  </div>
                )}

                {/* Mount list */}
                {mounts.length === 0 ? (
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
                  <div
                    style={{
                      background: "var(--bg-base)",
                      border: "1px solid var(--border-subtle)",
                      borderRadius: "var(--radius-md)",
                      overflow: "hidden",
                    }}
                  >
                    {/* Table header */}
                    <div
                      style={{
                        display: "grid",
                        gridTemplateColumns: "1fr 2fr 2fr auto",
                        gap: 0,
                        padding: "var(--sp-2) var(--sp-3)",
                        borderBottom: "1px solid var(--border-ghost)",
                      }}
                    >
                      <span style={labelStyle}>Name</span>
                      <span style={labelStyle}>Local Path</span>
                      <span style={labelStyle}>Remote Path</span>
                      <span style={{ ...labelStyle, width: 28 }} />
                    </div>

                    {/* Mount rows */}
                    {mounts.map((mount) => (
                      <div
                        key={mount.name}
                        style={{
                          display: "grid",
                          gridTemplateColumns: "1fr 2fr 2fr auto",
                          gap: 0,
                          padding: "var(--sp-2) var(--sp-3)",
                          alignItems: "center",
                          borderBottom: "1px solid var(--border-ghost)",
                          transition:
                            "background var(--duration-fast) var(--ease-out)",
                        }}
                        onMouseEnter={(e) => {
                          e.currentTarget.style.background = "var(--bg-hover)";
                        }}
                        onMouseLeave={(e) => {
                          e.currentTarget.style.background = "transparent";
                        }}
                      >
                        <span
                          style={{
                            fontFamily: "var(--font-mono)",
                            fontSize: "0.8rem",
                            color: "var(--text-primary)",
                            overflow: "hidden",
                            textOverflow: "ellipsis",
                            whiteSpace: "nowrap",
                          }}
                        >
                          {mount.name}
                        </span>
                        <span
                          style={{
                            fontFamily: "var(--font-mono)",
                            fontSize: "0.75rem",
                            color: "var(--text-secondary)",
                            overflow: "hidden",
                            textOverflow: "ellipsis",
                            whiteSpace: "nowrap",
                          }}
                          title={mount.local}
                        >
                          {mount.local}
                        </span>
                        <span
                          style={{
                            fontFamily: "var(--font-mono)",
                            fontSize: "0.75rem",
                            color: "var(--text-secondary)",
                            overflow: "hidden",
                            textOverflow: "ellipsis",
                            whiteSpace: "nowrap",
                          }}
                          title={mount.remote}
                        >
                          {mount.remote}
                        </span>
                        <button
                          onClick={() => handleDelete(mount.name)}
                          disabled={deleting === mount.name}
                          title={`Remove mount "${mount.name}"`}
                          style={{
                            background: "none",
                            border: "none",
                            color: "var(--st-failed)",
                            cursor:
                              deleting === mount.name
                                ? "not-allowed"
                                : "pointer",
                            padding: 4,
                            borderRadius: "var(--radius-sm)",
                            display: "flex",
                            alignItems: "center",
                            justifyContent: "center",
                            opacity: deleting === mount.name ? 0.5 : 1,
                            transition:
                              "background var(--duration-fast) var(--ease-out)",
                          }}
                          onMouseEnter={(e) => {
                            if (deleting !== mount.name) {
                              e.currentTarget.style.background =
                                "var(--st-failed-dim)";
                            }
                          }}
                          onMouseLeave={(e) => {
                            e.currentTarget.style.background = "none";
                          }}
                        >
                          {deleting === mount.name ? (
                            <Loader2
                              size={14}
                              style={{ animation: "spin 1s linear infinite" }}
                            />
                          ) : (
                            <Trash2 size={14} />
                          )}
                        </button>
                        {mount.exclude_patterns &&
                          mount.exclude_patterns.length > 0 && (
                            <div
                              style={{
                                gridColumn: "1 / -1",
                                fontFamily: "var(--font-mono)",
                                fontSize: "0.7rem",
                                color: "var(--text-muted)",
                                paddingTop: 2,
                              }}
                            >
                              exclude: {mount.exclude_patterns.join(", ")}
                            </div>
                          )}
                      </div>
                    ))}
                  </div>
                )}

                {/* Add mount form */}
                <div
                  style={{
                    borderTop: "1px solid var(--border-ghost)",
                    paddingTop: "var(--sp-4)",
                    display: "flex",
                    flexDirection: "column",
                    gap: "var(--sp-3)",
                  }}
                >
                  <div
                    style={{
                      fontFamily: "var(--font-display)",
                      fontSize: "0.7rem",
                      fontWeight: 500,
                      textTransform: "uppercase",
                      letterSpacing: "0.08em",
                      color: "var(--text-secondary)",
                    }}
                  >
                    Add Mount
                  </div>

                  <div
                    style={{
                      display: "grid",
                      gridTemplateColumns: "1fr 2fr 2fr",
                      gap: "var(--sp-2)",
                    }}
                  >
                    <div style={{ display: "flex", flexDirection: "column" }}>
                      <label style={labelStyle}>Name</label>
                      <input
                        className="input"
                        type="text"
                        value={newName}
                        onChange={(e) => setNewName(e.target.value)}
                        placeholder="my-project"
                        style={{
                          fontFamily: "var(--font-mono)",
                          fontSize: "0.8rem",
                        }}
                      />
                    </div>
                    <div style={{ display: "flex", flexDirection: "column" }}>
                      <label style={labelStyle}>Local Path</label>
                      <input
                        className="input"
                        type="text"
                        value={newLocal}
                        onChange={(e) => setNewLocal(e.target.value)}
                        placeholder="~/projects/my-project"
                        style={{
                          fontFamily: "var(--font-mono)",
                          fontSize: "0.8rem",
                        }}
                      />
                    </div>
                    <div style={{ display: "flex", flexDirection: "column" }}>
                      <label style={labelStyle}>Remote Path</label>
                      <input
                        className="input"
                        type="text"
                        value={newRemote}
                        onChange={(e) => setNewRemote(e.target.value)}
                        placeholder="/home/user/projects/my-project"
                        style={{
                          fontFamily: "var(--font-mono)",
                          fontSize: "0.8rem",
                        }}
                      />
                    </div>
                  </div>

                  <div style={{ display: "flex", flexDirection: "column" }}>
                    <label style={labelStyle}>Exclude Patterns</label>
                    <input
                      className="input"
                      type="text"
                      value={newExcludes}
                      onChange={(e) => setNewExcludes(e.target.value)}
                      placeholder="data/,logs/,*.bin (comma-separated)"
                      style={{
                        fontFamily: "var(--font-mono)",
                        fontSize: "0.8rem",
                      }}
                    />
                  </div>

                  <div style={{ display: "flex", justifyContent: "flex-end" }}>
                    <button
                      className="btn btn-primary"
                      onClick={handleAdd}
                      disabled={adding}
                      style={{
                        opacity: adding ? 0.6 : 1,
                        gap: 6,
                      }}
                    >
                      {adding ? (
                        <Loader2
                          size={14}
                          style={{ animation: "spin 1s linear infinite" }}
                        />
                      ) : (
                        <Plus size={14} />
                      )}
                      {adding ? "Adding..." : "Add Mount"}
                    </button>
                  </div>
                </div>
              </>
            )}
          </div>

          {/* Footer */}
          <div style={footerStyle}>
            <button className="btn btn-ghost" onClick={onClose}>
              Close
            </button>
          </div>
        </motion.div>
      </div>
    </AnimatePresence>
  );
}
