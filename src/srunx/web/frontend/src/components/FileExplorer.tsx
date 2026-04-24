import { useCallback, useEffect, useMemo, useRef, useState } from "react";
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
  Bell,
  BellOff,
  Code,
  Copy,
  FileText,
} from "lucide-react";
import hljs from "highlight.js/lib/core";
import python from "highlight.js/lib/languages/python";
import bash from "highlight.js/lib/languages/bash";
import yaml from "highlight.js/lib/languages/yaml";
import hljsJson from "highlight.js/lib/languages/json";
import javascript from "highlight.js/lib/languages/javascript";
import typescript from "highlight.js/lib/languages/typescript";
import xml from "highlight.js/lib/languages/xml";
import ini from "highlight.js/lib/languages/ini";
import dockerfile from "highlight.js/lib/languages/dockerfile";
import "highlight.js/styles/github-dark.css";
import { config, endpoints as endpointsApi, files, jobs } from "../lib/api.ts";
import type {
  Endpoint,
  FileEntry,
  FileEntryType,
  Mount,
} from "../lib/types.ts";

/* ── Notification presets ───────────────────── */

const NOTIFICATION_PRESETS = [
  { value: "terminal", label: "Terminal (completed / failed)" },
  { value: "running_and_terminal", label: "Running + terminal" },
  { value: "all", label: "All state changes" },
] as const;

type NotificationPresetValue = (typeof NOTIFICATION_PRESETS)[number]["value"];

function isKnownPreset(value: string): value is NotificationPresetValue {
  return NOTIFICATION_PRESETS.some((p) => p.value === value);
}

/* ── highlight.js setup ────────────────────── */

hljs.registerLanguage("python", python);
hljs.registerLanguage("bash", bash);
hljs.registerLanguage("yaml", yaml);
hljs.registerLanguage("json", hljsJson);
hljs.registerLanguage("javascript", javascript);
hljs.registerLanguage("typescript", typescript);
hljs.registerLanguage("xml", xml);
hljs.registerLanguage("ini", ini);
hljs.registerLanguage("dockerfile", dockerfile);

function detectLanguage(filename: string): string | undefined {
  const lower = filename.toLowerCase();
  if (lower === "dockerfile" || lower.startsWith("dockerfile."))
    return "dockerfile";
  if (lower === "makefile") return "bash";
  const ext = lower.split(".").pop();
  if (!ext) return undefined;
  const map: Record<string, string> = {
    py: "python",
    sh: "bash",
    bash: "bash",
    zsh: "bash",
    slurm: "bash",
    sbatch: "bash",
    yaml: "yaml",
    yml: "yaml",
    json: "json",
    js: "javascript",
    jsx: "javascript",
    ts: "typescript",
    tsx: "typescript",
    xml: "xml",
    html: "xml",
    ini: "ini",
    toml: "ini",
    cfg: "ini",
    conf: "bash",
  };
  return map[ext];
}

/* ── Helpers ────────────────────────────────── */

const SBATCH_EXTENSIONS = [".sh", ".slurm", ".sbatch", ".bash"];

function isSbatchFile(name: string): boolean {
  return SBATCH_EXTENSIONS.some((ext) => name.endsWith(ext));
}

function joinRemotePath(remote: string, relPath: string): string {
  const prefix = remote.replace(/\/+$/, "");
  const suffix = relPath.replace(/^\/+/, "");
  if (!suffix) return prefix || "/";
  return prefix ? `${prefix}/${suffix}` : `/${suffix}`;
}

type CopyButtonProps = {
  value: string;
  title?: string;
  style?: React.CSSProperties;
};

function CopyButton({ value, title, style }: CopyButtonProps) {
  const [copied, setCopied] = useState(false);

  async function handleCopy(e: React.MouseEvent) {
    e.stopPropagation();
    try {
      await navigator.clipboard.writeText(value);
      setCopied(true);
      setTimeout(() => setCopied(false), 1500);
    } catch {
      /* clipboard unavailable — silently ignore */
    }
  }

  const label = title ?? "Copy path";
  return (
    <button
      type="button"
      onClick={handleCopy}
      title={copied ? "Copied" : label}
      aria-label={copied ? `${label} (copied)` : label}
      style={{
        background: "transparent",
        border: "none",
        color: copied ? "var(--st-completed)" : "var(--text-muted)",
        cursor: "pointer",
        padding: 4,
        borderRadius: "var(--radius-sm)",
        display: "flex",
        alignItems: "center",
        flexShrink: 0,
        transition: "color var(--duration-fast) var(--ease-out)",
        ...style,
      }}
      onMouseEnter={(e) => {
        if (!copied) e.currentTarget.style.color = "var(--text-secondary)";
      }}
      onMouseLeave={(e) => {
        e.currentTarget.style.color = copied
          ? "var(--st-completed)"
          : "var(--text-muted)";
      }}
    >
      {copied ? <Check size={12} /> : <Copy size={12} />}
    </button>
  );
}

function getFileIcon(name: string, type: FileEntryType, isExpanded: boolean) {
  if (type === "directory")
    return isExpanded ? (
      <FolderOpen
        size={16}
        style={{ color: "var(--st-pending)", flexShrink: 0 }}
      />
    ) : (
      <Folder size={16} style={{ color: "var(--st-pending)", flexShrink: 0 }} />
    );
  if (type === "symlink")
    return (
      <Link2 size={16} style={{ color: "var(--text-muted)", flexShrink: 0 }} />
    );
  if (isSbatchFile(name))
    return (
      <FileCode
        size={16}
        style={{ color: "var(--st-running)", flexShrink: 0 }}
      />
    );
  return (
    <File size={16} style={{ color: "var(--text-muted)", flexShrink: 0 }} />
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
  mountRemote: string;
  onClose: () => void;
};

function SubmitDialog({
  fileName,
  filePath,
  mountName,
  mountRemote,
  onClose,
}: SubmitDialogProps) {
  const fullRemotePath = joinRemotePath(mountRemote, filePath);
  const [jobName, setJobName] = useState(fileName.replace(/\.[^.]+$/, ""));
  const [submitting, setSubmitting] = useState(false);
  const [result, setResult] = useState<{ job_id: number | null } | null>(null);
  const [error, setError] = useState<string | null>(null);

  // Notification controls
  const [notify, setNotify] = useState(false);
  const [endpointList, setEndpointList] = useState<Endpoint[]>([]);
  const [selectedEndpointId, setSelectedEndpointId] = useState<number | null>(
    null,
  );
  const [preset, setPreset] = useState<NotificationPresetValue>("terminal");
  const [endpointsLoading, setEndpointsLoading] = useState(true);

  // Script preview
  const [preview, setPreview] = useState<string | null>(null);
  const [previewOpen, setPreviewOpen] = useState(false);
  const [previewLoading, setPreviewLoading] = useState(false);

  // Fetch endpoints + notification defaults from config.
  useEffect(() => {
    let cancelled = false;
    async function loadNotificationState() {
      try {
        setEndpointsLoading(true);
        const [fetched, cfg] = await Promise.all([
          endpointsApi.list(),
          config.get().catch(() => null),
        ]);
        if (cancelled) return;
        setEndpointList(fetched);

        const defaultName = cfg?.notifications?.default_endpoint_name ?? null;
        const defaultPreset = cfg?.notifications?.default_preset;
        if (defaultPreset && isKnownPreset(defaultPreset)) {
          setPreset(defaultPreset);
        }

        if (fetched.length > 0) {
          // Pre-select the endpoint the user will most likely pick —
          // their configured default, or the first available — so the
          // dropdown is not empty the moment they tick the box.
          const match = defaultName
            ? fetched.find((e) => e.name === defaultName)
            : undefined;
          setSelectedEndpointId((match ?? fetched[0]).id);
          // Auto-opt-in only when the user has *explicitly* wired up
          // a ``default_endpoint_name`` in config. Merely having an
          // endpoint row in the DB (e.g. from a teammate) is not a
          // signal that this user wants every submit to notify — we
          // used to turn the toggle on in that case, which surprised
          // users who didn't realise their runs were firing Slack.
          // The checkbox still defaults to ON when the opt-in is
          // explicit via config, so configured users see no behaviour
          // change.
          setNotify(match !== undefined);
        } else {
          setSelectedEndpointId(null);
          setNotify(false);
        }
      } catch {
        if (!cancelled) {
          setEndpointList([]);
          setSelectedEndpointId(null);
          setNotify(false);
        }
      } finally {
        if (!cancelled) setEndpointsLoading(false);
      }
    }
    loadNotificationState();
    return () => {
      cancelled = true;
    };
  }, []);

  const hasEndpoints = endpointList.length > 0;
  const notifyEnabled = notify && hasEndpoints && selectedEndpointId !== null;

  // Load script preview on toggle
  useEffect(() => {
    if (previewOpen && preview === null && !previewLoading) {
      setPreviewLoading(true);
      files
        .read(mountName, filePath)
        .then(({ content }) => setPreview(content))
        .catch(() => setPreview("Failed to load preview"))
        .finally(() => setPreviewLoading(false));
    }
  }, [previewOpen, preview, previewLoading, mountName, filePath]);

  async function handleSubmit() {
    try {
      setSubmitting(true);
      setError(null);
      // Reuse cached preview if available, otherwise fetch
      const content =
        preview ?? (await files.read(mountName, filePath)).content;
      const res = await jobs.submit(
        content,
        jobName,
        mountName,
        // DEPRECATED `notify_slack` flag, retained for backward compatibility.
        notifyEnabled,
        {
          notify: notifyEnabled,
          endpointId: notifyEnabled ? selectedEndpointId : null,
          preset,
        },
      );
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
          maxWidth: 480,
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
          {/* Script path */}
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
                display: "flex",
                alignItems: "center",
                gap: "var(--sp-2)",
                fontFamily: "var(--font-mono)",
                fontSize: "0.8rem",
                color: "var(--text-secondary)",
                padding: "var(--sp-2) var(--sp-3)",
                background: "var(--bg-base)",
                borderRadius: "var(--radius-md)",
                border: "1px solid var(--border-subtle)",
              }}
            >
              <span
                style={{
                  flex: 1,
                  overflow: "hidden",
                  textOverflow: "ellipsis",
                  whiteSpace: "nowrap",
                }}
                title={fullRemotePath}
              >
                {fullRemotePath}
              </span>
              <CopyButton value={fullRemotePath} title="Copy full path" />
            </div>
          </div>

          {/* Script preview (collapsible) */}
          <div>
            <button
              onClick={() => setPreviewOpen((v) => !v)}
              style={{
                display: "flex",
                alignItems: "center",
                gap: 6,
                background: "none",
                border: "none",
                color: "var(--text-muted)",
                cursor: "pointer",
                padding: 0,
                fontFamily: "var(--font-mono)",
                fontSize: "0.7rem",
                textTransform: "uppercase",
                letterSpacing: "0.06em",
              }}
            >
              {previewOpen ? (
                <ChevronDown size={10} />
              ) : (
                <ChevronRight size={10} />
              )}
              <Code size={10} />
              Preview
            </button>
            {previewOpen && (
              <div
                style={{
                  marginTop: 6,
                  maxHeight: 180,
                  overflow: "auto",
                  fontFamily: "var(--font-mono)",
                  fontSize: "0.7rem",
                  lineHeight: 1.5,
                  color: "var(--text-secondary)",
                  padding: "var(--sp-2) var(--sp-3)",
                  background: "var(--bg-base)",
                  borderRadius: "var(--radius-md)",
                  border: "1px solid var(--border-subtle)",
                  whiteSpace: "pre",
                  tabSize: 4,
                }}
              >
                {previewLoading ? (
                  <span
                    style={{ color: "var(--text-muted)", fontStyle: "italic" }}
                  >
                    Loading...
                  </span>
                ) : (
                  preview
                )}
              </div>
            )}
          </div>

          {/* Job name */}
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

          {/* Notification controls */}
          <div
            style={{
              display: "flex",
              flexDirection: "column",
              gap: "var(--sp-2)",
            }}
          >
            <label
              style={{
                display: "flex",
                alignItems: "center",
                gap: 8,
                cursor: hasEndpoints ? "pointer" : "default",
                opacity: hasEndpoints ? 1 : 0.4,
              }}
              title={
                hasEndpoints
                  ? "Send a notification on job state changes"
                  : "Add an endpoint in Settings → Notifications first."
              }
            >
              <input
                type="checkbox"
                checked={notify && hasEndpoints}
                onChange={(e) => setNotify(e.target.checked)}
                disabled={
                  !hasEndpoints || endpointsLoading || submitting || !!result
                }
                style={{ accentColor: "var(--st-running)", margin: 0 }}
              />
              {notify && hasEndpoints ? (
                <Bell size={13} style={{ color: "var(--st-running)" }} />
              ) : (
                <BellOff size={13} style={{ color: "var(--text-muted)" }} />
              )}
              <span
                style={{
                  fontFamily: "var(--font-mono)",
                  fontSize: "0.75rem",
                  color: hasEndpoints
                    ? "var(--text-secondary)"
                    : "var(--text-muted)",
                }}
              >
                Notify on state changes
              </span>
            </label>

            {!endpointsLoading && !hasEndpoints && (
              <div
                style={{
                  display: "flex",
                  alignItems: "center",
                  gap: "var(--sp-2)",
                  padding: "var(--sp-2) var(--sp-3)",
                  background: "var(--bg-base)",
                  border: "1px solid var(--border-subtle)",
                  borderRadius: "var(--radius-md)",
                  fontFamily: "var(--font-mono)",
                  fontSize: "0.7rem",
                  color: "var(--text-muted)",
                }}
              >
                <AlertTriangle size={12} />
                <span style={{ flex: 1 }}>
                  Add an endpoint in Settings → Notifications first.
                </span>
                <a
                  href="/settings"
                  onClick={(e) => {
                    e.stopPropagation();
                    onClose();
                  }}
                  style={{
                    color: "var(--st-running)",
                    textDecoration: "none",
                  }}
                  onMouseEnter={(e) => {
                    e.currentTarget.style.textDecoration = "underline";
                  }}
                  onMouseLeave={(e) => {
                    e.currentTarget.style.textDecoration = "none";
                  }}
                >
                  Open Settings
                </a>
              </div>
            )}

            {hasEndpoints && notify && (
              <div
                style={{
                  display: "grid",
                  gridTemplateColumns: "1fr 1fr",
                  gap: "var(--sp-2)",
                }}
              >
                <div>
                  <label
                    style={{
                      fontFamily: "var(--font-mono)",
                      fontSize: "0.65rem",
                      color: "var(--text-muted)",
                      textTransform: "uppercase",
                      letterSpacing: "0.06em",
                      marginBottom: 4,
                      display: "block",
                    }}
                  >
                    Endpoint
                  </label>
                  <select
                    className="input"
                    style={{ width: "100%", fontSize: "0.75rem" }}
                    value={selectedEndpointId ?? ""}
                    onChange={(e) =>
                      setSelectedEndpointId(
                        e.target.value === "" ? null : Number(e.target.value),
                      )
                    }
                    disabled={submitting || !!result}
                  >
                    {endpointList.map((ep) => (
                      <option key={ep.id} value={ep.id}>
                        {ep.name} ({ep.kind})
                      </option>
                    ))}
                  </select>
                </div>
                <div>
                  <label
                    style={{
                      fontFamily: "var(--font-mono)",
                      fontSize: "0.65rem",
                      color: "var(--text-muted)",
                      textTransform: "uppercase",
                      letterSpacing: "0.06em",
                      marginBottom: 4,
                      display: "block",
                    }}
                  >
                    Preset
                  </label>
                  <select
                    className="input"
                    style={{ width: "100%", fontSize: "0.75rem" }}
                    value={preset}
                    onChange={(e) => {
                      const v = e.target.value;
                      if (isKnownPreset(v)) setPreset(v);
                    }}
                    disabled={submitting || !!result}
                  >
                    {NOTIFICATION_PRESETS.map((opt) => (
                      <option key={opt.value} value={opt.value}>
                        {opt.label}
                      </option>
                    ))}
                  </select>
                </div>
              </div>
            )}
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

/* ── File Viewer ───────────────────────────── */

type SelectedFile = {
  mount: string;
  remote: string;
  path: string;
  name: string;
};

type FileViewerProps = {
  selectedFile: SelectedFile | null;
  content: string | null;
  loading: boolean;
  error: string | null;
};

function FileViewer({
  selectedFile,
  content,
  loading,
  error,
}: FileViewerProps) {
  const codeRef = useRef<HTMLElement>(null);

  useEffect(() => {
    if (!codeRef.current || !content) return;
    const lang = detectLanguage(selectedFile?.name ?? "");
    if (lang) {
      try {
        codeRef.current.innerHTML = hljs.highlight(content, {
          language: lang,
        }).value;
        return;
      } catch {
        /* fallback to plain text */
      }
    }
    codeRef.current.textContent = content;
  }, [content, selectedFile?.name]);

  const lines = useMemo(() => (content ? content.split("\n") : []), [content]);

  if (!selectedFile) {
    return (
      <div
        style={{
          flex: 1,
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
          flexDirection: "column",
          gap: 12,
          color: "var(--text-muted)",
          fontSize: "0.9rem",
          fontFamily: "var(--font-body)",
        }}
      >
        <FileText size={36} strokeWidth={1} />
        Select a file to view its contents
      </div>
    );
  }

  if (loading) {
    return (
      <div
        style={{
          flex: 1,
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
          color: "var(--text-muted)",
          gap: 8,
        }}
      >
        <Loader2 size={16} style={{ animation: "spin 1s linear infinite" }} />
        <span style={{ fontSize: "0.9rem" }}>Loading...</span>
      </div>
    );
  }

  if (error) {
    return (
      <div
        style={{
          flex: 1,
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
          color: "var(--st-failed)",
          fontSize: "0.9rem",
          gap: 8,
        }}
      >
        <AlertTriangle size={16} />
        {error}
      </div>
    );
  }

  return (
    <div
      style={{
        flex: 1,
        display: "flex",
        flexDirection: "column",
        overflow: "hidden",
      }}
    >
      {/* File path bar */}
      <div
        style={{
          display: "flex",
          alignItems: "center",
          gap: 8,
          padding: "10px 18px",
          borderBottom: "1px solid var(--border-ghost)",
          fontSize: "0.85rem",
          fontFamily: "var(--font-mono)",
          color: "var(--text-secondary)",
          background: "var(--bg-surface)",
          flexShrink: 0,
          minHeight: 44,
        }}
      >
        <FileCode
          size={15}
          style={{ color: "var(--text-muted)", flexShrink: 0 }}
        />
        <span
          title={joinRemotePath(selectedFile.remote, selectedFile.path)}
          style={{
            overflow: "hidden",
            textOverflow: "ellipsis",
            whiteSpace: "nowrap",
          }}
        >
          {joinRemotePath(selectedFile.remote, selectedFile.path)}
        </span>
        <CopyButton
          value={joinRemotePath(selectedFile.remote, selectedFile.path)}
          title="Copy full path"
        />
        <span
          style={{
            marginLeft: "auto",
            color: "var(--text-muted)",
            fontSize: "0.75rem",
            flexShrink: 0,
          }}
        >
          {lines.length} lines
        </span>
      </div>
      {/* Code area */}
      <div style={{ flex: 1, overflow: "auto" }}>
        <div style={{ display: "flex", minHeight: "100%" }}>
          {/* Line numbers */}
          <div
            style={{
              padding: "12px 0",
              textAlign: "right",
              userSelect: "none",
              flexShrink: 0,
              borderRight: "1px solid var(--border-ghost)",
            }}
          >
            {lines.map((_, i) => (
              <div
                key={i}
                style={{
                  padding: "0 14px 0 18px",
                  fontSize: "0.88rem",
                  fontFamily: "var(--font-mono)",
                  lineHeight: "22px",
                  height: 22,
                  color: "var(--text-muted)",
                }}
              >
                {i + 1}
              </div>
            ))}
          </div>
          {/* Code content */}
          <pre
            style={{
              flex: 1,
              margin: 0,
              padding: "12px 18px",
              fontSize: "0.88rem",
              fontFamily: "var(--font-mono)",
              lineHeight: "22px",
              overflow: "visible",
              background: "transparent",
              color: "var(--text-primary)",
            }}
          >
            <code ref={codeRef} style={{ lineHeight: "22px" }}>
              {content}
            </code>
          </pre>
        </div>
      </div>
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
  onSubmit: (fullPath: string, entry: FileEntry) => void;
  onFileSelect: (fullPath: string, entry: FileEntry) => void;
  selectedPath: string | null;
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
  onSubmit,
  onFileSelect,
  selectedPath,
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
  const isSelected = !isExpandable && selectedPath === fullPath;

  function handleClick() {
    if (isInaccessible) return;
    if (isExpandable) {
      onToggleDir(fullPath);
    } else {
      onFileSelect(fullPath, entry);
    }
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
          gap: 6,
          paddingLeft: depth * 14 + 10,
          paddingRight: 10,
          paddingTop: 1,
          paddingBottom: 1,
          height: 24,
          fontFamily: "var(--font-body)",
          fontSize: "0.9rem",
          cursor: isInaccessible ? "default" : "pointer",
          opacity: isInaccessible ? 0.35 : 1,
          color: isSelected
            ? "var(--text-primary)"
            : isSbatchFile(entry.name)
              ? "var(--text-primary)"
              : "var(--text-secondary)",
          background: isSelected ? "var(--accent-dim)" : "transparent",
          transition: "background var(--duration-fast) var(--ease-out)",
          userSelect: "none",
        }}
        onMouseEnter={(e) => {
          if (!isInaccessible)
            e.currentTarget.style.background = isSelected
              ? "var(--accent-dim)"
              : "var(--bg-hover)";
        }}
        onMouseLeave={(e) => {
          e.currentTarget.style.background = isSelected
            ? "var(--accent-dim)"
            : "transparent";
        }}
      >
        {/* Expand arrow */}
        {isExpandable ? (
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
        ) : (
          <span style={{ width: 14, flexShrink: 0 }} />
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

        {/* Sbatch submit button */}
        {isSbatchFile(entry.name) && (
          <button
            onClick={(e) => {
              e.stopPropagation();
              onSubmit(fullPath, entry);
            }}
            title="Submit as sbatch"
            style={{
              background: "none",
              border: "none",
              color: "var(--st-running)",
              cursor: "pointer",
              padding: 2,
              borderRadius: "var(--radius-sm)",
              display: "flex",
              alignItems: "center",
              flexShrink: 0,
              opacity: 0.6,
              transition: "opacity var(--duration-fast) var(--ease-out)",
            }}
            onMouseEnter={(e) => {
              e.currentTarget.style.opacity = "1";
            }}
            onMouseLeave={(e) => {
              e.currentTarget.style.opacity = "0.6";
            }}
          >
            <Play size={12} />
          </button>
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
          onSubmit={onSubmit}
          onFileSelect={onFileSelect}
          selectedPath={selectedPath}
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
  onSubmit: (fullPath: string, entry: FileEntry) => void;
  onFileSelect: (fullPath: string, entry: FileEntry) => void;
  selectedPath: string | null;
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
  onSubmit,
  onFileSelect,
  selectedPath,
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
          onSubmit={onSubmit}
          onFileSelect={onFileSelect}
          selectedPath={selectedPath}
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
  onSubmit: (fullPath: string, entry: FileEntry, mountName: string) => void;
  onFileSelect: (fullPath: string, entry: FileEntry, mountName: string) => void;
  selectedFilePath: string | null;
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
  onSubmit,
  onFileSelect,
  selectedFilePath,
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
        {isOpen ? <ChevronDown size={13} /> : <ChevronRight size={13} />}
        <Folder size={15} style={{ color: "var(--st-pending)" }} />
        <span
          style={{
            flex: 1,
            fontFamily: "var(--font-display)",
            fontSize: "0.82rem",
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
              onSubmit={(fp, entry) => onSubmit(fp, entry, mount.name)}
              onFileSelect={(fp, entry) => onFileSelect(fp, entry, mount.name)}
              selectedPath={selectedFilePath}
            />
          )}
        </div>
      )}
    </div>
  );
}

/* ── FileExplorer (main) ────────────────────── */

export function FileExplorer() {
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

  // File viewer state
  const [selectedFile, setSelectedFile] = useState<SelectedFile | null>(null);
  const [fileContent, setFileContent] = useState<string | null>(null);
  const [fileLoading, setFileLoading] = useState(false);
  const [fileError, setFileError] = useState<string | null>(null);
  const fileRequestRef = useRef(0);

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
    mountRemote: string;
  } | null>(null);

  const remoteByMount = useMemo(() => {
    const map = new Map<string, string>();
    for (const m of mounts) map.set(m.name, m.remote);
    return map;
  }, [mounts]);

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

  /* File select handler */
  const handleFileSelect = useCallback(
    async (mountName: string, filePath: string, fileName: string) => {
      // Allow re-click to retry on error
      if (
        selectedFile?.mount === mountName &&
        selectedFile?.path === filePath &&
        !fileError
      ) {
        return;
      }
      const requestId = ++fileRequestRef.current;
      const remote = remoteByMount.get(mountName) ?? "";
      setSelectedFile({
        mount: mountName,
        remote,
        path: filePath,
        name: fileName,
      });
      setFileContent(null);
      setFileLoading(true);
      setFileError(null);
      try {
        const result = await files.read(mountName, filePath);
        if (fileRequestRef.current !== requestId) return;
        setFileContent(result.content);
      } catch (err) {
        if (fileRequestRef.current !== requestId) return;
        setFileError(
          err instanceof Error ? err.message : "Failed to read file",
        );
      } finally {
        if (fileRequestRef.current === requestId) {
          setFileLoading(false);
        }
      }
    },
    [selectedFile, fileError, remoteByMount],
  );

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
      <div
        style={{
          display: "flex",
          flex: 1,
          height: "100%",
          overflow: "hidden",
        }}
      >
        {/* Tree panel */}
        <div
          style={{
            width: 260,
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
              padding: "10px 12px",
              borderBottom: "1px solid var(--border-ghost)",
              minHeight: 40,
            }}
          >
            <span
              style={{
                fontFamily: "var(--font-display)",
                fontSize: "0.82rem",
                fontWeight: 600,
                textTransform: "uppercase",
                letterSpacing: "0.08em",
                color: "var(--text-secondary)",
              }}
            >
              Explorer
            </span>
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
                  onSubmit={(fp, entry, mn) =>
                    setSubmitTarget({
                      filePath: fp,
                      fileName: entry.name,
                      mountName: mn,
                      mountRemote: remoteByMount.get(mn) ?? "",
                    })
                  }
                  onFileSelect={(fp, entry, mn) =>
                    handleFileSelect(mn, fp, entry.name)
                  }
                  selectedFilePath={
                    selectedFile?.mount === mount.name
                      ? selectedFile.path
                      : null
                  }
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
        </div>

        {/* File viewer */}
        <FileViewer
          selectedFile={selectedFile}
          content={fileContent}
          loading={fileLoading}
          error={fileError}
        />
      </div>

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
              mountRemote: remoteByMount.get(contextMenu.mountName) ?? "",
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
            mountRemote={submitTarget.mountRemote}
            onClose={() => setSubmitTarget(null)}
          />
        )}
      </AnimatePresence>

      <style>{`
        pre code.hljs {
          background: transparent !important;
          padding: 0 !important;
        }
      `}</style>
    </>
  );
}
