import { useEffect, useState } from "react";
import { motion } from "framer-motion";
import {
  Play,
  Eye,
  Loader2,
  X,
  ChevronDown,
  ChevronUp,
  Bell,
  BellOff,
} from "lucide-react";
import {
  config as configApi,
  endpoints as endpointsApi,
  workflows as workflowsApi,
} from "../lib/api.ts";
import { ErrorBanner } from "./ErrorBanner.tsx";
import type {
  DryRunJobInfo,
  Endpoint,
  WorkflowRunOptions,
} from "../lib/types.ts";

type WorkflowRunDialogProps = {
  workflowName: string;
  mount: string;
  jobNames: string[];
  onClose: () => void;
  onRunStarted: (run: Record<string, unknown>) => void;
};

type ExecutionMode = "full" | "from" | "to" | "range" | "single";

const NOTIFICATION_PRESETS = [
  { value: "terminal", label: "Terminal (completed / failed)" },
  { value: "running_and_terminal", label: "Running + terminal" },
  { value: "all", label: "All state changes" },
] as const;

type PresetValue = (typeof NOTIFICATION_PRESETS)[number]["value"];

function isKnownPreset(value: string): value is PresetValue {
  return NOTIFICATION_PRESETS.some((p) => p.value === value);
}

export function WorkflowRunDialog({
  workflowName,
  mount,
  jobNames,
  onClose,
  onRunStarted,
}: WorkflowRunDialogProps) {
  const [mode, setMode] = useState<ExecutionMode>("full");
  const [fromJob, setFromJob] = useState(jobNames[0] ?? "");
  const [toJob, setToJob] = useState(jobNames[jobNames.length - 1] ?? "");
  const [singleJob, setSingleJob] = useState(jobNames[0] ?? "");
  const [dryRun, setDryRun] = useState(false);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [dryRunResult, setDryRunResult] = useState<DryRunJobInfo[] | null>(
    null,
  );
  const [expandedScript, setExpandedScript] = useState<string | null>(null);

  // Notification controls — mirror the submit-dialog pattern.
  // Auto-opt-in requires both (a) a configured ``default_endpoint_name``
  // and (b) that name matching an endpoint row; just having endpoints
  // in the DB is not a signal of user intent. (Matches P3-8.)
  const [notify, setNotify] = useState(false);
  const [endpointList, setEndpointList] = useState<Endpoint[]>([]);
  const [selectedEndpointId, setSelectedEndpointId] = useState<number | null>(
    null,
  );
  const [preset, setPreset] = useState<PresetValue>("terminal");
  const [endpointsLoading, setEndpointsLoading] = useState(true);

  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
    };
    document.addEventListener("keydown", handler);
    return () => document.removeEventListener("keydown", handler);
  }, [onClose]);

  useEffect(() => {
    let cancelled = false;
    async function loadNotificationState() {
      try {
        setEndpointsLoading(true);
        const [fetched, cfg] = await Promise.all([
          endpointsApi.list(),
          configApi.get().catch(() => null),
        ]);
        if (cancelled) return;
        setEndpointList(fetched);

        const defaultName = cfg?.notifications?.default_endpoint_name ?? null;
        const defaultPreset = cfg?.notifications?.default_preset;
        if (defaultPreset && isKnownPreset(defaultPreset)) {
          setPreset(defaultPreset);
        }

        if (fetched.length > 0) {
          const match = defaultName
            ? fetched.find((e) => e.name === defaultName)
            : undefined;
          setSelectedEndpointId((match ?? fetched[0]).id);
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

  const buildOptions = (): WorkflowRunOptions => {
    const opts: WorkflowRunOptions = {};
    switch (mode) {
      case "from":
        opts.from_job = fromJob;
        break;
      case "to":
        opts.to_job = toJob;
        break;
      case "range":
        opts.from_job = fromJob;
        opts.to_job = toJob;
        break;
      case "single":
        opts.single_job = singleJob;
        break;
    }
    if (dryRun) opts.dry_run = true;
    if (notifyEnabled) {
      opts.notify = true;
      opts.endpoint_id = selectedEndpointId;
      opts.preset = preset;
    }
    return opts;
  };

  const handleRun = async () => {
    setLoading(true);
    setError(null);
    setDryRunResult(null);
    try {
      const opts = buildOptions();
      const result = await workflowsApi.run(workflowName, mount, opts);
      if ("dry_run" in result && result.dry_run) {
        setDryRunResult(result.jobs as DryRunJobInfo[]);
      } else {
        onRunStarted(result);
        onClose();
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : "Run failed");
    } finally {
      setLoading(false);
    }
  };

  const selectStyle = {
    padding: "var(--sp-2) var(--sp-3)",
    background: "var(--bg-base)",
    border: "1px solid var(--border-subtle)",
    borderRadius: "var(--radius-md)",
    color: "var(--text-primary)",
    fontFamily: "var(--font-mono)",
    fontSize: "0.8rem",
    flex: 1,
  };

  return (
    <div
      style={{
        position: "fixed",
        inset: 0,
        background: "rgba(0,0,0,0.5)",
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        zIndex: 1000,
      }}
      onClick={onClose}
    >
      <motion.div
        className="panel"
        initial={{ opacity: 0, scale: 0.95 }}
        animate={{ opacity: 1, scale: 1 }}
        onClick={(e) => e.stopPropagation()}
        style={{
          width: "min(560px, 90vw)",
          maxHeight: "80vh",
          overflow: "auto",
        }}
      >
        <div className="panel-header">
          <h3 style={{ textTransform: "none", letterSpacing: 0 }}>
            Run: {workflowName}
          </h3>
          <button
            onClick={onClose}
            style={{
              background: "transparent",
              border: "none",
              color: "var(--text-muted)",
              cursor: "pointer",
              padding: 4,
              display: "flex",
            }}
          >
            <X size={16} />
          </button>
        </div>
        <div
          className="panel-body"
          style={{
            display: "flex",
            flexDirection: "column",
            gap: "var(--sp-4)",
          }}
        >
          {/* Execution mode */}
          <div>
            <span
              style={{
                fontSize: "0.7rem",
                color: "var(--text-muted)",
                textTransform: "uppercase",
                letterSpacing: "0.08em",
                marginBottom: "var(--sp-2)",
                display: "block",
              }}
            >
              Execution Mode
            </span>
            <div
              style={{
                display: "flex",
                flexDirection: "column",
                gap: "var(--sp-2)",
              }}
            >
              {(
                [
                  ["full", "Full workflow"],
                  ["from", "From job"],
                  ["to", "Up to job"],
                  ["range", "Range (from → to)"],
                  ["single", "Single job"],
                ] as const
              ).map(([value, label]) => (
                <label
                  key={value}
                  style={{
                    display: "flex",
                    alignItems: "center",
                    gap: "var(--sp-2)",
                    fontSize: "0.8rem",
                    cursor: "pointer",
                  }}
                >
                  <input
                    type="radio"
                    name="mode"
                    value={value}
                    checked={mode === value}
                    onChange={() => setMode(value)}
                  />
                  <span>{label}</span>
                  {value === "from" && mode === "from" && (
                    <select
                      value={fromJob}
                      onChange={(e) => setFromJob(e.target.value)}
                      style={selectStyle}
                    >
                      {jobNames.map((n) => (
                        <option key={n} value={n}>
                          {n}
                        </option>
                      ))}
                    </select>
                  )}
                  {value === "to" && mode === "to" && (
                    <select
                      value={toJob}
                      onChange={(e) => setToJob(e.target.value)}
                      style={selectStyle}
                    >
                      {jobNames.map((n) => (
                        <option key={n} value={n}>
                          {n}
                        </option>
                      ))}
                    </select>
                  )}
                  {value === "single" && mode === "single" && (
                    <select
                      value={singleJob}
                      onChange={(e) => setSingleJob(e.target.value)}
                      style={selectStyle}
                    >
                      {jobNames.map((n) => (
                        <option key={n} value={n}>
                          {n}
                        </option>
                      ))}
                    </select>
                  )}
                </label>
              ))}
              {mode === "range" && (
                <div
                  style={{
                    display: "flex",
                    gap: "var(--sp-2)",
                    alignItems: "center",
                    paddingLeft: 24,
                  }}
                >
                  <select
                    value={fromJob}
                    onChange={(e) => setFromJob(e.target.value)}
                    style={selectStyle}
                  >
                    {jobNames.map((n) => (
                      <option key={n} value={n}>
                        {n}
                      </option>
                    ))}
                  </select>
                  <span
                    style={{ color: "var(--text-muted)", fontSize: "0.8rem" }}
                  >
                    →
                  </span>
                  <select
                    value={toJob}
                    onChange={(e) => setToJob(e.target.value)}
                    style={selectStyle}
                  >
                    {jobNames.map((n) => (
                      <option key={n} value={n}>
                        {n}
                      </option>
                    ))}
                  </select>
                </div>
              )}
            </div>
          </div>

          {/* Options */}
          <label
            style={{
              display: "flex",
              alignItems: "center",
              gap: "var(--sp-2)",
              fontSize: "0.8rem",
              cursor: "pointer",
            }}
          >
            <input
              type="checkbox"
              checked={dryRun}
              onChange={(e) => {
                setDryRun(e.target.checked);
                if (!e.target.checked) setDryRunResult(null);
              }}
            />
            Dry run (preview scripts without submitting)
          </label>

          {/* Notification controls */}
          <div
            style={{
              display: "flex",
              flexDirection: "column",
              gap: "var(--sp-2)",
              paddingTop: "var(--sp-3)",
              borderTop: "1px solid var(--border-ghost)",
            }}
          >
            <label
              title={
                hasEndpoints
                  ? "Notify via the notification pipeline when the run finishes"
                  : "Add an endpoint in Settings → Notifications first."
              }
              style={{
                display: "flex",
                alignItems: "center",
                gap: "var(--sp-2)",
                fontSize: "0.8rem",
                cursor: hasEndpoints ? "pointer" : "not-allowed",
                color: hasEndpoints
                  ? "var(--text-primary)"
                  : "var(--text-muted)",
              }}
            >
              <input
                type="checkbox"
                checked={notify && hasEndpoints}
                onChange={(e) => setNotify(e.target.checked)}
                disabled={!hasEndpoints || endpointsLoading || loading}
              />
              {notify && hasEndpoints ? (
                <Bell size={13} aria-hidden="true" />
              ) : (
                <BellOff size={13} aria-hidden="true" />
              )}
              Notify on workflow completion
            </label>
            {!endpointsLoading && !hasEndpoints && (
              <div
                style={{
                  fontSize: "0.72rem",
                  color: "var(--text-muted)",
                  paddingLeft: 24,
                }}
              >
                Add an endpoint in Settings → Notifications first.
              </div>
            )}
            {hasEndpoints && notify && (
              <div
                style={{
                  display: "grid",
                  gridTemplateColumns: "1fr 1fr",
                  gap: "var(--sp-2)",
                  paddingLeft: 24,
                }}
              >
                <select
                  value={selectedEndpointId ?? ""}
                  onChange={(e) =>
                    setSelectedEndpointId(
                      e.target.value ? Number(e.target.value) : null,
                    )
                  }
                  aria-label="Notification endpoint"
                  style={{
                    padding: "var(--sp-2)",
                    background: "var(--bg-base)",
                    border: "1px solid var(--border-subtle)",
                    borderRadius: "var(--radius-md)",
                    color: "var(--text-primary)",
                    fontFamily: "var(--font-mono)",
                    fontSize: "0.8rem",
                  }}
                >
                  {endpointList.map((ep) => (
                    <option key={ep.id} value={ep.id}>
                      {ep.kind}:{ep.name}
                    </option>
                  ))}
                </select>
                <select
                  value={preset}
                  onChange={(e) => {
                    const v = e.target.value;
                    if (isKnownPreset(v)) setPreset(v);
                  }}
                  aria-label="Notification preset"
                  style={{
                    padding: "var(--sp-2)",
                    background: "var(--bg-base)",
                    border: "1px solid var(--border-subtle)",
                    borderRadius: "var(--radius-md)",
                    color: "var(--text-primary)",
                    fontFamily: "var(--font-mono)",
                    fontSize: "0.8rem",
                  }}
                >
                  {NOTIFICATION_PRESETS.map((p) => (
                    <option key={p.value} value={p.value}>
                      {p.label}
                    </option>
                  ))}
                </select>
              </div>
            )}
          </div>

          <ErrorBanner error={error} />

          {/* Dry run results */}
          {dryRunResult && (
            <div
              style={{
                display: "flex",
                flexDirection: "column",
                gap: "var(--sp-2)",
              }}
            >
              <span
                style={{
                  fontSize: "0.7rem",
                  color: "var(--text-muted)",
                  textTransform: "uppercase",
                  letterSpacing: "0.08em",
                }}
              >
                Rendered Scripts ({dryRunResult.length} jobs)
              </span>
              {dryRunResult.map((job) => (
                <div
                  key={job.name}
                  style={{
                    background: "var(--bg-base)",
                    border: "1px solid var(--border-ghost)",
                    borderRadius: "var(--radius-md)",
                    overflow: "hidden",
                  }}
                >
                  <button
                    onClick={() =>
                      setExpandedScript(
                        expandedScript === job.name ? null : job.name,
                      )
                    }
                    style={{
                      width: "100%",
                      display: "flex",
                      alignItems: "center",
                      justifyContent: "space-between",
                      padding: "var(--sp-2) var(--sp-3)",
                      background: "transparent",
                      border: "none",
                      color: "var(--text-secondary)",
                      cursor: "pointer",
                      fontFamily: "var(--font-mono)",
                      fontSize: "0.8rem",
                    }}
                  >
                    <span>{job.name}</span>
                    {expandedScript === job.name ? (
                      <ChevronUp size={12} />
                    ) : (
                      <ChevronDown size={12} />
                    )}
                  </button>
                  {expandedScript === job.name && (
                    <pre
                      style={{
                        padding: "var(--sp-3) var(--sp-4)",
                        margin: 0,
                        fontFamily: "var(--font-mono)",
                        fontSize: "0.7rem",
                        lineHeight: 1.6,
                        color: "var(--text-secondary)",
                        overflow: "auto",
                        maxHeight: 300,
                        borderTop: "1px solid var(--border-ghost)",
                      }}
                    >
                      {job.script}
                    </pre>
                  )}
                </div>
              ))}
            </div>
          )}

          {/* Actions */}
          <div
            style={{
              display: "flex",
              gap: "var(--sp-3)",
              justifyContent: "flex-end",
            }}
          >
            <button className="btn btn-ghost" onClick={onClose}>
              Cancel
            </button>
            <button
              className="btn btn-primary"
              onClick={handleRun}
              disabled={loading}
              style={{ gap: 6 }}
            >
              {loading ? (
                <Loader2 size={14} className="spin" />
              ) : dryRun ? (
                <Eye size={14} />
              ) : (
                <Play size={14} />
              )}
              {dryRun ? "Preview" : "Run Workflow"}
            </button>
          </div>
        </div>
      </motion.div>
    </div>
  );
}
