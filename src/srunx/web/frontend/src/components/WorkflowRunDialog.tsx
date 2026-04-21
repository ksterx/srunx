import { useEffect, useMemo, useState } from "react";
import { useNavigate } from "react-router-dom";
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
  Plus,
  List as ListIcon,
  Minus,
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
  /**
   * Workflow-level ``args`` keys seeded into the editor. The user
   * can add new keys on top of these — the dialog stays usable when
   * the workflow defines no args at all.
   */
  args?: Record<string, string>;
  onClose: () => void;
  onRunStarted: (run: Record<string, unknown>) => void;
  /**
   * Sweep submissions hand back ``{sweep_run_id, status, cell_count}``
   * with HTTP 202 and don't match the ``WorkflowRun`` shape. Callers
   * that want to navigate to the sweep detail page pass this handler;
   * the dialog falls back to ``onRunStarted`` when it isn't provided
   * so legacy callers keep compiling.
   */
  onSweepStarted?: (sweepRunId: number) => void;
};

type ArgMode = "single" | "list";

type ArgEntry = {
  /** Monotonic id for stable React keys across name renames. */
  id: number;
  name: string;
  mode: ArgMode;
  /** Raw text contents; in ``list`` mode this is comma-separated. */
  value: string;
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

/** Parse a ``list``-mode raw string into matrix values. Empty elements
 * are preserved as ``""`` to match the CLI contract (R3.9). */
function parseListValues(raw: string): string[] {
  if (raw === "") return [];
  return raw.split(",").map((s) => s.trim());
}

/** Seed the editor with existing workflow-level ``args`` in ``single``
 * mode. When the workflow defines no args we start with a single
 * blank row so the user has a visible target to edit. */
function seedArgEntries(args: Record<string, string> | undefined): ArgEntry[] {
  const entries = Object.entries(args ?? {});
  if (entries.length === 0) return [];
  return entries.map<ArgEntry>(([name, value], i) => ({
    id: i,
    name,
    mode: "single",
    value,
  }));
}

export function WorkflowRunDialog({
  workflowName,
  mount,
  jobNames,
  args,
  onClose,
  onRunStarted,
  onSweepStarted,
}: WorkflowRunDialogProps) {
  const navigate = useNavigate();
  const [mode, setMode] = useState<ExecutionMode>("full");
  const [fromJob, setFromJob] = useState(jobNames[0] ?? "");
  const [toJob, setToJob] = useState(jobNames[jobNames.length - 1] ?? "");
  const [singleJob, setSingleJob] = useState(jobNames[0] ?? "");
  const [dryRun, setDryRun] = useState(false);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  /* ── Args / Sweep state ───────────────────── */
  const [argEntries, setArgEntries] = useState<ArgEntry[]>(() =>
    seedArgEntries(args),
  );
  const [nextArgId, setNextArgId] = useState<number>(
    () => seedArgEntries(args).length,
  );
  // Snapshot of the workflow's original args so we can emit ONLY changed
  // keys to ``args_override`` at submit time (UI-FIX-2). The backend is
  // idempotent w.r.t. unchanged values, but always sending them makes
  // the Web access log noisy and makes it impossible to tell a deliberate
  // override from a passthrough.
  const initialArgs = useMemo<Record<string, string>>(
    () => ({ ...(args ?? {}) }),
    [args],
  );
  const [advancedOpen, setAdvancedOpen] = useState(false);
  const [failFast, setFailFast] = useState(false);
  const [maxParallel, setMaxParallel] = useState(4);
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

  /* ── Derived sweep / args payloads ──────────── */

  /**
   * Split the arg editor rows into:
   * - ``single`` mode rows → ``args_override`` map
   * - ``list`` mode rows   → ``sweep.matrix`` axes
   * Rows with empty names are ignored (the UX keeps them editable
   * rather than auto-removing the row). In ``list`` mode an empty
   * raw value yields a zero-length axis which we also drop —
   * the backend rejects empty matrix axes (R2.4) and surfacing that
   * as "submit disabled" is clearer than a 422.
   */
  const { argsOverride, matrix, invalidReason } = useMemo(() => {
    const ov: Record<string, string> = {};
    const mx: Record<string, string[]> = {};
    const seen = new Set<string>();
    let problem: string | null = null;
    for (const row of argEntries) {
      const name = row.name.trim();
      if (!name) continue;
      if (seen.has(name)) {
        problem = `Duplicate arg key "${name}"`;
        continue;
      }
      seen.add(name);
      if (row.mode === "single") {
        ov[name] = row.value;
      } else {
        const values = parseListValues(row.value);
        if (values.length === 0) {
          problem = `List axis "${name}" must have at least one value`;
          continue;
        }
        mx[name] = values;
      }
    }
    return { argsOverride: ov, matrix: mx, invalidReason: problem };
  }, [argEntries]);

  const isSweepMode = Object.keys(matrix).length > 0;

  /**
   * Only include keys the user actually changed (UI-FIX-2). A key is
   * "changed" when it (a) is absent from the workflow's initial args
   * (i.e. a brand-new user-added key) or (b) its string value differs
   * from the seed. This keeps HTTP bodies tidy and lets backend logs
   * distinguish intentional overrides from dialog passthrough.
   */
  const argsOverrideDiff = useMemo<Record<string, string>>(() => {
    const diff: Record<string, string> = {};
    for (const [k, v] of Object.entries(argsOverride)) {
      if (!(k in initialArgs) || initialArgs[k] !== v) {
        diff[k] = v;
      }
    }
    return diff;
  }, [argsOverride, initialArgs]);

  const cellCount = useMemo(
    () =>
      Math.max(
        1,
        Object.values(matrix).reduce((acc, axis) => acc * axis.length, 1),
      ),
    [matrix],
  );

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
    if (Object.keys(argsOverrideDiff).length > 0) {
      opts.args_override = argsOverrideDiff;
    }
    if (isSweepMode) {
      opts.sweep = {
        matrix,
        fail_fast: failFast,
        max_parallel: Math.max(1, maxParallel),
      };
    }
    return opts;
  };

  const SWEEP_CONFIRM_THRESHOLD = 10;

  const handleRun = async () => {
    if (invalidReason) {
      setError(invalidReason);
      return;
    }
    // R8.3: confirm before submitting large sweeps. This is the same
    // threshold spec'd in the requirements (fixed 10 in Phase 1).
    if (isSweepMode && cellCount > SWEEP_CONFIRM_THRESHOLD) {
      const ok = window.confirm(
        `This sweep will submit ${cellCount} workflow runs. Continue?`,
      );
      if (!ok) return;
    }
    setLoading(true);
    setError(null);
    setDryRunResult(null);
    try {
      const opts = buildOptions();
      const result = await workflowsApi.run(workflowName, mount, opts);
      if ("dry_run" in result && result.dry_run) {
        setDryRunResult(result.jobs as DryRunJobInfo[]);
        return;
      }
      if (
        typeof result === "object" &&
        result !== null &&
        "sweep_run_id" in result
      ) {
        // Backend returned the sweep-start envelope. Prefer the
        // dedicated callback; fall back to navigation so existing
        // callers (that don't know about sweeps) still land on a
        // useful page instead of being left on the run dialog.
        const sweepId = Number(
          (result as { sweep_run_id: number | string }).sweep_run_id,
        );
        if (onSweepStarted) {
          onSweepStarted(sweepId);
        } else {
          navigate(`/sweep_runs/${sweepId}`);
        }
        onClose();
        return;
      }
      onRunStarted(result);
      onClose();
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

          {/* Workflow args editor (+ sweep list toggle) */}
          <ArgsEditor
            entries={argEntries}
            onChange={setArgEntries}
            nextId={nextArgId}
            setNextId={setNextArgId}
          />

          {/* Sweep preview */}
          {isSweepMode && (
            <div
              data-testid="sweep-preview"
              style={{
                display: "flex",
                alignItems: "center",
                gap: "var(--sp-3)",
                padding: "var(--sp-2) var(--sp-3)",
                background: "var(--accent-dim)",
                border: "1px solid var(--accent)",
                borderRadius: "var(--radius-md)",
                fontFamily: "var(--font-mono)",
                fontSize: "0.78rem",
                color: "var(--accent)",
              }}
            >
              <ListIcon size={13} />
              <span>
                Sweep: <strong>{cellCount}</strong> cells ·{" "}
                {Object.entries(matrix)
                  .map(([k, v]) => `${k}[${v.length}]`)
                  .join(" × ")}
              </span>
            </div>
          )}

          {/* Advanced (sweep-only options) */}
          {isSweepMode && (
            <div
              style={{
                borderTop: "1px solid var(--border-ghost)",
                paddingTop: "var(--sp-3)",
                display: "flex",
                flexDirection: "column",
                gap: "var(--sp-2)",
              }}
            >
              <button
                type="button"
                onClick={() => setAdvancedOpen((v) => !v)}
                aria-expanded={advancedOpen}
                style={{
                  display: "flex",
                  alignItems: "center",
                  gap: 6,
                  background: "transparent",
                  border: "none",
                  padding: 0,
                  color: "var(--text-muted)",
                  cursor: "pointer",
                  fontFamily: "var(--font-display)",
                  fontSize: "0.7rem",
                  textTransform: "uppercase",
                  letterSpacing: "0.08em",
                }}
              >
                {advancedOpen ? (
                  <ChevronUp size={12} />
                ) : (
                  <ChevronDown size={12} />
                )}
                Advanced
              </button>
              {advancedOpen && (
                <div
                  style={{
                    display: "grid",
                    gridTemplateColumns: "1fr 1fr",
                    gap: "var(--sp-3)",
                    paddingLeft: 18,
                  }}
                >
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
                      checked={failFast}
                      onChange={(e) => setFailFast(e.target.checked)}
                    />
                    fail_fast
                  </label>
                  <label
                    style={{
                      display: "flex",
                      alignItems: "center",
                      gap: "var(--sp-2)",
                      fontSize: "0.8rem",
                    }}
                  >
                    <span
                      style={{
                        fontFamily: "var(--font-mono)",
                        color: "var(--text-muted)",
                      }}
                    >
                      max_parallel
                    </span>
                    <input
                      type="number"
                      min={1}
                      value={maxParallel}
                      onChange={(e) => {
                        const n = Number(e.target.value);
                        setMaxParallel(
                          Number.isFinite(n) && n >= 1 ? n : maxParallel,
                        );
                      }}
                      aria-label="max_parallel"
                      style={{
                        width: 80,
                        padding: "var(--sp-1) var(--sp-2)",
                        background: "var(--bg-base)",
                        border: "1px solid var(--border-subtle)",
                        borderRadius: "var(--radius-sm)",
                        color: "var(--text-primary)",
                        fontFamily: "var(--font-mono)",
                        fontSize: "0.78rem",
                      }}
                    />
                  </label>
                </div>
              )}
            </div>
          )}

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
              {dryRun
                ? "Preview"
                : isSweepMode
                  ? `Run Sweep (${cellCount})`
                  : "Run Workflow"}
            </button>
          </div>
        </div>
      </motion.div>
    </div>
  );
}

/* ── Args editor ─────────────────────────────── */

type ArgsEditorProps = {
  entries: ArgEntry[];
  onChange: (entries: ArgEntry[]) => void;
  nextId: number;
  setNextId: (n: number) => void;
};

function ArgsEditor({ entries, onChange, nextId, setNextId }: ArgsEditorProps) {
  const update = (id: number, patch: Partial<ArgEntry>) => {
    onChange(entries.map((e) => (e.id === id ? { ...e, ...patch } : e)));
  };
  const remove = (id: number) => {
    onChange(entries.filter((e) => e.id !== id));
  };
  const add = () => {
    onChange([...entries, { id: nextId, name: "", mode: "single", value: "" }]);
    setNextId(nextId + 1);
  };

  return (
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
        Args
      </span>
      {entries.length === 0 && (
        <span
          style={{
            fontSize: "0.72rem",
            color: "var(--text-muted)",
            fontStyle: "italic",
          }}
        >
          No args defined. Add one below to override workflow values or build a
          parameter sweep.
        </span>
      )}
      {entries.map((entry) => (
        <ArgRow
          key={entry.id}
          entry={entry}
          onUpdate={(patch) => update(entry.id, patch)}
          onRemove={() => remove(entry.id)}
        />
      ))}
      <button
        type="button"
        onClick={add}
        data-testid="add-arg-button"
        style={{
          alignSelf: "flex-start",
          display: "flex",
          alignItems: "center",
          gap: 6,
          padding: "4px 10px",
          background: "none",
          border: "1px dashed var(--border-subtle)",
          borderRadius: "var(--radius-sm)",
          color: "var(--text-muted)",
          fontFamily: "var(--font-mono)",
          fontSize: "0.7rem",
          textTransform: "uppercase",
          letterSpacing: "0.04em",
          cursor: "pointer",
        }}
      >
        <Plus size={12} />
        Add Arg
      </button>
    </div>
  );
}

type ArgRowProps = {
  entry: ArgEntry;
  onUpdate: (patch: Partial<ArgEntry>) => void;
  onRemove: () => void;
};

function ArgRow({ entry, onUpdate, onRemove }: ArgRowProps) {
  const inputStyle: React.CSSProperties = {
    height: 30,
    padding: "var(--sp-1) var(--sp-2)",
    background: "var(--bg-base)",
    border: "1px solid var(--border-subtle)",
    borderRadius: "var(--radius-sm)",
    color: "var(--text-primary)",
    fontFamily: "var(--font-mono)",
    fontSize: "0.76rem",
  };

  return (
    <div
      data-testid={`arg-row-${entry.name || entry.id}`}
      style={{
        display: "grid",
        gridTemplateColumns: "minmax(100px, 140px) auto 1fr auto",
        gap: 6,
        alignItems: "center",
      }}
    >
      <input
        type="text"
        value={entry.name}
        placeholder="name"
        spellCheck={false}
        onChange={(e) => onUpdate({ name: e.target.value })}
        aria-label="arg name"
        style={{ ...inputStyle, color: "var(--accent)" }}
      />

      {/* Mode toggle: single ↔ list */}
      <div
        role="group"
        aria-label="arg mode"
        style={{
          display: "flex",
          border: "1px solid var(--border-subtle)",
          borderRadius: "var(--radius-sm)",
          overflow: "hidden",
        }}
      >
        {(["single", "list"] as const).map((m) => {
          const active = entry.mode === m;
          return (
            <button
              key={m}
              type="button"
              data-testid={`arg-mode-${m}-${entry.name || entry.id}`}
              onClick={() => onUpdate({ mode: m })}
              aria-pressed={active}
              style={{
                padding: "4px 10px",
                background: active ? "var(--accent-dim)" : "transparent",
                color: active ? "var(--accent)" : "var(--text-muted)",
                border: "none",
                cursor: "pointer",
                fontFamily: "var(--font-mono)",
                fontSize: "0.7rem",
                textTransform: "uppercase",
                letterSpacing: "0.04em",
              }}
            >
              {m}
            </button>
          );
        })}
      </div>

      {/* Value input — same control; the placeholder shifts to signal list mode. */}
      <input
        type="text"
        value={entry.value}
        placeholder={entry.mode === "list" ? "v1, v2, v3" : "value"}
        spellCheck={false}
        onChange={(e) => onUpdate({ value: e.target.value })}
        aria-label={
          entry.mode === "list" ? "arg values (comma-separated)" : "arg value"
        }
        style={inputStyle}
      />

      <button
        type="button"
        onClick={onRemove}
        aria-label={`remove ${entry.name || "arg"}`}
        style={{
          width: 26,
          height: 26,
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
          background: "none",
          border: "none",
          color: "var(--text-muted)",
          cursor: "pointer",
          borderRadius: "var(--radius-sm)",
        }}
      >
        <Minus size={12} />
      </button>
    </div>
  );
}
