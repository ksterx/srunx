import { useEffect, useState } from "react";
import { motion } from "framer-motion";
import { Play, Eye, Loader2, X, ChevronDown, ChevronUp } from "lucide-react";
import { workflows as workflowsApi } from "../lib/api.ts";
import type { WorkflowRunOptions, DryRunJobInfo } from "../lib/types.ts";

type WorkflowRunDialogProps = {
  workflowName: string;
  mount: string;
  jobNames: string[];
  onClose: () => void;
  onRunStarted: (run: Record<string, unknown>) => void;
};

type ExecutionMode = "full" | "from" | "to" | "range" | "single";

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

  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
    };
    document.addEventListener("keydown", handler);
    return () => document.removeEventListener("keydown", handler);
  }, [onClose]);

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

          {error && (
            <div
              style={{
                padding: "var(--sp-3) var(--sp-4)",
                background: "var(--st-failed-dim)",
                border: "1px solid rgba(244,63,94,0.3)",
                borderRadius: "var(--radius-md)",
                color: "var(--st-failed)",
                fontFamily: "var(--font-mono)",
                fontSize: "0.8rem",
              }}
            >
              {error}
            </div>
          )}

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
