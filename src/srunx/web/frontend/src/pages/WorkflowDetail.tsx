import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  useParams,
  Link,
  useNavigate,
  useSearchParams,
} from "react-router-dom";
import { motion } from "framer-motion";
import {
  ArrowLeft,
  Pencil,
  Play,
  RefreshCw,
  List,
  Trash2,
  XCircle,
} from "lucide-react";
import { useApi } from "../hooks/use-api.ts";
import { workflows as workflowsApi } from "../lib/api.ts";
import type {
  JobStatus,
  RunnableJob,
  WorkflowRun,
  WorkflowRunStatus,
} from "../lib/types.ts";
import { DAGView } from "../components/DAGView.tsx";
import { StatusBadge } from "../components/StatusBadge.tsx";
import { WorkflowRunDialog } from "../components/WorkflowRunDialog.tsx";

const TERMINAL_STATUSES: ReadonlySet<WorkflowRunStatus> = new Set([
  "completed",
  "failed",
  "cancelled",
]);

const RUN_STATUS_COLORS: Record<WorkflowRunStatus, string> = {
  syncing: "var(--st-pending)",
  submitting: "var(--st-pending)",
  running: "var(--st-running)",
  completed: "var(--st-completed)",
  failed: "var(--st-failed)",
  cancelled: "var(--text-muted)",
};

const RUN_STATUS_LABELS: Record<WorkflowRunStatus, string> = {
  syncing: "Syncing...",
  submitting: "Submitting...",
  running: "Running",
  completed: "Completed",
  failed: "Failed",
  cancelled: "Cancelled",
};

export function WorkflowDetail() {
  const { name } = useParams<{ name: string }>();
  const [searchParams] = useSearchParams();
  const mount = searchParams.get("mount");
  const navigate = useNavigate();
  const [selectedJob, setSelectedJob] = useState<string | null>(null);
  const [view, setView] = useState<"dag" | "list">("dag");

  /* ── Run state ──────────────────────────────── */
  const [runData, setRunData] = useState<WorkflowRun | null>(null);
  const [runError, setRunError] = useState<string | null>(null);
  const [showRunDialog, setShowRunDialog] = useState(false);
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null);

  if (!name || !mount) {
    return (
      <div
        style={{ padding: 48, textAlign: "center", color: "var(--text-muted)" }}
      >
        {!name ? "Invalid workflow name" : "No mount specified"}
      </div>
    );
  }

  const {
    data: workflow,
    loading,
    error,
  } = useApi(() => workflowsApi.get(name, mount), [name, mount], {
    pollInterval: 10000,
  });

  /* ── Stop polling on unmount ──────────────── */
  useEffect(() => {
    return () => {
      if (pollRef.current) clearInterval(pollRef.current);
    };
  }, []);

  /* ── Poll run status when runData is set ──── */
  useEffect(() => {
    if (!runData) return;
    if (TERMINAL_STATUSES.has(runData.status)) {
      if (pollRef.current) {
        clearInterval(pollRef.current);
        pollRef.current = null;
      }
      return;
    }

    // Start polling if not already
    if (pollRef.current) clearInterval(pollRef.current);
    pollRef.current = setInterval(async () => {
      try {
        const updated = await workflowsApi.getRun(runData.id);
        setRunData(updated);
        if (TERMINAL_STATUSES.has(updated.status)) {
          if (pollRef.current) {
            clearInterval(pollRef.current);
            pollRef.current = null;
          }
        }
      } catch {
        // Ignore transient fetch errors during polling
      }
    }, 10000);

    return () => {
      if (pollRef.current) {
        clearInterval(pollRef.current);
        pollRef.current = null;
      }
    };
  }, [runData?.id, runData?.status]);

  /* ── Run handler (via dialog) ────────────── */
  const handleRunStarted = useCallback(async (runId: string) => {
    try {
      const run = await workflowsApi.getRun(runId);
      setRunData(run);
    } catch (err) {
      setRunError(
        err instanceof Error ? err.message : "Failed to fetch run status",
      );
    }
  }, []);

  /* ── Delete handler ────────────────────────── */
  const handleDelete = useCallback(async () => {
    if (!name) return;
    if (!window.confirm(`Delete workflow "${name}"? This cannot be undone.`))
      return;
    try {
      await workflowsApi.delete(name, mount);
      navigate("/workflows");
    } catch (err) {
      setRunError(
        err instanceof Error ? err.message : "Failed to delete workflow",
      );
    }
  }, [name, mount, navigate]);

  /* ── Cancel handler ────────────────────────── */
  const handleCancel = useCallback(async () => {
    if (!runData) return;
    try {
      await workflowsApi.cancelRun(runData.id);
      setRunData((prev) => (prev ? { ...prev, status: "cancelled" } : null));
    } catch (err) {
      setRunError(err instanceof Error ? err.message : "Failed to cancel run");
    }
  }, [runData]);

  /* ── Merge run status into jobs ───────────── */
  const liveJobs: RunnableJob[] = useMemo(() => {
    if (!workflow) return [];
    return workflow.jobs.map((job) => {
      const runStatus = runData?.job_statuses[job.name] as
        | JobStatus
        | undefined;
      const runJobId = runData?.job_ids[job.name];
      return {
        ...job,
        status: runStatus ?? job.status,
        job_id: runJobId ? Number(runJobId) : job.job_id,
      };
    });
  }, [workflow, runData]);

  const selected = liveJobs.find((j) => j.name === selectedJob);

  if (loading) {
    return (
      <div style={{ padding: 48, textAlign: "center" }}>
        <div
          className="skeleton"
          style={{ width: 200, height: 24, margin: "0 auto" }}
        />
      </div>
    );
  }

  if (error) {
    return (
      <div
        style={{ padding: 48, textAlign: "center", color: "var(--st-failed)" }}
      >
        <div
          style={{
            fontFamily: "var(--font-display)",
            fontSize: "1.1rem",
            marginBottom: 8,
          }}
        >
          Failed to load workflow
        </div>
        <div
          style={{
            fontFamily: "var(--font-mono)",
            fontSize: "0.8rem",
            color: "var(--text-muted)",
          }}
        >
          {error}
        </div>
      </div>
    );
  }

  if (!workflow) {
    return (
      <div
        style={{ padding: 48, textAlign: "center", color: "var(--text-muted)" }}
      >
        Workflow &ldquo;{name}&rdquo; not found
      </div>
    );
  }

  return (
    <div
      style={{
        display: "flex",
        flexDirection: "column",
        gap: "var(--sp-4)",
        height: "100%",
      }}
    >
      {/* Header */}
      <motion.div
        initial={{ opacity: 0, y: 12 }}
        animate={{ opacity: 1, y: 0 }}
        style={{
          display: "flex",
          justifyContent: "space-between",
          alignItems: "center",
        }}
      >
        <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
          <Link
            to="/workflows"
            style={{
              display: "flex",
              padding: 6,
              borderRadius: 6,
              color: "var(--text-muted)",
              border: "1px solid var(--border-subtle)",
            }}
          >
            <ArrowLeft size={16} />
          </Link>
          <div>
            <h1 style={{ fontSize: "1.3rem" }}>{workflow.name}</h1>
            <span
              style={{
                fontFamily: "var(--font-mono)",
                fontSize: "0.75rem",
                color: "var(--text-muted)",
              }}
            >
              {workflow.jobs.length} jobs
            </span>
          </div>
        </div>

        <div style={{ display: "flex", gap: 8 }}>
          {/* View toggle */}
          <div
            style={{
              display: "flex",
              border: "1px solid var(--border-default)",
              borderRadius: 6,
              overflow: "hidden",
            }}
          >
            {(["dag", "list"] as const).map((v) => (
              <button
                key={v}
                onClick={() => setView(v)}
                style={{
                  padding: "6px 12px",
                  background: view === v ? "var(--accent-dim)" : "transparent",
                  color: view === v ? "var(--accent)" : "var(--text-muted)",
                  border: "none",
                  cursor: "pointer",
                  fontFamily: "var(--font-display)",
                  fontSize: "0.75rem",
                  textTransform: "uppercase",
                  letterSpacing: "0.06em",
                  display: "flex",
                  alignItems: "center",
                  gap: 6,
                }}
              >
                {v === "dag" ? <RefreshCw size={12} /> : <List size={12} />}
                {v}
              </button>
            ))}
          </div>

          <Link
            to={`/workflows/${encodeURIComponent(name)}/edit?mount=${encodeURIComponent(mount)}`}
            className="btn btn-ghost"
            title="Edit workflow"
          >
            <Pencil size={14} />
            Edit
          </Link>

          {/* Run status badge */}
          {runData && (
            <span
              style={{
                fontFamily: "var(--font-mono)",
                fontSize: "0.7rem",
                padding: "3px 10px",
                borderRadius: 4,
                color: RUN_STATUS_COLORS[runData.status],
                background: "var(--bg-overlay)",
                border: `1px solid ${RUN_STATUS_COLORS[runData.status]}`,
                textTransform: "uppercase",
                letterSpacing: "0.04em",
              }}
            >
              {RUN_STATUS_LABELS[runData.status]}
            </span>
          )}

          <button
            className="btn btn-primary"
            onClick={() => setShowRunDialog(true)}
            disabled={!!runData && !TERMINAL_STATUSES.has(runData.status)}
            style={{
              opacity:
                runData && !TERMINAL_STATUSES.has(runData.status) ? 0.6 : 1,
            }}
          >
            <Play size={14} />
            Run Workflow
          </button>

          {/* Cancel button — visible only when a run is active */}
          {runData && !TERMINAL_STATUSES.has(runData.status) && (
            <button className="btn btn-danger" onClick={handleCancel}>
              <XCircle size={14} />
              Cancel
            </button>
          )}

          <button
            className="btn btn-danger"
            onClick={handleDelete}
            title="Delete workflow"
          >
            <Trash2 size={14} />
          </button>
        </div>
      </motion.div>

      {/* Run error */}
      {runError && (
        <div
          style={{
            padding: "var(--sp-3) var(--sp-5)",
            background: "var(--st-failed-dim)",
            border: "1px solid rgba(244,63,94,0.3)",
            borderRadius: "var(--radius-md)",
            color: "var(--st-failed)",
            fontFamily: "var(--font-mono)",
            fontSize: "0.8rem",
          }}
        >
          {runError}
        </div>
      )}

      {/* Main content */}
      <div
        style={{
          flex: 1,
          display: "grid",
          gridTemplateColumns: selectedJob ? "1fr 340px" : "1fr",
          gap: "var(--sp-4)",
          minHeight: 0,
        }}
      >
        {/* DAG / List view */}
        <motion.div
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          transition={{ delay: 0.1 }}
          className="panel"
          style={{ overflow: "hidden" }}
        >
          {view === "dag" ? (
            <DAGView
              jobs={liveJobs}
              onJobClick={(name) =>
                setSelectedJob((prev) => (prev === name ? null : name))
              }
            />
          ) : (
            <div style={{ overflow: "auto", height: "100%" }}>
              <table className="data-table">
                <thead>
                  <tr>
                    <th>Name</th>
                    <th>Status</th>
                    <th>Depends On</th>
                    <th>Command</th>
                    <th>GPUs</th>
                  </tr>
                </thead>
                <tbody>
                  {liveJobs.map((job) => (
                    <tr
                      key={job.name}
                      onClick={() =>
                        setSelectedJob((prev) =>
                          prev === job.name ? null : job.name,
                        )
                      }
                      style={{
                        cursor: "pointer",
                        background:
                          selectedJob === job.name
                            ? "var(--accent-dim)"
                            : undefined,
                      }}
                    >
                      <td style={{ fontWeight: 500 }}>{job.name}</td>
                      <td>
                        <StatusBadge status={job.status} size="sm" />
                      </td>
                      <td
                        className="col-mono col-muted"
                        style={{ fontSize: "0.75rem" }}
                      >
                        {job.depends_on?.join(", ") || "—"}
                      </td>
                      <td
                        className="col-mono col-muted truncate"
                        style={{ maxWidth: 200 }}
                      >
                        {"command" in job
                          ? job.command.join(" ")
                          : job.script_path}
                      </td>
                      <td className="col-mono">
                        {"resources" in job
                          ? (job.resources.gpus_per_node ?? 0)
                          : "—"}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </motion.div>

        {/* Job detail sidebar */}
        {selected && (
          <motion.div
            initial={{ opacity: 0, x: 16 }}
            animate={{ opacity: 1, x: 0 }}
            transition={{ duration: 0.25, ease: [0.16, 1, 0.3, 1] }}
            className="panel"
            style={{ overflow: "auto" }}
          >
            <div className="panel-header">
              <h3>{selected.name}</h3>
              <button
                onClick={() => setSelectedJob(null)}
                style={{
                  background: "none",
                  border: "none",
                  color: "var(--text-muted)",
                  cursor: "pointer",
                  fontSize: "1.2rem",
                  lineHeight: 1,
                }}
              >
                &times;
              </button>
            </div>
            <div
              className="panel-body"
              style={{ display: "flex", flexDirection: "column", gap: 16 }}
            >
              <div>
                <span className="metric-label">Status</span>
                <div style={{ marginTop: 6 }}>
                  <StatusBadge status={selected.status} />
                </div>
              </div>

              {selected.job_id && (
                <div>
                  <span className="metric-label">Job ID</span>
                  <div
                    style={{
                      fontFamily: "var(--font-mono)",
                      fontSize: "0.9rem",
                      marginTop: 4,
                    }}
                  >
                    {selected.job_id}
                  </div>
                </div>
              )}

              <div>
                <span className="metric-label">Command</span>
                <div
                  style={{
                    fontFamily: "var(--font-mono)",
                    fontSize: "0.8rem",
                    color: "var(--text-secondary)",
                    marginTop: 4,
                    padding: "8px 12px",
                    background: "var(--bg-base)",
                    borderRadius: 4,
                    border: "1px solid var(--border-ghost)",
                    wordBreak: "break-all",
                  }}
                >
                  {"command" in selected
                    ? selected.command.join(" ")
                    : selected.script_path}
                </div>
              </div>

              {selected.depends_on && selected.depends_on.length > 0 && (
                <div>
                  <span className="metric-label">Dependencies</span>
                  <div
                    style={{
                      display: "flex",
                      flexWrap: "wrap",
                      gap: 6,
                      marginTop: 6,
                    }}
                  >
                    {selected.depends_on.map((dep) => (
                      <span
                        key={dep}
                        style={{
                          fontFamily: "var(--font-mono)",
                          fontSize: "0.7rem",
                          padding: "2px 8px",
                          borderRadius: 3,
                          background: "var(--bg-overlay)",
                          color: "var(--accent)",
                          border: "1px solid var(--border-ghost)",
                        }}
                      >
                        {dep}
                      </span>
                    ))}
                  </div>
                </div>
              )}

              {"resources" in selected && (
                <div>
                  <span className="metric-label">Resources</span>
                  <div
                    style={{
                      display: "grid",
                      gridTemplateColumns: "1fr 1fr",
                      gap: 8,
                      marginTop: 6,
                    }}
                  >
                    {[
                      { label: "Nodes", value: selected.resources.nodes ?? 1 },
                      {
                        label: "GPUs",
                        value: selected.resources.gpus_per_node ?? 0,
                      },
                      {
                        label: "Partition",
                        value: selected.resources.partition ?? "—",
                      },
                      {
                        label: "Time",
                        value: selected.resources.time_limit ?? "—",
                      },
                    ].map((item) => (
                      <div
                        key={item.label}
                        style={{
                          padding: "8px 10px",
                          background: "var(--bg-base)",
                          borderRadius: 4,
                          border: "1px solid var(--border-ghost)",
                        }}
                      >
                        <div
                          style={{
                            fontSize: "0.6rem",
                            textTransform: "uppercase",
                            letterSpacing: "0.1em",
                            color: "var(--text-muted)",
                            marginBottom: 2,
                          }}
                        >
                          {item.label}
                        </div>
                        <div
                          style={{
                            fontFamily: "var(--font-mono)",
                            fontSize: "0.85rem",
                            color: "var(--text-primary)",
                          }}
                        >
                          {item.value}
                        </div>
                      </div>
                    ))}
                  </div>
                </div>
              )}

              {/* Actions */}
              {(() => {
                const jobId =
                  runData?.job_ids[selected.name] ?? selected.job_id;
                return jobId ? (
                  <Link
                    to={`/jobs/${jobId}/logs`}
                    className="btn btn-ghost"
                    style={{ justifyContent: "center", marginTop: 8 }}
                  >
                    View Logs
                  </Link>
                ) : null;
              })()}
            </div>
          </motion.div>
        )}
      </div>

      {/* Run dialog */}
      {showRunDialog && workflow && (
        <WorkflowRunDialog
          workflowName={name}
          mount={mount}
          jobNames={workflow.jobs.map((j) => j.name)}
          onClose={() => setShowRunDialog(false)}
          onRunStarted={(runId) => {
            setShowRunDialog(false);
            handleRunStarted(runId);
          }}
        />
      )}
    </div>
  );
}
