import { useCallback, useEffect, useState } from "react";
import { Link, useParams } from "react-router-dom";
import { motion } from "framer-motion";
import { ArrowLeft, Grid3x3, ExternalLink } from "lucide-react";
import { workflows as workflowsApi } from "../lib/api.ts";
import type { WorkflowRun, WorkflowRunStatus } from "../lib/types.ts";

/**
 * Read-only detail view for an individual ``workflow_run`` row, reached
 * by clicking a cell row on the Sweep detail page (UI-FIX-1 option B).
 *
 * Intentionally minimal: the in-session :page:`WorkflowDetail`
 * experience owns the "I just submitted this run" lifecycle; this page
 * exists so sweep cell drilldown (and shareable URLs) resolve without
 * a 404. It surfaces the run's DB row + child job ids without trying
 * to reconstruct the DAG visualisation.
 */

const TERMINAL: ReadonlySet<WorkflowRunStatus> = new Set([
  "completed",
  "failed",
  "cancelled",
]);

const STATUS_COLOR: Record<WorkflowRunStatus, string> = {
  pending: "var(--st-pending)",
  running: "var(--st-running)",
  completed: "var(--st-completed)",
  failed: "var(--st-failed)",
  cancelled: "var(--text-muted)",
};

function StatusPill({ status }: { status: WorkflowRunStatus }) {
  return (
    <span
      style={{
        fontFamily: "var(--font-mono)",
        fontSize: "0.7rem",
        padding: "3px 10px",
        borderRadius: 4,
        color: STATUS_COLOR[status],
        background: "var(--bg-overlay)",
        border: `1px solid ${STATUS_COLOR[status]}`,
        textTransform: "uppercase",
        letterSpacing: "0.06em",
      }}
    >
      {status}
    </span>
  );
}

function formatTs(ts: string | null): string {
  if (!ts) return "—";
  try {
    return new Date(ts).toLocaleString();
  } catch {
    return ts;
  }
}

function MetaCell({
  label,
  children,
}: {
  label: string;
  children: React.ReactNode;
}) {
  return (
    <div
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
        {label}
      </div>
      <div
        style={{
          fontFamily: "var(--font-mono)",
          fontSize: "0.85rem",
          color: "var(--text-primary)",
          wordBreak: "break-word",
        }}
      >
        {children}
      </div>
    </div>
  );
}

export function WorkflowRunStandalonePage() {
  const { id: idParam } = useParams<{ id: string }>();
  const [run, setRun] = useState<WorkflowRun | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const load = useCallback(async () => {
    if (!idParam) {
      setError("Missing workflow run id");
      setLoading(false);
      return;
    }
    try {
      const res = await workflowsApi.getRun(idParam);
      setRun(res);
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to load workflow run");
    } finally {
      setLoading(false);
    }
  }, [idParam]);

  useEffect(() => {
    void load();
  }, [load]);

  /* Poll while the run is still active so the child job list updates
     without the user refreshing. Stops as soon as the run reaches a
     terminal status — the sweep detail page already handles drill-
     backs independently. */
  useEffect(() => {
    if (!run) return;
    if (TERMINAL.has(run.status)) return;
    const h = setInterval(() => void load(), 10000);
    return () => clearInterval(h);
  }, [run, load]);

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

  if (error || !run) {
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
          Failed to load workflow run
        </div>
        <div
          style={{
            fontFamily: "var(--font-mono)",
            fontSize: "0.8rem",
            color: "var(--text-muted)",
          }}
        >
          {error ?? "not found"}
        </div>
      </div>
    );
  }

  const sweepRunId = run.sweep_run_id ?? null;
  const backTo = sweepRunId !== null ? `/sweep_runs/${sweepRunId}` : "/jobs";
  const backLabel =
    sweepRunId !== null ? `Back to Sweep #${sweepRunId}` : "Back to Jobs";

  const jobRows = Object.entries(run.job_ids ?? {});

  return (
    <div
      style={{ display: "flex", flexDirection: "column", gap: "var(--sp-4)" }}
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
            to={backTo}
            style={{
              display: "flex",
              alignItems: "center",
              gap: 6,
              padding: "6px 10px",
              borderRadius: 6,
              color: "var(--text-muted)",
              border: "1px solid var(--border-subtle)",
              fontSize: "0.75rem",
              fontFamily: "var(--font-mono)",
            }}
            title={backLabel}
            data-testid="workflow-run-back"
          >
            <ArrowLeft size={14} />
            {sweepRunId !== null ? <Grid3x3 size={12} /> : null}
            {backLabel}
          </Link>
          <div>
            <h1 style={{ fontSize: "1.3rem" }}>{run.workflow_name}</h1>
            <span
              style={{
                fontFamily: "var(--font-mono)",
                fontSize: "0.75rem",
                color: "var(--text-muted)",
              }}
            >
              workflow_run #{run.id}
              {sweepRunId !== null ? ` · sweep #${sweepRunId}` : ""}
            </span>
          </div>
        </div>
        <StatusPill status={run.status} />
      </motion.div>

      {/* Error banner */}
      {run.error && (
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
          {run.error}
        </div>
      )}

      {/* Meta panel */}
      <motion.div
        initial={{ opacity: 0 }}
        animate={{ opacity: 1 }}
        transition={{ delay: 0.05 }}
        className="panel"
        style={{ padding: "var(--sp-4)" }}
      >
        <div
          style={{
            display: "grid",
            gridTemplateColumns: "repeat(auto-fit, minmax(180px, 1fr))",
            gap: "var(--sp-3)",
          }}
        >
          <MetaCell label="started">{formatTs(run.started_at)}</MetaCell>
          <MetaCell label="completed">{formatTs(run.completed_at)}</MetaCell>
          <MetaCell label="job count">{jobRows.length}</MetaCell>
          {sweepRunId !== null && (
            <MetaCell label="sweep">
              <Link
                to={`/sweep_runs/${sweepRunId}`}
                style={{ color: "var(--accent)" }}
              >
                #{sweepRunId}
              </Link>
            </MetaCell>
          )}
        </div>
      </motion.div>

      {/* Jobs table */}
      <motion.div
        initial={{ opacity: 0 }}
        animate={{ opacity: 1 }}
        transition={{ delay: 0.1 }}
        className="panel"
        style={{ overflow: "hidden" }}
      >
        <div className="panel-header">
          <h3 style={{ textTransform: "none", letterSpacing: 0 }}>
            Jobs ({jobRows.length})
          </h3>
        </div>
        <div style={{ overflow: "auto", maxHeight: "50vh" }}>
          <table className="data-table">
            <thead>
              <tr>
                <th>Name</th>
                <th style={{ width: 120 }}>Status</th>
                <th style={{ width: 140 }}>Job ID</th>
                <th style={{ width: 80 }}>Logs</th>
              </tr>
            </thead>
            <tbody>
              {jobRows.length > 0 ? (
                jobRows.map(([name, jobIdStr]) => {
                  const jobStatus = run.job_statuses?.[name] ?? "UNKNOWN";
                  return (
                    <tr key={name} data-testid={`wf-run-job-${name}`}>
                      <td style={{ fontWeight: 500 }}>{name}</td>
                      <td className="col-mono">{jobStatus}</td>
                      <td className="col-mono col-muted">{jobIdStr}</td>
                      <td>
                        <Link
                          to={`/jobs/${jobIdStr}/logs`}
                          className="btn btn-ghost"
                          style={{ padding: "4px 8px" }}
                          title="View logs"
                        >
                          <ExternalLink size={13} />
                        </Link>
                      </td>
                    </tr>
                  );
                })
              ) : (
                <tr>
                  <td
                    colSpan={4}
                    style={{
                      textAlign: "center",
                      color: "var(--text-muted)",
                      padding: 32,
                    }}
                  >
                    No jobs recorded for this workflow run yet.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </motion.div>
    </div>
  );
}
