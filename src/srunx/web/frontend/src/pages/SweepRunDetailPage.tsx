import { useCallback, useEffect, useMemo, useState } from "react";
import { Link, useParams } from "react-router-dom";
import { motion } from "framer-motion";
import { ArrowLeft, XCircle, ExternalLink } from "lucide-react";
import { sweepRuns as sweepRunsApi } from "../lib/api.ts";
import type {
  SweepCellRow,
  SweepRun,
  SweepStatus,
  WorkflowRunStatus,
} from "../lib/types.ts";
import { StatusBadge } from "../components/StatusBadge.tsx";
import { ErrorBanner } from "../components/ErrorBanner.tsx";

const TERMINAL_SWEEP_STATUSES: ReadonlySet<SweepStatus> = new Set([
  "completed",
  "failed",
  "cancelled",
]);

const SWEEP_STATUS_COLOR: Record<SweepStatus, string> = {
  pending: "var(--st-pending)",
  running: "var(--st-running)",
  draining: "var(--st-pending)",
  completed: "var(--st-completed)",
  failed: "var(--st-failed)",
  cancelled: "var(--text-muted)",
};

/** Map the lowercase workflow_run status returned by
 *  ``/api/sweep_runs/{id}/cells`` onto the uppercase ``JobStatus`` that
 *  the shared :component:`StatusBadge` expects. The two enums overlap
 *  semantically but use different casing. */
function workflowRunStatusToBadge(status: WorkflowRunStatus): string {
  switch (status) {
    case "pending":
      return "PENDING";
    case "running":
      return "RUNNING";
    case "completed":
      return "COMPLETED";
    case "failed":
      return "FAILED";
    case "cancelled":
      return "CANCELLED";
    default:
      return "UNKNOWN";
  }
}

function SweepStatusBadge({ status }: { status: SweepStatus }) {
  return (
    <span
      style={{
        fontFamily: "var(--font-mono)",
        fontSize: "0.7rem",
        padding: "3px 10px",
        borderRadius: 4,
        color: SWEEP_STATUS_COLOR[status],
        background: "var(--bg-overlay)",
        border: `1px solid ${SWEEP_STATUS_COLOR[status]}`,
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
        }}
      >
        {children}
      </div>
    </div>
  );
}

export function SweepRunDetailPage() {
  const { id: idParam } = useParams<{ id: string }>();
  const id = Number(idParam);
  const [sweep, setSweep] = useState<SweepRun | null>(null);
  const [cells, setCells] = useState<SweepCellRow[] | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [cancelError, setCancelError] = useState<string | null>(null);

  const load = useCallback(async () => {
    if (!Number.isFinite(id)) {
      setError("Invalid sweep id");
      setLoading(false);
      return;
    }
    try {
      const [sweepRes, cellsRes] = await Promise.all([
        sweepRunsApi.get(id),
        sweepRunsApi.listCells(id),
      ]);
      setSweep(sweepRes);
      setCells(cellsRes);
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to load sweep");
    } finally {
      setLoading(false);
    }
  }, [id]);

  useEffect(() => {
    void load();
  }, [load]);

  /* Poll while the sweep is still active so the progress table updates
     without the user refreshing. */
  useEffect(() => {
    if (!sweep) return;
    if (TERMINAL_SWEEP_STATUSES.has(sweep.status)) return;
    const h = setInterval(() => void load(), 10000);
    return () => clearInterval(h);
  }, [sweep, load]);

  const axisNames = useMemo(
    () => (sweep ? Object.keys(sweep.matrix) : []),
    [sweep],
  );

  const handleCancel = async () => {
    if (!sweep) return;
    try {
      setCancelError(null);
      await sweepRunsApi.cancel(sweep.id);
      void load();
    } catch (e) {
      setCancelError(e instanceof Error ? e.message : "Failed to cancel sweep");
    }
  };

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

  if (error || !sweep) {
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
          Failed to load sweep
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

  const isActive = !TERMINAL_SWEEP_STATUSES.has(sweep.status);

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
            to="/sweep_runs"
            style={{
              display: "flex",
              padding: 6,
              borderRadius: 6,
              color: "var(--text-muted)",
              border: "1px solid var(--border-subtle)",
            }}
            title="Back to sweeps"
          >
            <ArrowLeft size={16} />
          </Link>
          <div>
            <h1 style={{ fontSize: "1.3rem" }}>{sweep.name}</h1>
            <span
              style={{
                fontFamily: "var(--font-mono)",
                fontSize: "0.75rem",
                color: "var(--text-muted)",
              }}
            >
              sweep #{sweep.id} · {sweep.cell_count} cells
            </span>
          </div>
        </div>
        <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
          <SweepStatusBadge status={sweep.status} />
          {isActive && (
            <button
              className="btn btn-danger"
              onClick={handleCancel}
              data-testid="sweep-cancel-button"
            >
              <XCircle size={14} />
              Cancel
            </button>
          )}
        </div>
      </motion.div>

      <ErrorBanner error={cancelError} />
      {sweep.error && (
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
          {sweep.error}
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
            gridTemplateColumns: "repeat(auto-fit, minmax(140px, 1fr))",
            gap: "var(--sp-3)",
          }}
        >
          <MetaCell label="fail_fast">
            {sweep.fail_fast ? "true" : "false"}
          </MetaCell>
          <MetaCell label="max_parallel">{sweep.max_parallel}</MetaCell>
          <MetaCell label="cells_pending">{sweep.cells_pending}</MetaCell>
          <MetaCell label="cells_running">{sweep.cells_running}</MetaCell>
          <MetaCell label="cells_completed">{sweep.cells_completed}</MetaCell>
          <MetaCell label="cells_failed">{sweep.cells_failed}</MetaCell>
          <MetaCell label="cells_cancelled">{sweep.cells_cancelled}</MetaCell>
          <MetaCell label="source">{sweep.submission_source}</MetaCell>
        </div>

        <div style={{ marginTop: "var(--sp-4)" }}>
          <div
            style={{
              fontSize: "0.6rem",
              textTransform: "uppercase",
              letterSpacing: "0.1em",
              color: "var(--text-muted)",
              marginBottom: 6,
            }}
          >
            Matrix
          </div>
          <pre
            style={{
              margin: 0,
              padding: "var(--sp-3)",
              background: "var(--bg-base)",
              border: "1px solid var(--border-ghost)",
              borderRadius: "var(--radius-md)",
              fontFamily: "var(--font-mono)",
              fontSize: "0.75rem",
              color: "var(--text-secondary)",
              overflow: "auto",
              maxHeight: 160,
            }}
          >
            {JSON.stringify(sweep.matrix, null, 2)}
          </pre>
        </div>
      </motion.div>

      {/* Cells table */}
      <motion.div
        initial={{ opacity: 0 }}
        animate={{ opacity: 1 }}
        transition={{ delay: 0.1 }}
        className="panel"
        style={{ overflow: "hidden" }}
      >
        <div className="panel-header">
          <h3 style={{ textTransform: "none", letterSpacing: 0 }}>
            Cells ({cells?.length ?? 0})
          </h3>
        </div>
        <div style={{ overflow: "auto", maxHeight: "50vh" }}>
          <table className="data-table">
            <thead>
              <tr>
                <th style={{ width: 60 }}>#</th>
                {axisNames.map((axis) => (
                  <th key={axis}>{axis}</th>
                ))}
                <th style={{ width: 120 }}>Status</th>
                <th style={{ width: 180 }}>Started</th>
                <th style={{ width: 180 }}>Completed</th>
                <th>Error</th>
                <th style={{ width: 60 }}>Run</th>
              </tr>
            </thead>
            <tbody>
              {cells && cells.length > 0 ? (
                cells.map((cell, i) => {
                  const a = cell.args ?? {};
                  return (
                    <tr key={cell.id} data-testid={`sweep-cell-row-${cell.id}`}>
                      <td className="col-mono col-muted">{i + 1}</td>
                      {axisNames.map((axis) => (
                        <td key={axis} className="col-mono">
                          {formatAxisValue(a[axis])}
                        </td>
                      ))}
                      <td>
                        <StatusBadge
                          status={workflowRunStatusToBadge(cell.status)}
                          size="sm"
                        />
                      </td>
                      <td className="col-mono col-muted">
                        {formatTs(cell.started_at)}
                      </td>
                      <td className="col-mono col-muted">
                        {formatTs(cell.completed_at)}
                      </td>
                      <td
                        className="col-mono truncate"
                        style={{
                          maxWidth: 240,
                          color: cell.error
                            ? "var(--st-failed)"
                            : "var(--text-muted)",
                        }}
                        title={cell.error ?? undefined}
                      >
                        {cell.error ?? "—"}
                      </td>
                      <td>
                        <Link
                          to={`/workflow_runs/${cell.id}`}
                          className="btn btn-ghost"
                          style={{ padding: "4px 8px" }}
                          title="Open workflow run"
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
                    colSpan={axisNames.length + 6}
                    style={{
                      textAlign: "center",
                      color: "var(--text-muted)",
                      padding: 32,
                    }}
                  >
                    No cells materialized for this sweep.
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

function formatAxisValue(v: unknown): string {
  if (v === null || v === undefined) return "—";
  if (typeof v === "string") return v;
  try {
    return JSON.stringify(v);
  } catch {
    return String(v);
  }
}
