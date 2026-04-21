import { useMemo, useState } from "react";
import { motion } from "framer-motion";
import { Link } from "react-router-dom";
import { Grid3x3, XCircle, Eye } from "lucide-react";
import { useApi } from "../hooks/use-api.ts";
import { sweepRuns as sweepRunsApi } from "../lib/api.ts";
import type { SweepRun, SweepStatus } from "../lib/types.ts";
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

function SweepStatusBadge({ status }: { status: SweepStatus }) {
  return (
    <span
      style={{
        fontFamily: "var(--font-mono)",
        fontSize: "0.65rem",
        padding: "2px 8px",
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

function formatStarted(ts: string): string {
  try {
    return new Date(ts).toLocaleString();
  } catch {
    return ts;
  }
}

function ProgressBar({
  completed,
  total,
}: {
  completed: number;
  total: number;
}) {
  const pct = total > 0 ? Math.min(100, (completed / total) * 100) : 0;
  return (
    <div
      style={{
        display: "flex",
        alignItems: "center",
        gap: 8,
        fontFamily: "var(--font-mono)",
        fontSize: "0.72rem",
      }}
    >
      <div
        style={{
          width: 100,
          height: 6,
          background: "var(--bg-overlay)",
          borderRadius: 3,
          overflow: "hidden",
        }}
      >
        <div
          style={{
            width: `${pct}%`,
            height: "100%",
            background: "var(--accent)",
            transition: "width var(--duration-normal) var(--ease-out)",
          }}
        />
      </div>
      <span style={{ color: "var(--text-muted)" }}>
        {completed}/{total}
      </span>
    </div>
  );
}

export function SweepRunsPage() {
  const {
    data: sweeps,
    error,
    refetch,
  } = useApi(() => sweepRunsApi.list(), [], { pollInterval: 10000 });
  const [cancelError, setCancelError] = useState<string | null>(null);

  const rows = useMemo<SweepRun[]>(() => sweeps ?? [], [sweeps]);

  const handleCancel = async (id: number) => {
    try {
      setCancelError(null);
      await sweepRunsApi.cancel(id);
      refetch();
    } catch (e) {
      setCancelError(e instanceof Error ? e.message : "Failed to cancel sweep");
    }
  };

  return (
    <div
      style={{ display: "flex", flexDirection: "column", gap: "var(--sp-6)" }}
    >
      <motion.div
        initial={{ opacity: 0, y: 12 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ duration: 0.4, ease: [0.16, 1, 0.3, 1] }}
      >
        <h1 style={{ marginBottom: 4 }}>Sweeps</h1>
        <p style={{ color: "var(--text-muted)", fontSize: "0.85rem" }}>
          Parameter-matrix workflow runs. Each sweep materializes N
          ``workflow_runs`` rows executed with ``max_parallel`` concurrency.
        </p>
      </motion.div>

      <ErrorBanner error={cancelError ?? error} />

      <motion.div
        initial={{ opacity: 0 }}
        animate={{ opacity: 1 }}
        transition={{ delay: 0.1, duration: 0.4 }}
        className="panel"
        style={{ overflow: "hidden" }}
      >
        <div style={{ overflow: "auto", maxHeight: "calc(100dvh - 200px)" }}>
          <table className="data-table">
            <thead>
              <tr>
                <th>Name</th>
                <th style={{ width: 120 }}>Status</th>
                <th style={{ width: 180 }}>Progress</th>
                <th style={{ width: 80 }}>Failed</th>
                <th style={{ width: 100 }}>Cancelled</th>
                <th style={{ width: 180 }}>Started</th>
                <th style={{ width: 120 }}>Actions</th>
              </tr>
            </thead>
            <tbody>
              {rows.length > 0 ? (
                rows.map((sweep, i) => {
                  const isActive = !TERMINAL_SWEEP_STATUSES.has(sweep.status);
                  return (
                    <motion.tr
                      key={sweep.id}
                      initial={{ opacity: 0, x: -8 }}
                      animate={{ opacity: 1, x: 0 }}
                      transition={{ delay: i * 0.02, duration: 0.25 }}
                      data-testid={`sweep-row-${sweep.id}`}
                    >
                      <td style={{ fontWeight: 500 }}>{sweep.name}</td>
                      <td>
                        <SweepStatusBadge status={sweep.status} />
                      </td>
                      <td>
                        <ProgressBar
                          completed={sweep.cells_completed}
                          total={sweep.cell_count}
                        />
                      </td>
                      <td
                        className="col-mono"
                        style={{
                          color:
                            sweep.cells_failed > 0
                              ? "var(--st-failed)"
                              : "var(--text-muted)",
                        }}
                      >
                        {sweep.cells_failed}
                      </td>
                      <td className="col-mono col-muted">
                        {sweep.cells_cancelled}
                      </td>
                      <td className="col-mono col-muted">
                        {formatStarted(sweep.started_at)}
                      </td>
                      <td>
                        <div style={{ display: "flex", gap: 4 }}>
                          <Link
                            to={`/sweep_runs/${sweep.id}`}
                            className="btn btn-ghost"
                            style={{ padding: "4px 8px" }}
                            title="View Details"
                            data-testid={`sweep-view-${sweep.id}`}
                          >
                            <Eye size={13} />
                          </Link>
                          {isActive && (
                            <button
                              className="btn btn-danger"
                              style={{ padding: "4px 8px" }}
                              onClick={() => handleCancel(sweep.id)}
                              title="Cancel Sweep"
                            >
                              <XCircle size={13} />
                            </button>
                          )}
                        </div>
                      </td>
                    </motion.tr>
                  );
                })
              ) : (
                <tr>
                  <td
                    colSpan={7}
                    style={{
                      textAlign: "center",
                      color: "var(--text-muted)",
                      padding: 48,
                    }}
                  >
                    {sweeps === null ? (
                      <span
                        className="skeleton"
                        style={{
                          width: 160,
                          height: 16,
                          display: "inline-block",
                        }}
                      />
                    ) : (
                      <>
                        <Grid3x3
                          size={28}
                          style={{
                            marginBottom: 10,
                            opacity: 0.4,
                            display: "block",
                            margin: "0 auto 10px",
                          }}
                        />
                        No sweep runs yet. Start one from the Run dialog by
                        switching an arg to list mode.
                      </>
                    )}
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
