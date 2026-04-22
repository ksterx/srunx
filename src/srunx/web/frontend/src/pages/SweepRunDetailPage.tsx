import { useCallback, useEffect, useMemo, useState } from "react";
import { Link, useParams } from "react-router-dom";
import { motion } from "framer-motion";
import {
  ArrowLeft,
  ArrowUp,
  ArrowDown,
  ArrowUpDown,
  Filter,
  XCircle,
  ExternalLink,
} from "lucide-react";
import {
  sweepRuns as sweepRunsApi,
  workflows as workflowsApi,
} from "../lib/api.ts";
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

const TERMINAL_CELL_STATUSES: ReadonlySet<WorkflowRunStatus> = new Set([
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

const CELL_STATUS_OPTIONS: readonly (WorkflowRunStatus | "all")[] = [
  "all",
  "pending",
  "running",
  "completed",
  "failed",
  "cancelled",
];

type SortKey = "index" | "status" | "started" | "completed" | "duration";
type SortDir = "asc" | "desc";

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

function durationSeconds(
  startedAt: string | null,
  completedAt: string | null,
): number | null {
  if (!startedAt) return null;
  const start = Date.parse(startedAt);
  if (Number.isNaN(start)) return null;
  const end = completedAt ? Date.parse(completedAt) : Date.now();
  if (Number.isNaN(end) || end < start) return null;
  return (end - start) / 1000;
}

function formatDuration(seconds: number | null): string {
  if (seconds === null) return "—";
  if (seconds < 60) return `${seconds.toFixed(1)}s`;
  const mins = Math.floor(seconds / 60);
  const secs = Math.round(seconds - mins * 60);
  if (mins < 60) return `${mins}m ${secs}s`;
  const hours = Math.floor(mins / 60);
  const remMins = mins - hours * 60;
  return `${hours}h ${remMins}m`;
}

/** Naive linear-throughput ETA: remaining ÷ throughput.
 *
 *  ``throughput`` is computed from the elapsed wall-clock since the
 *  sweep's ``started_at`` divided by the number of cells that have
 *  already reached a terminal state. The estimate is noisy in the
 *  very first seconds (zero completions) and towards the tail (fewer
 *  cells left to average), but for a multi-minute sweep it gives
 *  operators a useful upper-bound glance. */
function computeEta(sweep: SweepRun): number | null {
  const completedTerminal =
    sweep.cells_completed + sweep.cells_failed + sweep.cells_cancelled;
  if (completedTerminal <= 0) return null;
  const remaining = sweep.cell_count - completedTerminal;
  if (remaining <= 0) return 0;
  const elapsed = durationSeconds(sweep.started_at, null);
  if (elapsed === null || elapsed <= 0) return null;
  const throughput = completedTerminal / elapsed;
  if (throughput <= 0) return null;
  return remaining / throughput;
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

function ProgressBar({ sweep }: { sweep: SweepRun }) {
  const completed = sweep.cells_completed;
  const failed = sweep.cells_failed;
  const cancelled = sweep.cells_cancelled;
  const running = sweep.cells_running;
  const total = Math.max(sweep.cell_count, 1);

  const segments: Array<{ key: string; value: number; color: string }> = [
    { key: "completed", value: completed, color: "var(--st-completed)" },
    { key: "failed", value: failed, color: "var(--st-failed)" },
    { key: "cancelled", value: cancelled, color: "var(--text-muted)" },
    { key: "running", value: running, color: "var(--st-running)" },
  ];

  const eta = computeEta(sweep);

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
      <div
        style={{
          position: "relative",
          height: 10,
          background: "var(--bg-base)",
          borderRadius: 5,
          overflow: "hidden",
          border: "1px solid var(--border-ghost)",
          display: "flex",
        }}
        data-testid="sweep-progress-bar"
      >
        {segments.map((s) => (
          <div
            key={s.key}
            style={{
              width: `${(s.value / total) * 100}%`,
              background: s.color,
              transition: "width 0.3s ease",
            }}
            title={`${s.key}: ${s.value}`}
          />
        ))}
      </div>
      <div
        style={{
          display: "flex",
          gap: 16,
          fontSize: "0.72rem",
          fontFamily: "var(--font-mono)",
          color: "var(--text-muted)",
        }}
      >
        <span>
          {completed + failed + cancelled}/{sweep.cell_count} terminal
        </span>
        {eta !== null && !TERMINAL_SWEEP_STATUSES.has(sweep.status) && (
          <span data-testid="sweep-eta">ETA ≈ {formatDuration(eta)}</span>
        )}
        {sweep.status === "draining" && (
          <span
            style={{
              display: "inline-flex",
              alignItems: "center",
              gap: 4,
              color: "var(--st-pending)",
            }}
            data-testid="sweep-draining-indicator"
          >
            <motion.span
              animate={{ rotate: 360 }}
              transition={{ repeat: Infinity, duration: 1.2, ease: "linear" }}
              style={{ display: "inline-block", width: 10, height: 10 }}
            >
              ◐
            </motion.span>
            draining pending cells
          </span>
        )}
      </div>
    </div>
  );
}

function SortHeader({
  label,
  sortKey,
  activeKey,
  direction,
  onToggle,
  width,
}: {
  label: string;
  sortKey: SortKey;
  activeKey: SortKey;
  direction: SortDir;
  onToggle: (key: SortKey) => void;
  width?: number;
}) {
  const isActive = activeKey === sortKey;
  const Icon = isActive
    ? direction === "asc"
      ? ArrowUp
      : ArrowDown
    : ArrowUpDown;
  return (
    <th style={width ? { width } : undefined}>
      <button
        type="button"
        onClick={() => onToggle(sortKey)}
        data-testid={`sort-${sortKey}`}
        style={{
          display: "inline-flex",
          alignItems: "center",
          gap: 4,
          background: "transparent",
          border: "none",
          color: isActive ? "var(--text-primary)" : "inherit",
          font: "inherit",
          cursor: "pointer",
          padding: 0,
          textTransform: "inherit",
          letterSpacing: "inherit",
        }}
      >
        {label}
        <Icon size={12} style={{ opacity: isActive ? 1 : 0.4 }} />
      </button>
    </th>
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
  const [pendingCellCancels, setPendingCellCancels] = useState<Set<number>>(
    new Set(),
  );
  const [statusFilter, setStatusFilter] = useState<WorkflowRunStatus | "all">(
    "all",
  );
  const [sortKey, setSortKey] = useState<SortKey>("index");
  const [sortDir, setSortDir] = useState<SortDir>("asc");

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

  const toggleSort = useCallback((key: SortKey) => {
    setSortKey((prev) => {
      if (prev === key) {
        setSortDir((d) => (d === "asc" ? "desc" : "asc"));
        return key;
      }
      setSortDir("asc");
      return key;
    });
  }, []);

  const visibleCells = useMemo(() => {
    if (!cells) return [];
    const filtered =
      statusFilter === "all"
        ? cells
        : cells.filter((c) => c.status === statusFilter);

    const indexed = filtered.map((cell) => {
      // Stable "insertion" index anchored on the original cells array so
      // the leftmost column stays stable regardless of sort direction.
      const originalIndex = cells.indexOf(cell);
      return { cell, originalIndex };
    });

    const mul = sortDir === "asc" ? 1 : -1;
    indexed.sort((a, b) => {
      const A = a.cell;
      const B = b.cell;
      switch (sortKey) {
        case "status":
          return mul * A.status.localeCompare(B.status);
        case "started": {
          const aT = A.started_at ? Date.parse(A.started_at) : 0;
          const bT = B.started_at ? Date.parse(B.started_at) : 0;
          return mul * (aT - bT);
        }
        case "completed": {
          const aT = A.completed_at ? Date.parse(A.completed_at) : 0;
          const bT = B.completed_at ? Date.parse(B.completed_at) : 0;
          return mul * (aT - bT);
        }
        case "duration": {
          const aD = durationSeconds(A.started_at, A.completed_at) ?? -1;
          const bD = durationSeconds(B.started_at, B.completed_at) ?? -1;
          return mul * (aD - bD);
        }
        case "index":
        default:
          return mul * (a.originalIndex - b.originalIndex);
      }
    });

    return indexed;
  }, [cells, statusFilter, sortKey, sortDir]);

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

  const handleCancelCell = useCallback(
    async (cellId: number) => {
      try {
        setCancelError(null);
        setPendingCellCancels((s) => new Set(s).add(cellId));
        await workflowsApi.cancelRun(String(cellId));
        void load();
      } catch (e) {
        setCancelError(
          e instanceof Error ? e.message : `Failed to cancel cell ${cellId}`,
        );
      } finally {
        setPendingCellCancels((s) => {
          const next = new Set(s);
          next.delete(cellId);
          return next;
        });
      }
    },
    [load],
  );

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
        <div style={{ marginBottom: "var(--sp-4)" }}>
          <ProgressBar sweep={sweep} />
        </div>

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
        <div
          className="panel-header"
          style={{
            display: "flex",
            alignItems: "center",
            justifyContent: "space-between",
            gap: 12,
            flexWrap: "wrap",
          }}
        >
          <h3 style={{ textTransform: "none", letterSpacing: 0 }}>
            Cells ({visibleCells.length}
            {cells && visibleCells.length !== cells.length
              ? ` of ${cells.length}`
              : ""}
            )
          </h3>
          <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
            <Filter size={14} color="var(--text-muted)" />
            <select
              className="select"
              value={statusFilter}
              onChange={(e) =>
                setStatusFilter(e.target.value as WorkflowRunStatus | "all")
              }
              data-testid="sweep-cell-status-filter"
            >
              {CELL_STATUS_OPTIONS.map((s) => (
                <option key={s} value={s}>
                  {s === "all" ? "All statuses" : s}
                </option>
              ))}
            </select>
          </div>
        </div>
        <div style={{ overflow: "auto", maxHeight: "50vh" }}>
          <table className="data-table">
            <thead>
              <tr>
                <SortHeader
                  label="#"
                  sortKey="index"
                  activeKey={sortKey}
                  direction={sortDir}
                  onToggle={toggleSort}
                  width={60}
                />
                {axisNames.map((axis) => (
                  <th key={axis}>{axis}</th>
                ))}
                <SortHeader
                  label="Status"
                  sortKey="status"
                  activeKey={sortKey}
                  direction={sortDir}
                  onToggle={toggleSort}
                  width={120}
                />
                <SortHeader
                  label="Started"
                  sortKey="started"
                  activeKey={sortKey}
                  direction={sortDir}
                  onToggle={toggleSort}
                  width={180}
                />
                <SortHeader
                  label="Completed"
                  sortKey="completed"
                  activeKey={sortKey}
                  direction={sortDir}
                  onToggle={toggleSort}
                  width={180}
                />
                <SortHeader
                  label="Duration"
                  sortKey="duration"
                  activeKey={sortKey}
                  direction={sortDir}
                  onToggle={toggleSort}
                  width={100}
                />
                <th>Error</th>
                <th style={{ width: 110 }}>Actions</th>
              </tr>
            </thead>
            <tbody>
              {visibleCells.length > 0 ? (
                visibleCells.map(({ cell, originalIndex }) => {
                  const a = cell.args ?? {};
                  const duration = durationSeconds(
                    cell.started_at,
                    cell.completed_at,
                  );
                  const cellIsActive = !TERMINAL_CELL_STATUSES.has(cell.status);
                  const isCancelling = pendingCellCancels.has(cell.id);
                  return (
                    <tr key={cell.id} data-testid={`sweep-cell-row-${cell.id}`}>
                      <td className="col-mono col-muted">
                        {originalIndex + 1}
                      </td>
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
                      <td className="col-mono col-muted">
                        {formatDuration(duration)}
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
                        <div style={{ display: "flex", gap: 4 }}>
                          {cellIsActive && (
                            <button
                              type="button"
                              className="btn btn-ghost"
                              style={{
                                padding: "4px 8px",
                                color: "var(--st-failed)",
                              }}
                              disabled={isCancelling}
                              onClick={() => void handleCancelCell(cell.id)}
                              data-testid={`sweep-cell-cancel-${cell.id}`}
                              title="Cancel this cell"
                            >
                              <XCircle size={13} />
                            </button>
                          )}
                          <Link
                            to={`/workflow_runs/${cell.id}`}
                            className="btn btn-ghost"
                            style={{ padding: "4px 8px" }}
                            title="Open workflow run"
                          >
                            <ExternalLink size={13} />
                          </Link>
                        </div>
                      </td>
                    </tr>
                  );
                })
              ) : (
                <tr>
                  <td
                    colSpan={axisNames.length + 7}
                    style={{
                      textAlign: "center",
                      color: "var(--text-muted)",
                      padding: 32,
                    }}
                  >
                    {cells && cells.length > 0
                      ? `No cells match the "${statusFilter}" filter.`
                      : "No cells materialized for this sweep."}
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
