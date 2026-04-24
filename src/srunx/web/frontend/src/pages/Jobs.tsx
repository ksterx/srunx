import { useEffect, useMemo, useState } from "react";
import { motion } from "framer-motion";
import { Link, useNavigate } from "react-router-dom";
import {
  Bell,
  BellRing,
  FileText,
  Filter,
  Search,
  XCircle,
} from "lucide-react";
import { useApi } from "../hooks/use-api.ts";
import {
  config as configApi,
  endpoints as endpointsApi,
  jobs as jobsApi,
  watches as watchesApi,
} from "../lib/api.ts";
import type { JobStatus, NotificationPreset } from "../lib/types.ts";
import { StatusBadge } from "../components/StatusBadge.tsx";
import { ErrorBanner } from "../components/ErrorBanner.tsx";

const ALL_STATUSES: JobStatus[] = [
  "PENDING",
  "RUNNING",
  "COMPLETED",
  "FAILED",
  "CANCELLED",
  "TIMEOUT",
];

// Mirror of ``SLURM_TERMINAL_JOB_STATES`` in the backend — jobs in these
// states are split into a "finished" bucket below the active ones.
const TERMINAL_STATUSES: ReadonlySet<JobStatus> = new Set([
  "COMPLETED",
  "FAILED",
  "CANCELLED",
  "TIMEOUT",
]);

type Toast =
  | { kind: "success"; message: string }
  | {
      kind: "warning";
      message: string;
      action?: { label: string; onClick: () => void };
    }
  | { kind: "error"; message: string };

// Extract every job id that currently has at least one open ``kind=job``
// watch. The ``target_ref`` is ``job:local:<id>`` or ``job:ssh:<profile>:<id>``
// — both end with ``:<id>``, so stripping the last colon segment gives us
// the job id regardless of transport.
function jobIdFromTargetRef(targetRef: string): number | null {
  const tail = targetRef.split(":").pop();
  if (!tail) return null;
  const n = Number(tail);
  return Number.isFinite(n) ? n : null;
}

export function Jobs() {
  const navigate = useNavigate();
  const [search, setSearch] = useState("");
  const [statusFilter, setStatusFilter] = useState<JobStatus | "ALL">("ALL");
  const [actionError, setActionError] = useState<string | null>(null);
  const [toast, setToast] = useState<Toast | null>(null);
  const [bellPending, setBellPending] = useState<Set<number>>(new Set());

  const {
    data: jobList,
    error,
    refetch,
  } = useApi(() => jobsApi.list(), [], {
    pollInterval: 10000,
  });

  const { data: openWatches, refetch: refetchWatches } = useApi(
    () => watchesApi.list({ kind: "job", open: true }),
    [],
    { pollInterval: 10000 },
  );

  const { data: srunxConfig } = useApi(() => configApi.get(), []);

  const jobsWithWatches = useMemo(() => {
    const set = new Set<number>();
    for (const w of openWatches ?? []) {
      const id = jobIdFromTargetRef(w.target_ref);
      if (id !== null) set.add(id);
    }
    return set;
  }, [openWatches]);

  const filtered = (jobList ?? [])
    .filter((job) => {
      if (statusFilter !== "ALL" && job.status !== statusFilter) return false;
      if (search && !job.name.toLowerCase().includes(search.toLowerCase()))
        return false;
      return true;
    })
    // Two-tier sort: active jobs first (newest first), then terminal jobs
    // (newest first). Within each bucket, descending by ``job_id`` so the
    // latest submission is on top. Rows without a ``job_id`` fall to the
    // bottom. "Terminal" matches backend ``SLURM_TERMINAL_JOB_STATES``.
    .sort((a, b) => {
      const aTerminal = TERMINAL_STATUSES.has(a.status);
      const bTerminal = TERMINAL_STATUSES.has(b.status);
      if (aTerminal !== bTerminal) return aTerminal ? 1 : -1;
      return (b.job_id ?? -Infinity) - (a.job_id ?? -Infinity);
    });

  // Auto-dismiss success/warning toasts after 4s. Errors stick until cleared.
  useEffect(() => {
    if (!toast || toast.kind === "error") return;
    const timer = setTimeout(() => setToast(null), 4000);
    return () => clearTimeout(timer);
  }, [toast]);

  const handleCancel = async (jobId: number) => {
    try {
      setActionError(null);
      await jobsApi.cancel(jobId);
      refetch();
    } catch (err) {
      setActionError(
        err instanceof Error ? err.message : "Failed to cancel job",
      );
    }
  };

  const setBellPendingFor = (jobId: number, pending: boolean) => {
    setBellPending((prev) => {
      const next = new Set(prev);
      if (pending) next.add(jobId);
      else next.delete(jobId);
      return next;
    });
  };

  const handleToggleBell = async (jobId: number) => {
    if (bellPending.has(jobId)) return;
    setActionError(null);
    setBellPendingFor(jobId, true);
    try {
      if (jobsWithWatches.has(jobId)) {
        // OFF: close every open watch for this job across transports.
        const matching = (openWatches ?? []).filter(
          (w) => jobIdFromTargetRef(w.target_ref) === jobId,
        );
        await Promise.all(matching.map((w) => watchesApi.close(w.id)));
        setToast({
          kind: "success",
          message: `Notifications disabled for job ${jobId}.`,
        });
      } else {
        // ON: use the configured default endpoint + preset.
        const defaults = srunxConfig?.notifications;
        const endpointName = defaults?.default_endpoint_name ?? null;
        const preset = (defaults?.default_preset ??
          "terminal") as NotificationPreset;
        if (!endpointName) {
          setToast({
            kind: "warning",
            message:
              "No default notification endpoint is configured. Set one in Settings → Notifications.",
            action: {
              label: "Configure now",
              onClick: () => navigate("/settings"),
            },
          });
          return;
        }
        // Resolve endpoint name → id via the endpoints list.
        const eps = await endpointsApi.list({ include_disabled: false });
        const endpoint = eps.find((e) => e.name === endpointName);
        if (!endpoint) {
          setToast({
            kind: "warning",
            message: `Default endpoint "${endpointName}" not found or disabled. Update Settings → Notifications.`,
            action: {
              label: "Configure now",
              onClick: () => navigate("/settings"),
            },
          });
          return;
        }
        await watchesApi.createForJob({
          job_id: jobId,
          endpoint_id: endpoint.id,
          preset,
        });
        setToast({
          kind: "success",
          message: `Notifications enabled for job ${jobId} (${endpointName}, ${preset}).`,
        });
      }
      await refetchWatches();
    } catch (err) {
      setActionError(
        err instanceof Error ? err.message : "Failed to toggle notifications",
      );
    } finally {
      setBellPendingFor(jobId, false);
    }
  };

  const openJobDetail = (jobId: number) => {
    navigate(`/jobs/${jobId}`);
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
        <h1 style={{ marginBottom: 4 }}>Jobs</h1>
        <p style={{ color: "var(--text-muted)", fontSize: "0.85rem" }}>
          All SLURM jobs submitted through srunx
        </p>
      </motion.div>

      {/* Toolbar */}
      <motion.div
        initial={{ opacity: 0, y: 8 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ delay: 0.05, duration: 0.35 }}
        style={{
          display: "flex",
          alignItems: "center",
          gap: "var(--sp-3)",
        }}
      >
        {/* Search */}
        <div style={{ position: "relative", flex: 1, maxWidth: 360 }}>
          <Search
            size={14}
            style={{
              position: "absolute",
              left: 10,
              top: "50%",
              transform: "translateY(-50%)",
              color: "var(--text-muted)",
            }}
          />
          <input
            className="input"
            placeholder="Search jobs..."
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            style={{ paddingLeft: 32, width: "100%" }}
          />
        </div>

        {/* Status filter */}
        <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
          <Filter size={14} color="var(--text-muted)" />
          <select
            className="select"
            value={statusFilter}
            onChange={(e) =>
              setStatusFilter(e.target.value as JobStatus | "ALL")
            }
          >
            <option value="ALL">All Status</option>
            {ALL_STATUSES.map((s) => (
              <option key={s} value={s}>
                {s}
              </option>
            ))}
          </select>
        </div>

        {/* Count */}
        <span
          style={{
            fontFamily: "var(--font-mono)",
            fontSize: "0.75rem",
            color: "var(--text-muted)",
            marginLeft: "auto",
          }}
        >
          {filtered.length} jobs
        </span>
      </motion.div>

      {/* Error + toast banners */}
      <ErrorBanner error={actionError ?? error} />
      {toast && <ToastBanner toast={toast} onDismiss={() => setToast(null)} />}

      {/* Table */}
      <motion.div
        initial={{ opacity: 0 }}
        animate={{ opacity: 1 }}
        transition={{ delay: 0.1, duration: 0.4 }}
        className="panel"
        style={{ overflow: "hidden" }}
      >
        <div style={{ overflow: "auto", maxHeight: "calc(100dvh - 240px)" }}>
          <table className="data-table">
            <thead>
              <tr>
                <th style={{ width: 80 }}>Job ID</th>
                <th>Name</th>
                <th style={{ width: 100 }}>User</th>
                <th style={{ width: 120 }}>Status</th>
                <th>Command</th>
                <th style={{ width: 90 }}>Partition</th>
                <th style={{ width: 60 }}>Nodes</th>
                <th style={{ width: 60 }}>GPUs</th>
                <th style={{ width: 80 }}>Elapsed</th>
                <th style={{ width: 80 }}>Limit</th>
                <th style={{ width: 90 }}>Actions</th>
              </tr>
            </thead>
            <tbody>
              {filtered.length > 0 ? (
                filtered.map((job, i) => {
                  const hasWatch = job.job_id
                    ? jobsWithWatches.has(job.job_id)
                    : false;
                  const bellBusy = job.job_id
                    ? bellPending.has(job.job_id)
                    : false;
                  // Separator: the first terminal row that directly follows
                  // an active one gets a thicker top border so the two
                  // buckets visually split apart.
                  const isBucketBreak =
                    i > 0 &&
                    TERMINAL_STATUSES.has(job.status) &&
                    !TERMINAL_STATUSES.has(filtered[i - 1].status);
                  return (
                    <motion.tr
                      key={job.job_id ?? job.name}
                      initial={{ opacity: 0, x: -8 }}
                      animate={{ opacity: 1, x: 0 }}
                      transition={{ delay: i * 0.02, duration: 0.25 }}
                      onClick={
                        job.job_id
                          ? () => openJobDetail(job.job_id!)
                          : undefined
                      }
                      style={{
                        cursor: job.job_id ? "pointer" : "default",
                        // Separator between active and terminal buckets:
                        // double-line via two box-shadow insets keeps the
                        // data-table's own borders untouched (borderTop
                        // would fight the sibling cell borders).
                        boxShadow: isBucketBreak
                          ? "inset 0 2px 0 0 var(--border-strong), inset 0 14px 0 -12px var(--bg-base)"
                          : undefined,
                      }}
                    >
                      <td className="col-mono">{job.job_id ?? "—"}</td>
                      <td style={{ fontWeight: 500 }}>{job.name}</td>
                      <td className="col-mono col-muted">{job.user ?? "—"}</td>
                      <td>
                        <StatusBadge status={job.status} size="sm" />
                      </td>
                      <td
                        className="col-mono col-muted truncate"
                        style={{ maxWidth: 240 }}
                      >
                        {job.command?.join(" ") ?? "—"}
                      </td>
                      <td className="col-muted">
                        {job.resources.partition ?? "—"}
                      </td>
                      <td className="col-mono">{job.resources.nodes ?? 1}</td>
                      <td className="col-mono">
                        {job.resources.gpus_per_node ?? 0}
                      </td>
                      <td className="col-mono">{job.elapsed_time ?? "—"}</td>
                      <td className="col-mono col-muted">
                        {job.resources.time_limit ?? "—"}
                      </td>
                      <td onClick={(e) => e.stopPropagation()}>
                        <div style={{ display: "flex", gap: 4 }}>
                          {job.job_id && job.status !== "PENDING" && (
                            <Link
                              to={`/jobs/${job.job_id}/logs`}
                              className="btn btn-ghost"
                              style={{ padding: "4px 8px" }}
                              title="View logs"
                            >
                              <FileText size={13} />
                            </Link>
                          )}
                          {job.job_id && !TERMINAL_STATUSES.has(job.status) && (
                            <button
                              className="btn btn-ghost"
                              style={{
                                padding: "4px 8px",
                                color: hasWatch
                                  ? "var(--st-completed)"
                                  : undefined,
                              }}
                              onClick={() => handleToggleBell(job.job_id!)}
                              disabled={bellBusy}
                              title={
                                hasWatch
                                  ? "Disable notifications"
                                  : "Enable notifications"
                              }
                              aria-pressed={hasWatch}
                            >
                              {hasWatch ? (
                                <BellRing size={13} />
                              ) : (
                                <Bell size={13} />
                              )}
                            </button>
                          )}
                          {job.job_id &&
                            (job.status === "RUNNING" ||
                              job.status === "PENDING") && (
                              <button
                                className="btn btn-danger"
                                style={{ padding: "4px 8px" }}
                                onClick={() => handleCancel(job.job_id!)}
                                title="Cancel Job"
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
                    colSpan={11}
                    style={{
                      textAlign: "center",
                      color: "var(--text-muted)",
                      padding: 48,
                    }}
                  >
                    {jobList === null ? (
                      <span
                        className="skeleton"
                        style={{
                          width: 160,
                          height: 16,
                          display: "inline-block",
                        }}
                      />
                    ) : (
                      "No jobs match the current filters"
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

type ToastBannerProps = {
  toast: Toast;
  onDismiss: () => void;
};

function ToastBanner({ toast, onDismiss }: ToastBannerProps) {
  const palette =
    toast.kind === "success"
      ? {
          bg: "var(--st-completed-dim)",
          border: "rgba(34,197,94,0.3)",
          fg: "var(--st-completed)",
        }
      : toast.kind === "warning"
        ? {
            bg: "rgba(234,179,8,0.1)",
            border: "rgba(234,179,8,0.3)",
            fg: "#eab308",
          }
        : {
            bg: "var(--st-failed-dim)",
            border: "rgba(244,63,94,0.3)",
            fg: "var(--st-failed)",
          };

  const hasAction =
    toast.kind === "warning" && "action" in toast && toast.action;

  return (
    <motion.div
      initial={{ opacity: 0, y: -6 }}
      animate={{ opacity: 1, y: 0 }}
      exit={{ opacity: 0, y: -6 }}
      style={{
        padding: "var(--sp-2) var(--sp-3)",
        background: palette.bg,
        border: `1px solid ${palette.border}`,
        borderRadius: "var(--radius-md)",
        color: palette.fg,
        fontSize: "0.82rem",
        display: "flex",
        alignItems: "center",
        gap: "var(--sp-3)",
      }}
    >
      <span style={{ flex: 1 }}>{toast.message}</span>
      {hasAction && toast.kind === "warning" && toast.action && (
        <button
          type="button"
          className="btn btn-ghost"
          onClick={() => {
            toast.action!.onClick();
            onDismiss();
          }}
          style={{
            padding: "4px 10px",
            fontSize: "0.78rem",
            color: palette.fg,
          }}
        >
          {toast.action.label}
        </button>
      )}
      <button
        type="button"
        onClick={onDismiss}
        aria-label="Dismiss"
        style={{
          background: "transparent",
          border: "none",
          color: palette.fg,
          cursor: "pointer",
          padding: 2,
          fontFamily: "var(--font-mono)",
        }}
      >
        ×
      </button>
    </motion.div>
  );
}
