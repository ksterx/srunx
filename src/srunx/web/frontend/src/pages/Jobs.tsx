import { useState } from "react";
import { motion } from "framer-motion";
import { Link } from "react-router-dom";
import { Search, Filter, XCircle, FileText } from "lucide-react";
import { useApi } from "../hooks/use-api.ts";
import { jobs as jobsApi } from "../lib/api.ts";
import type { JobStatus } from "../lib/types.ts";
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

export function Jobs() {
  const [search, setSearch] = useState("");
  const [statusFilter, setStatusFilter] = useState<JobStatus | "ALL">("ALL");
  const [cancelError, setCancelError] = useState<string | null>(null);
  const {
    data: jobList,
    error,
    refetch,
  } = useApi(() => jobsApi.list(), [], {
    pollInterval: 10000,
  });

  const filtered = (jobList ?? []).filter((job) => {
    if (statusFilter !== "ALL" && job.status !== statusFilter) return false;
    if (search && !job.name.toLowerCase().includes(search.toLowerCase()))
      return false;
    return true;
  });

  const handleCancel = async (jobId: number) => {
    try {
      setCancelError(null);
      await jobsApi.cancel(jobId);
      refetch();
    } catch (err) {
      setCancelError(
        err instanceof Error ? err.message : "Failed to cancel job",
      );
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

      {/* Error banners */}
      <ErrorBanner error={cancelError ?? error} />

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
                <th style={{ width: 120 }}>Status</th>
                <th>Command</th>
                <th style={{ width: 100 }}>Partition</th>
                <th style={{ width: 60 }}>Nodes</th>
                <th style={{ width: 60 }}>GPUs</th>
                <th style={{ width: 90 }}>Time</th>
                <th style={{ width: 100 }}>Actions</th>
              </tr>
            </thead>
            <tbody>
              {filtered.length > 0 ? (
                filtered.map((job, i) => (
                  <motion.tr
                    key={job.job_id ?? job.name}
                    initial={{ opacity: 0, x: -8 }}
                    animate={{ opacity: 1, x: 0 }}
                    transition={{ delay: i * 0.02, duration: 0.25 }}
                  >
                    <td className="col-mono">{job.job_id ?? "—"}</td>
                    <td style={{ fontWeight: 500 }}>{job.name}</td>
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
                    <td className="col-mono col-muted">
                      {job.resources.time_limit ?? "—"}
                    </td>
                    <td>
                      <div style={{ display: "flex", gap: 4 }}>
                        {job.job_id && (
                          <Link
                            to={`/jobs/${job.job_id}/logs`}
                            className="btn btn-ghost"
                            style={{ padding: "4px 8px" }}
                            title="View Logs"
                          >
                            <FileText size={13} />
                          </Link>
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
                ))
              ) : (
                <tr>
                  <td
                    colSpan={9}
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
