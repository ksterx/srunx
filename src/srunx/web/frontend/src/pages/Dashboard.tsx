import { motion } from "framer-motion";
import {
  Layers,
  GitFork,
  Cpu,
  AlertTriangle,
  CheckCircle2,
  Clock,
  Activity,
} from "lucide-react";
import { Link } from "react-router-dom";
import { useApi } from "../hooks/use-api.ts";
import { jobs, resources, history } from "../lib/api.ts";
import { StatusBadge } from "../components/StatusBadge.tsx";
import { ResourceGauge } from "../components/ResourceGauge.tsx";
import { ErrorBanner } from "../components/ErrorBanner.tsx";

const EASE = [0.16, 1, 0.3, 1] as [number, number, number, number];

const stagger = (i: number) => ({
  initial: { opacity: 0, y: 12 },
  animate: { opacity: 1, y: 0 },
  transition: { delay: i * 0.06, duration: 0.4, ease: EASE },
});

export function Dashboard() {
  const { data: jobList, error: jobsError } = useApi(() => jobs.list(), [], {
    pollInterval: 10000,
  });
  const { data: snapshots, error: resourcesError } = useApi(
    () => resources.snapshot(),
    [],
    { pollInterval: 15000 },
  );
  const { data: stats } = useApi(() => history.stats(), []);

  const apiError = jobsError || resourcesError;

  const activeJobs = jobList?.filter(
    (j) => j.status === "RUNNING" || j.status === "PENDING",
  );
  const failedJobs = jobList?.filter((j) => j.status === "FAILED");

  const totalGpuAvail =
    snapshots?.reduce((s, r) => s + r.gpus_available, 0) ?? 0;
  const totalGpu = snapshots?.reduce((s, r) => s + r.total_gpus, 0) ?? 0;

  return (
    <div
      style={{ display: "flex", flexDirection: "column", gap: "var(--sp-6)" }}
    >
      {/* Page title */}
      <motion.div {...stagger(0)}>
        <h1 style={{ marginBottom: 4 }}>Dashboard</h1>
        <p style={{ color: "var(--text-muted)", fontSize: "0.85rem" }}>
          Cluster overview and recent activity
        </p>
      </motion.div>

      <ErrorBanner error={apiError} />

      {/* ── Metric cards row ──────────────────────── */}
      <div
        style={{
          display: "grid",
          gridTemplateColumns: "repeat(4, 1fr)",
          gap: "var(--sp-4)",
        }}
      >
        {[
          {
            icon: <Activity size={16} />,
            label: "Active Jobs",
            value: activeJobs?.length ?? "—",
            color: "var(--st-running)",
            bg: "var(--st-running-dim)",
          },
          {
            icon: <AlertTriangle size={16} />,
            label: "Failed",
            value: failedJobs?.length ?? "—",
            color: "var(--st-failed)",
            bg: "var(--st-failed-dim)",
          },
          {
            icon: <CheckCircle2 size={16} />,
            label: "Completed (Total)",
            value: stats?.completed ?? "—",
            color: "var(--st-completed)",
            bg: "var(--st-completed-dim)",
          },
          {
            icon: <Cpu size={16} />,
            label: "GPUs Available",
            value: `${totalGpuAvail}/${totalGpu}`,
            color: "var(--resource)",
            bg: "var(--resource-dim)",
          },
        ].map((card, i) => (
          <motion.div
            key={card.label}
            {...stagger(i + 1)}
            className="panel"
            style={{ padding: "var(--sp-5)" }}
          >
            <div
              style={{
                display: "flex",
                alignItems: "center",
                gap: 10,
                marginBottom: 12,
              }}
            >
              <div
                style={{
                  width: 32,
                  height: 32,
                  borderRadius: 8,
                  background: card.bg,
                  color: card.color,
                  display: "flex",
                  alignItems: "center",
                  justifyContent: "center",
                }}
              >
                {card.icon}
              </div>
              <span className="metric-label">{card.label}</span>
            </div>
            <span
              className="metric-value"
              style={{ color: card.color, fontSize: "1.6rem" }}
            >
              {card.value}
            </span>
          </motion.div>
        ))}
      </div>

      {/* ── Main content grid ─────────────────────── */}
      <div
        style={{
          display: "grid",
          gridTemplateColumns: "1fr 340px",
          gap: "var(--sp-4)",
        }}
      >
        {/* Active Jobs table */}
        <motion.div
          {...stagger(5)}
          className="panel"
          style={{ overflow: "hidden" }}
        >
          <div className="panel-header">
            <h3>
              <Layers size={14} style={{ marginRight: 8, verticalAlign: -2 }} />
              Active Jobs
            </h3>
            <Link
              to="/jobs"
              className="btn btn-ghost"
              style={{ fontSize: "0.7rem" }}
            >
              View All
            </Link>
          </div>
          <div style={{ overflow: "auto", maxHeight: 360 }}>
            <table className="data-table">
              <thead>
                <tr>
                  <th>ID</th>
                  <th>Name</th>
                  <th>Status</th>
                  <th>Partition</th>
                  <th>GPUs</th>
                </tr>
              </thead>
              <tbody>
                {activeJobs && activeJobs.length > 0 ? (
                  activeJobs.slice(0, 10).map((job) => (
                    <tr key={job.job_id ?? job.name}>
                      <td className="col-mono">{job.job_id ?? "—"}</td>
                      <td>
                        <Link
                          to={job.job_id ? `/jobs/${job.job_id}/logs` : "#"}
                          style={{ color: "var(--text-primary)" }}
                        >
                          {job.name}
                        </Link>
                      </td>
                      <td>
                        <StatusBadge status={job.status} size="sm" />
                      </td>
                      <td className="col-muted">
                        {job.resources.partition ?? "—"}
                      </td>
                      <td className="col-mono">
                        {job.resources.gpus_per_node ?? 0}
                      </td>
                    </tr>
                  ))
                ) : (
                  <tr>
                    <td
                      colSpan={5}
                      style={{
                        textAlign: "center",
                        color: "var(--text-muted)",
                        padding: 32,
                      }}
                    >
                      No active jobs
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </motion.div>

        {/* Right sidebar: Resources + Activity feed */}
        <div
          style={{
            display: "flex",
            flexDirection: "column",
            gap: "var(--sp-4)",
          }}
        >
          {/* GPU Resources */}
          <motion.div {...stagger(6)} className="panel">
            <div className="panel-header">
              <h3>
                <Cpu size={14} style={{ marginRight: 8, verticalAlign: -2 }} />
                GPU Resources
              </h3>
            </div>
            <div
              className="panel-body"
              style={{ display: "flex", flexDirection: "column", gap: 20 }}
            >
              {snapshots && snapshots.length > 0 ? (
                snapshots.map((snap, i) => (
                  <ResourceGauge
                    key={snap.partition ?? `p-${i}`}
                    label={snap.partition ?? "all"}
                    used={snap.gpus_in_use}
                    total={snap.total_gpus}
                    unit="GPU"
                  />
                ))
              ) : (
                <div
                  style={{
                    color: "var(--text-muted)",
                    fontSize: "0.85rem",
                    padding: "12px 0",
                  }}
                >
                  No resource data
                </div>
              )}
            </div>
          </motion.div>

          {/* Recent Activity (polling-based) */}
          <motion.div {...stagger(7)} className="panel" style={{ flex: 1 }}>
            <div className="panel-header">
              <h3>
                <GitFork
                  size={14}
                  style={{ marginRight: 8, verticalAlign: -2 }}
                />
                Recent Activity
              </h3>
            </div>
            <div className="panel-body">
              {activeJobs && activeJobs.length > 0 ? (
                <div
                  style={{ display: "flex", flexDirection: "column", gap: 8 }}
                >
                  {activeJobs.slice(0, 5).map((job) => (
                    <div
                      key={job.job_id ?? job.name}
                      style={{
                        display: "flex",
                        alignItems: "center",
                        gap: 10,
                        fontSize: "0.8rem",
                      }}
                    >
                      <Clock size={14} color="var(--text-muted)" />
                      <strong style={{ color: "var(--text-primary)" }}>
                        {job.name}
                      </strong>
                      <StatusBadge status={job.status} size="sm" />
                    </div>
                  ))}
                </div>
              ) : (
                <div style={{ color: "var(--text-muted)", fontSize: "0.8rem" }}>
                  No active jobs
                </div>
              )}
            </div>
          </motion.div>
        </div>
      </div>
    </div>
  );
}
