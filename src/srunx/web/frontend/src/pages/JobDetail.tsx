import { useMemo } from "react";
import { Link, useLocation, useParams } from "react-router-dom";
import { motion } from "framer-motion";
import { ArrowLeft, Bell, FileText, Info } from "lucide-react";
import { useApi } from "../hooks/use-api.ts";
import { jobs as jobsApi } from "../lib/api.ts";
import { StatusBadge } from "../components/StatusBadge.tsx";
import { JobLogsTab } from "../components/JobLogsTab.tsx";
import { JobNotificationsTab } from "../components/JobNotificationsTab.tsx";

type TabKey = "overview" | "logs" | "notifications";

const TABS: ReadonlyArray<{
  key: TabKey;
  label: string;
  icon: typeof Info;
  suffix: string;
}> = [
  { key: "overview", label: "Overview", icon: Info, suffix: "" },
  { key: "logs", label: "Logs", icon: FileText, suffix: "/logs" },
  {
    key: "notifications",
    label: "Notifications",
    icon: Bell,
    suffix: "/notifications",
  },
];

function tabFromPath(pathname: string, jobId: string): TabKey {
  if (pathname.endsWith(`/jobs/${jobId}/logs`)) return "logs";
  if (pathname.endsWith(`/jobs/${jobId}/notifications`)) return "notifications";
  return "overview";
}

export function JobDetail() {
  const { jobId } = useParams<{ jobId: string }>();
  const id = Number(jobId);
  const isValidId = Boolean(jobId) && !Number.isNaN(id);
  const location = useLocation();
  const activeTab = useMemo(
    () => (jobId ? tabFromPath(location.pathname, jobId) : "overview"),
    [location.pathname, jobId],
  );

  const { data: job, error: jobError } = useApi(
    () => (isValidId ? jobsApi.get(id) : Promise.resolve(null)),
    [id, isValidId],
    { pollInterval: 5000 },
  );

  if (!isValidId) {
    return (
      <div
        style={{ padding: 48, textAlign: "center", color: "var(--text-muted)" }}
      >
        Invalid job ID
      </div>
    );
  }

  if (jobError) {
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
          Failed to load job
        </div>
        <div
          style={{
            fontFamily: "var(--font-mono)",
            fontSize: "0.8rem",
            color: "var(--text-muted)",
          }}
        >
          {jobError}
        </div>
      </div>
    );
  }

  const isRunning = job?.status === "RUNNING";

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
          gap: "var(--sp-3)",
        }}
      >
        <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
          <Link
            to="/jobs"
            style={{
              display: "flex",
              padding: 6,
              borderRadius: 6,
              color: "var(--text-muted)",
              border: "1px solid var(--border-subtle)",
            }}
            aria-label="Back to jobs"
          >
            <ArrowLeft size={16} />
          </Link>
          <div>
            <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
              <h1 style={{ fontSize: "1.25rem" }}>
                Job {jobId}
                {job && (
                  <span
                    style={{
                      fontFamily: "var(--font-body)",
                      fontWeight: 400,
                      color: "var(--text-secondary)",
                      fontSize: "0.95rem",
                      marginLeft: 8,
                    }}
                  >
                    {job.name}
                  </span>
                )}
              </h1>
              {job && <StatusBadge status={job.status} />}
            </div>
          </div>
        </div>
      </motion.div>

      {/* Tab nav */}
      <div
        style={{
          display: "flex",
          borderBottom: "1px solid var(--border-subtle)",
          gap: 0,
        }}
      >
        {TABS.map((tab) => {
          const Icon = tab.icon;
          const active = activeTab === tab.key;
          return (
            <Link
              key={tab.key}
              to={`/jobs/${jobId}${tab.suffix}`}
              style={{
                padding: "10px 18px",
                display: "flex",
                alignItems: "center",
                gap: 8,
                borderBottom: active
                  ? "2px solid var(--accent, #818cf8)"
                  : "2px solid transparent",
                color: active ? "var(--text-primary)" : "var(--text-muted)",
                textDecoration: "none",
                fontSize: "0.85rem",
                transition: "all 150ms",
              }}
            >
              <Icon size={14} />
              <span>{tab.label}</span>
            </Link>
          );
        })}
      </div>

      {/* Tab content */}
      <div style={{ flex: 1, minHeight: 0 }}>
        {activeTab === "overview" && <OverviewContent job={job} />}
        {activeTab === "logs" && (
          <JobLogsTab jobId={id} isRunning={isRunning} />
        )}
        {activeTab === "notifications" && <JobNotificationsTab jobId={id} />}
      </div>
    </div>
  );
}

type OverviewContentProps = {
  job: Awaited<ReturnType<typeof jobsApi.get>> | null | undefined;
};

function OverviewContent({ job }: OverviewContentProps) {
  if (job === undefined || job === null) {
    return (
      <div
        className="panel"
        style={{
          padding: "var(--sp-5)",
          color: "var(--text-muted)",
          fontSize: "0.85rem",
        }}
      >
        Loading job details…
      </div>
    );
  }

  const rows: ReadonlyArray<[string, React.ReactNode]> = [
    ["Job ID", job.job_id ?? "—"],
    ["Name", job.name],
    [
      "Command",
      <code
        key="cmd"
        style={{
          fontFamily: "var(--font-mono)",
          fontSize: "0.78rem",
          wordBreak: "break-all",
        }}
      >
        {job.command?.join(" ") ?? "—"}
      </code>,
    ],
    ["Status", <StatusBadge key="st" status={job.status} />],
    ["Partition", job.resources.partition ?? "—"],
    ["Nodes", job.resources.nodes ?? 1],
    ["GPUs per node", job.resources.gpus_per_node ?? 0],
    ["Time limit", job.resources.time_limit ?? "—"],
    ["Memory per node", job.resources.memory_per_node ?? "—"],
  ];

  return (
    <motion.div
      initial={{ opacity: 0, y: 8 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.2 }}
      className="panel"
    >
      <div className="panel-body">
        <table
          style={{
            width: "100%",
            borderCollapse: "collapse",
            fontSize: "0.85rem",
          }}
        >
          <tbody>
            {rows.map(([label, value]) => (
              <tr key={label}>
                <td
                  style={{
                    padding: "8px 12px",
                    color: "var(--text-muted)",
                    width: 160,
                    borderBottom: "1px solid var(--border-ghost)",
                    verticalAlign: "top",
                  }}
                >
                  {label}
                </td>
                <td
                  style={{
                    padding: "8px 12px",
                    borderBottom: "1px solid var(--border-ghost)",
                  }}
                >
                  {value}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </motion.div>
  );
}
