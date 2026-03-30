import { useState } from "react";
import { useParams, Link } from "react-router-dom";
import { motion } from "framer-motion";
import { ArrowLeft, Download } from "lucide-react";
import { useApi } from "../hooks/use-api.ts";
import { jobs as jobsApi } from "../lib/api.ts";
import { LogStream } from "../components/LogStream.tsx";
import { StatusBadge } from "../components/StatusBadge.tsx";

const MAX_LOG_LINES = 10000;

export function LogViewer() {
  const { jobId } = useParams<{ jobId: string }>();
  const id = Number(jobId);

  if (!jobId || Number.isNaN(id)) {
    return (
      <div
        style={{ padding: 48, textAlign: "center", color: "var(--text-muted)" }}
      >
        Invalid job ID
      </div>
    );
  }

  const { data: job, error: jobError } = useApi(() => jobsApi.get(id), [id], {
    pollInterval: 5000,
  });

  const [activeTab, setActiveTab] = useState<"stdout" | "stderr">("stdout");

  /* Poll logs — more frequently when job is running */
  const isRunning = job?.status === "RUNNING";
  const { data: logData } = useApi(() => jobsApi.logs(id), [id], {
    pollInterval: isRunning ? 3000 : undefined,
  });

  const stdoutLines = logData
    ? logData.stdout.split("\n").filter(Boolean).slice(-MAX_LOG_LINES)
    : [];
  const stderrLines = logData
    ? logData.stderr.split("\n").filter(Boolean).slice(-MAX_LOG_LINES)
    : [];

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
            to="/jobs"
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
            <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
              <h1 style={{ fontSize: "1.3rem" }}>
                Job {jobId}
                {job && (
                  <span
                    style={{
                      fontFamily: "var(--font-body)",
                      fontWeight: 400,
                      color: "var(--text-secondary)",
                      fontSize: "1rem",
                      marginLeft: 8,
                    }}
                  >
                    {job.name}
                  </span>
                )}
              </h1>
              {job && <StatusBadge status={job.status} />}
            </div>
            <span
              style={{
                fontFamily: "var(--font-mono)",
                fontSize: "0.75rem",
                color: "var(--text-muted)",
              }}
            >
              {isRunning ? "Polling logs..." : "Static logs"}
            </span>
          </div>
        </div>

        <button className="btn btn-ghost">
          <Download size={14} />
          Download
        </button>
      </motion.div>

      {/* Tab bar */}
      <div
        style={{
          display: "flex",
          borderBottom: "1px solid var(--border-subtle)",
          gap: 0,
        }}
      >
        {(["stdout", "stderr"] as const).map((tab) => {
          const lines = tab === "stdout" ? stdoutLines : stderrLines;
          const active = activeTab === tab;
          return (
            <button
              key={tab}
              onClick={() => setActiveTab(tab)}
              style={{
                padding: "10px 20px",
                background: "transparent",
                border: "none",
                borderBottom: active
                  ? `2px solid ${tab === "stderr" ? "var(--st-failed)" : "var(--st-completed)"}`
                  : "2px solid transparent",
                color: active ? "var(--text-primary)" : "var(--text-muted)",
                cursor: "pointer",
                fontFamily: "var(--font-mono)",
                fontSize: "0.8rem",
                display: "flex",
                alignItems: "center",
                gap: 8,
                transition: "all 150ms",
              }}
            >
              <span style={{ textTransform: "uppercase" }}>{tab}</span>
              <span
                style={{
                  fontFamily: "var(--font-mono)",
                  fontSize: "0.65rem",
                  padding: "1px 6px",
                  borderRadius: 3,
                  background: active ? "var(--bg-overlay)" : "var(--bg-raised)",
                  color: "var(--text-secondary)",
                }}
              >
                {lines.length}
              </span>
            </button>
          );
        })}
      </div>

      {/* Log content */}
      <motion.div
        initial={{ opacity: 0 }}
        animate={{ opacity: 1 }}
        transition={{ delay: 0.1 }}
        className="panel"
        style={{ flex: 1, overflow: "hidden" }}
      >
        <LogStream
          lines={activeTab === "stdout" ? stdoutLines : stderrLines}
          stream={activeTab}
          loading={isRunning}
        />
      </motion.div>
    </div>
  );
}
