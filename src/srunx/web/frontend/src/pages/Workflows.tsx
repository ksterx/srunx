import { useRef, useState } from "react";
import { motion } from "framer-motion";
import { Link } from "react-router-dom";
import { GitFork, Play, Eye, Upload, Plus } from "lucide-react";
import { useApi } from "../hooks/use-api.ts";
import { workflows as workflowsApi } from "../lib/api.ts";
import type { Workflow } from "../lib/types.ts";

export function Workflows() {
  const {
    data: workflowList,
    error,
    refetch,
  } = useApi(() => workflowsApi.list(), []);
  const fileInputRef = useRef<HTMLInputElement>(null);
  const [uploadError, setUploadError] = useState<string | null>(null);

  const handleUpload = async (file: File) => {
    try {
      setUploadError(null);
      const text = await file.text();
      await workflowsApi.upload(text, file.name);
      refetch();
    } catch (err) {
      setUploadError(err instanceof Error ? err.message : "Upload failed");
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
        style={{
          display: "flex",
          justifyContent: "space-between",
          alignItems: "flex-start",
        }}
      >
        <div>
          <h1 style={{ marginBottom: 4 }}>Workflows</h1>
          <p style={{ color: "var(--text-muted)", fontSize: "0.85rem" }}>
            YAML-defined pipelines with dependency graphs
          </p>
        </div>
        <input
          ref={fileInputRef}
          type="file"
          accept=".yaml,.yml"
          style={{ display: "none" }}
          onChange={(e) => {
            const file = e.target.files?.[0];
            if (file) handleUpload(file);
            e.target.value = "";
          }}
        />
        <div style={{ display: "flex", gap: 8 }}>
          <Link to="/workflows/new" className="btn btn-primary">
            <Plus size={14} />
            New Workflow
          </Link>
          <button
            className="btn btn-ghost"
            onClick={() => fileInputRef.current?.click()}
          >
            <Upload size={14} />
            Upload YAML
          </button>
        </div>
      </motion.div>

      {(error || uploadError) && (
        <div
          style={{
            padding: "var(--sp-3) var(--sp-4)",
            background: "var(--st-failed-dim)",
            border: "1px solid rgba(244,63,94,0.3)",
            borderRadius: "var(--radius-md)",
            color: "var(--st-failed)",
            fontFamily: "var(--font-mono)",
            fontSize: "0.8rem",
          }}
        >
          {uploadError ?? error}
        </div>
      )}

      {/* Workflow cards grid */}
      <div
        style={{
          display: "grid",
          gridTemplateColumns: "repeat(auto-fill, minmax(320px, 1fr))",
          gap: "var(--sp-4)",
        }}
      >
        {workflowList && workflowList.length > 0 ? (
          workflowList.map((wf, i) => (
            <WorkflowCard key={wf.name} workflow={wf} index={i} />
          ))
        ) : workflowList === null ? (
          /* Loading skeletons */
          Array.from({ length: 3 }).map((_, i) => (
            <div key={i} className="panel skeleton" style={{ height: 180 }} />
          ))
        ) : (
          <motion.div
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            className="panel"
            style={{
              gridColumn: "1 / -1",
              padding: 48,
              textAlign: "center",
              color: "var(--text-muted)",
            }}
          >
            <GitFork size={32} style={{ marginBottom: 12, opacity: 0.4 }} />
            <div>No workflows found. Upload a YAML file to get started.</div>
          </motion.div>
        )}
      </div>
    </div>
  );
}

/* ── Workflow Card ─────────────────────────────── */

type WorkflowCardProps = {
  workflow: Workflow;
  index: number;
};

function WorkflowCard({ workflow, index }: WorkflowCardProps) {
  const jobCount = workflow.jobs.length;
  const depCount = workflow.jobs.reduce(
    (sum, j) => sum + (j.depends_on?.length ?? 0),
    0,
  );
  const totalGpus = workflow.jobs.reduce((sum, j) => {
    if ("resources" in j && j.resources.gpus_per_node) {
      return sum + j.resources.gpus_per_node;
    }
    return sum;
  }, 0);

  return (
    <motion.div
      initial={{ opacity: 0, y: 16 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{
        delay: index * 0.06,
        duration: 0.4,
        ease: [0.16, 1, 0.3, 1],
      }}
      className="panel"
      style={{ cursor: "pointer", overflow: "hidden" }}
    >
      {/* Top accent bar */}
      <div
        style={{
          height: 3,
          background: "linear-gradient(90deg, var(--accent), var(--resource))",
        }}
      />

      <div style={{ padding: "var(--sp-5)" }}>
        {/* Header */}
        <div
          style={{
            display: "flex",
            justifyContent: "space-between",
            alignItems: "center",
            marginBottom: 16,
          }}
        >
          <h3
            style={{
              fontFamily: "var(--font-display)",
              fontSize: "1rem",
              color: "var(--text-primary)",
            }}
          >
            {workflow.name}
          </h3>
          <GitFork size={16} color="var(--text-muted)" />
        </div>

        {/* Stats */}
        <div
          style={{
            display: "flex",
            gap: 24,
            marginBottom: 20,
          }}
        >
          <div className="metric">
            <span className="metric-label">Jobs</span>
            <span
              style={{
                fontFamily: "var(--font-mono)",
                fontSize: "1.1rem",
                fontWeight: 600,
                color: "var(--text-primary)",
              }}
            >
              {jobCount}
            </span>
          </div>
          <div className="metric">
            <span className="metric-label">Deps</span>
            <span
              style={{
                fontFamily: "var(--font-mono)",
                fontSize: "1.1rem",
                fontWeight: 600,
                color: "var(--text-secondary)",
              }}
            >
              {depCount}
            </span>
          </div>
          <div className="metric">
            <span className="metric-label">GPUs</span>
            <span
              style={{
                fontFamily: "var(--font-mono)",
                fontSize: "1.1rem",
                fontWeight: 600,
                color: "var(--resource)",
              }}
            >
              {totalGpus}
            </span>
          </div>
        </div>

        {/* Job name chips */}
        <div
          style={{
            display: "flex",
            flexWrap: "wrap",
            gap: 6,
            marginBottom: 16,
          }}
        >
          {workflow.jobs.slice(0, 5).map((job) => (
            <span
              key={job.name}
              style={{
                fontFamily: "var(--font-mono)",
                fontSize: "0.65rem",
                padding: "2px 8px",
                borderRadius: 3,
                background: "var(--bg-overlay)",
                color: "var(--text-secondary)",
                border: "1px solid var(--border-ghost)",
              }}
            >
              {job.name}
            </span>
          ))}
          {workflow.jobs.length > 5 && (
            <span
              style={{
                fontFamily: "var(--font-mono)",
                fontSize: "0.65rem",
                padding: "2px 8px",
                color: "var(--text-muted)",
              }}
            >
              +{workflow.jobs.length - 5}
            </span>
          )}
        </div>

        {/* Actions */}
        <div style={{ display: "flex", gap: 8 }}>
          <Link
            to={`/workflows/${workflow.name}`}
            className="btn btn-ghost"
            style={{ flex: 1, justifyContent: "center" }}
          >
            <Eye size={13} />
            View DAG
          </Link>
          <button
            className="btn btn-primary"
            style={{ flex: 1, justifyContent: "center" }}
          >
            <Play size={13} />
            Run
          </button>
        </div>
      </div>
    </motion.div>
  );
}
