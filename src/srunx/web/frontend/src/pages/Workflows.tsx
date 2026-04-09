import { useEffect, useRef, useState } from "react";
import { motion } from "framer-motion";
import { Link } from "react-router-dom";
import {
  GitFork,
  Pencil,
  Play,
  Eye,
  Upload,
  Plus,
  X,
  FolderOpen,
} from "lucide-react";
import { useApi } from "../hooks/use-api.ts";
import { workflows as workflowsApi, files as filesApi } from "../lib/api.ts";
import type { Mount, Workflow } from "../lib/types.ts";
import { ErrorBanner } from "../components/ErrorBanner.tsx";

export function Workflows() {
  const [mounts, setMounts] = useState<Mount[]>([]);
  const [selectedMount, setSelectedMount] = useState<string | null>(null);

  // Load available mounts
  useEffect(() => {
    filesApi
      .mounts()
      .then((m) => {
        setMounts(m);
        if (m.length > 0) setSelectedMount(m[0].name);
      })
      .catch(() => {});
  }, []);

  const {
    data: workflowList,
    error,
    refetch,
  } = useApi(
    () =>
      selectedMount ? workflowsApi.list(selectedMount) : Promise.resolve([]),
    [selectedMount],
  );
  const fileInputRef = useRef<HTMLInputElement>(null);
  const [uploadError, setUploadError] = useState<string | null>(null);

  const handleUpload = async (file: File) => {
    if (!selectedMount) return;
    try {
      setUploadError(null);
      const text = await file.text();
      await workflowsApi.upload(text, file.name, selectedMount);
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
          <div
            style={{
              display: "flex",
              alignItems: "center",
              gap: 8,
              color: "var(--text-muted)",
              fontSize: "0.85rem",
            }}
          >
            {mounts.length > 0 ? (
              <>
                <FolderOpen size={14} />
                <select
                  className="input"
                  value={selectedMount ?? ""}
                  onChange={(e) => setSelectedMount(e.target.value || null)}
                  style={{
                    width: 180,
                    fontFamily: "var(--font-mono)",
                    fontSize: "0.8rem",
                    padding: "2px 8px",
                  }}
                >
                  {mounts.map((m) => (
                    <option key={m.name} value={m.name}>
                      {m.name}
                    </option>
                  ))}
                </select>
              </>
            ) : (
              <span>No mounts configured. Add one in Settings.</span>
            )}
          </div>
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
          <Link
            to={
              selectedMount
                ? `/workflows/new?mount=${encodeURIComponent(selectedMount)}`
                : "/workflows/new"
            }
            className="btn btn-primary"
          >
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

      <ErrorBanner error={uploadError ?? error} />

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
            <WorkflowCard
              key={wf.name}
              workflow={wf}
              mount={selectedMount!}
              index={i}
              onDelete={async () => {
                if (!selectedMount) return;
                try {
                  await workflowsApi.delete(wf.name, selectedMount);
                  refetch();
                } catch (err) {
                  setUploadError(
                    err instanceof Error ? err.message : "Delete failed",
                  );
                }
              }}
            />
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
  mount: string;
  index: number;
  onDelete: () => void;
};

function WorkflowCard({ workflow, mount, index, onDelete }: WorkflowCardProps) {
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
          <div style={{ display: "flex", alignItems: "center", gap: 4 }}>
            <button
              className="btn btn-ghost"
              onClick={(e) => {
                e.stopPropagation();
                e.preventDefault();
                if (
                  window.confirm(
                    `Delete workflow "${workflow.name}"? This cannot be undone.`,
                  )
                ) {
                  onDelete();
                }
              }}
              style={{
                padding: 4,
                minWidth: "auto",
                color: "var(--text-muted)",
              }}
              title="Delete workflow"
            >
              <X size={14} />
            </button>
            <GitFork size={16} color="var(--text-muted)" />
          </div>
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
            to={`/workflows/${encodeURIComponent(workflow.name)}?mount=${encodeURIComponent(mount)}`}
            className="btn btn-ghost"
            style={{ flex: 1, justifyContent: "center" }}
          >
            <Eye size={13} />
            View DAG
          </Link>
          <Link
            to={`/workflows/${encodeURIComponent(workflow.name)}/edit?mount=${encodeURIComponent(mount)}`}
            className="btn btn-ghost"
            style={{ flex: 1, justifyContent: "center" }}
            onClick={(e) => e.stopPropagation()}
          >
            <Pencil size={13} />
            Edit
          </Link>
          <Link
            to={`/workflows/${encodeURIComponent(workflow.name)}?mount=${encodeURIComponent(mount)}&run=1`}
            className="btn btn-primary"
            style={{ flex: 1, justifyContent: "center" }}
            onClick={(e) => e.stopPropagation()}
          >
            <Play size={13} />
            Run
          </Link>
        </div>
      </div>
    </motion.div>
  );
}
