import { useState } from "react";
import { motion } from "framer-motion";
import { X, Trash2, FolderOpen } from "lucide-react";
import type {
  BuilderJob,
  BuilderContainer,
  ContainerRuntime,
} from "../lib/types.ts";
import { FileBrowser } from "./FileBrowser.tsx";

/* ── Props ───────────────────────────────────── */

type JobPropertyPanelProps = {
  job: BuilderJob;
  onUpdate: (updates: Partial<BuilderJob>) => void;
  onClose: () => void;
  onDelete: () => void;
};

/* ── Shared styles ───────────────────────────── */

const labelStyle: React.CSSProperties = {
  fontFamily: "var(--font-mono)",
  fontSize: "0.7rem",
  color: "var(--text-muted)",
  textTransform: "uppercase",
  letterSpacing: "0.06em",
  marginBottom: 4,
};

const fieldStyle: React.CSSProperties = {
  display: "flex",
  flexDirection: "column",
};

const sectionDividerStyle: React.CSSProperties = {
  borderTop: "1px solid var(--border-ghost)",
  paddingTop: "var(--sp-5)",
  display: "flex",
  flexDirection: "column",
  gap: "var(--sp-3)",
};

const sectionTitleStyle: React.CSSProperties = {
  fontFamily: "var(--font-display)",
  fontSize: "0.7rem",
  fontWeight: 500,
  textTransform: "uppercase",
  letterSpacing: "0.08em",
  color: "var(--text-secondary)",
  marginBottom: 2,
};

const gridStyle: React.CSSProperties = {
  display: "grid",
  gridTemplateColumns: "1fr 1fr",
  gap: "var(--sp-3)",
};

const fileBtnStyle: React.CSSProperties = {
  background: "none",
  border: "none",
  color: "var(--text-muted)",
  padding: 4,
  cursor: "pointer",
  borderRadius: "var(--radius-sm)",
  display: "flex",
  alignItems: "center",
  justifyContent: "center",
  flexShrink: 0,
};

const inputRowStyle: React.CSSProperties = {
  display: "flex",
  alignItems: "center",
  gap: 4,
};

/* ── Component ───────────────────────────────── */

export function JobPropertyPanel({
  job,
  onUpdate,
  onClose,
  onDelete,
}: JobPropertyPanelProps) {
  function handleTextChange(
    field: keyof BuilderJob,
    value: string,
    required: boolean,
  ) {
    if (required) {
      onUpdate({ [field]: value });
    } else {
      onUpdate({ [field]: value === "" ? null : value });
    }
  }

  function handleNumberChange(field: keyof BuilderJob, value: string) {
    if (value === "") {
      onUpdate({ [field]: null });
    } else {
      const parsed = Number(value);
      if (!Number.isNaN(parsed)) {
        onUpdate({ [field]: parsed });
      }
    }
  }

  function handleCondaChange(value: string) {
    if (value === "") {
      onUpdate({ conda: null });
    } else {
      onUpdate({ conda: value, venv: null });
    }
  }

  function handleVenvChange(value: string) {
    if (value === "") {
      onUpdate({ venv: null });
    } else {
      onUpdate({ venv: value, conda: null });
    }
  }

  const [browserTarget, setBrowserTarget] = useState<
    "command" | "work_dir" | "log_dir" | null
  >(null);

  function handleBrowserSelect(remotePath: string) {
    if (!browserTarget) return;

    if (browserTarget === "command") {
      // Insert path relative to work_dir if possible, else absolute
      let insertPath = remotePath;
      if (job.work_dir) {
        const workDirPrefix = job.work_dir.endsWith("/")
          ? job.work_dir
          : job.work_dir + "/";
        if (remotePath.startsWith(workDirPrefix)) {
          insertPath = remotePath.slice(workDirPrefix.length);
        } else if (remotePath === job.work_dir) {
          insertPath = ".";
        }
      }
      const current = job.command.trim();
      const newCommand = current ? `${current} ${insertPath}` : insertPath;
      onUpdate({ command: newCommand });
    } else if (browserTarget === "work_dir") {
      onUpdate({ work_dir: remotePath });
    } else if (browserTarget === "log_dir") {
      onUpdate({ log_dir: remotePath });
    }

    setBrowserTarget(null);
  }

  const hasContainer = job.container !== null;

  function toggleContainer() {
    if (hasContainer) {
      onUpdate({ container: null });
    } else {
      onUpdate({
        container: { runtime: "pyxis", image: "", mounts: "", workdir: "" },
      });
    }
  }

  function handleContainerField(field: keyof BuilderContainer, value: string) {
    if (!job.container) return;
    onUpdate({ container: { ...job.container, [field]: value } });
  }

  function handleContainerRuntime(value: string) {
    if (!job.container) return;
    onUpdate({
      container: { ...job.container, runtime: value as ContainerRuntime },
    });
  }

  return (
    <motion.div
      initial={{ opacity: 0, x: 16 }}
      animate={{ opacity: 1, x: 0 }}
      transition={{ duration: 0.25, ease: [0.16, 1, 0.3, 1] }}
      style={{
        width: 340,
        flexShrink: 0,
        background: "var(--bg-surface)",
        borderLeft: "1px solid var(--border-default)",
        padding: "var(--sp-5)",
        display: "flex",
        flexDirection: "column",
        gap: "var(--sp-5)",
        overflow: "auto",
        height: "100%",
      }}
    >
      {/* ── Header ────────────────────────────── */}
      <div
        style={{
          display: "flex",
          alignItems: "flex-start",
          justifyContent: "space-between",
          gap: 8,
        }}
      >
        <h3
          style={{
            fontSize: "1.1rem",
            wordBreak: "break-word",
            flex: 1,
          }}
        >
          {job.name}
        </h3>
        <div style={{ display: "flex", gap: 4, flexShrink: 0 }}>
          <button
            onClick={onDelete}
            title="Delete job"
            style={{
              background: "none",
              border: "none",
              color: "var(--st-failed)",
              cursor: "pointer",
              padding: 4,
              borderRadius: "var(--radius-sm)",
              display: "flex",
              alignItems: "center",
              justifyContent: "center",
              transition: "background var(--duration-fast) var(--ease-out)",
            }}
            onMouseEnter={(e) => {
              e.currentTarget.style.background = "var(--st-failed-dim)";
            }}
            onMouseLeave={(e) => {
              e.currentTarget.style.background = "none";
            }}
          >
            <Trash2 size={14} />
          </button>
          <button
            onClick={onClose}
            title="Close panel"
            style={{
              background: "none",
              border: "none",
              color: "var(--text-muted)",
              cursor: "pointer",
              padding: 4,
              borderRadius: "var(--radius-sm)",
              display: "flex",
              alignItems: "center",
              justifyContent: "center",
              transition: "background var(--duration-fast) var(--ease-out)",
            }}
            onMouseEnter={(e) => {
              e.currentTarget.style.background = "var(--bg-hover)";
            }}
            onMouseLeave={(e) => {
              e.currentTarget.style.background = "none";
            }}
          >
            <X size={14} />
          </button>
        </div>
      </div>

      {/* ── Basic ─────────────────────────────── */}
      <div style={sectionDividerStyle}>
        <div style={sectionTitleStyle}>Basic</div>

        <div style={fieldStyle}>
          <label style={labelStyle}>Name</label>
          <input
            className="input"
            type="text"
            value={job.name}
            onChange={(e) => handleTextChange("name", e.target.value, true)}
            placeholder="job_name"
            required
          />
        </div>

        <div style={fieldStyle}>
          <label style={labelStyle}>Command</label>
          <div style={inputRowStyle}>
            <input
              className="input"
              type="text"
              value={job.command}
              onChange={(e) =>
                handleTextChange("command", e.target.value, true)
              }
              placeholder="python train.py --epochs 100"
              required
              style={{ flex: 1 }}
            />
            <button
              title="Browse files"
              style={fileBtnStyle}
              onClick={() => setBrowserTarget("command")}
              onMouseEnter={(e) => {
                e.currentTarget.style.background = "var(--bg-hover)";
              }}
              onMouseLeave={(e) => {
                e.currentTarget.style.background = "none";
              }}
            >
              <FolderOpen size={14} />
            </button>
          </div>
        </div>

        <div style={gridStyle}>
          <div style={fieldStyle}>
            <label style={labelStyle}>Work Dir</label>
            <div style={inputRowStyle}>
              <input
                className="input"
                type="text"
                value={job.work_dir ?? ""}
                onChange={(e) =>
                  handleTextChange("work_dir", e.target.value, false)
                }
                placeholder="/path/to/workdir"
                style={{ flex: 1, minWidth: 0 }}
              />
              <button
                title="Browse directories"
                style={fileBtnStyle}
                onClick={() => setBrowserTarget("work_dir")}
                onMouseEnter={(e) => {
                  e.currentTarget.style.background = "var(--bg-hover)";
                }}
                onMouseLeave={(e) => {
                  e.currentTarget.style.background = "none";
                }}
              >
                <FolderOpen size={14} />
              </button>
            </div>
          </div>
          <div style={fieldStyle}>
            <label style={labelStyle}>Log Dir</label>
            <div style={inputRowStyle}>
              <input
                className="input"
                type="text"
                value={job.log_dir ?? ""}
                onChange={(e) =>
                  handleTextChange("log_dir", e.target.value, false)
                }
                placeholder="/path/to/logs"
                style={{ flex: 1, minWidth: 0 }}
              />
              <button
                title="Browse directories"
                style={fileBtnStyle}
                onClick={() => setBrowserTarget("log_dir")}
                onMouseEnter={(e) => {
                  e.currentTarget.style.background = "var(--bg-hover)";
                }}
                onMouseLeave={(e) => {
                  e.currentTarget.style.background = "none";
                }}
              >
                <FolderOpen size={14} />
              </button>
            </div>
          </div>
        </div>
      </div>

      {/* ── Resources ─────────────────────────── */}
      <div style={sectionDividerStyle}>
        <div style={sectionTitleStyle}>Resources</div>

        <div style={gridStyle}>
          <div style={fieldStyle}>
            <label style={labelStyle}>Nodes</label>
            <input
              className="input"
              type="number"
              min={1}
              value={job.nodes ?? ""}
              onChange={(e) => handleNumberChange("nodes", e.target.value)}
              placeholder="1"
            />
          </div>
          <div style={fieldStyle}>
            <label style={labelStyle}>GPUs per Node</label>
            <input
              className="input"
              type="number"
              min={0}
              value={job.gpus_per_node ?? ""}
              onChange={(e) =>
                handleNumberChange("gpus_per_node", e.target.value)
              }
              placeholder="0"
            />
          </div>
        </div>

        <div style={gridStyle}>
          <div style={fieldStyle}>
            <label style={labelStyle}>Tasks per Node</label>
            <input
              className="input"
              type="number"
              min={1}
              value={job.ntasks_per_node ?? ""}
              onChange={(e) =>
                handleNumberChange("ntasks_per_node", e.target.value)
              }
              placeholder="1"
            />
          </div>
          <div style={fieldStyle}>
            <label style={labelStyle}>CPUs per Task</label>
            <input
              className="input"
              type="number"
              min={1}
              value={job.cpus_per_task ?? ""}
              onChange={(e) =>
                handleNumberChange("cpus_per_task", e.target.value)
              }
              placeholder="1"
            />
          </div>
        </div>

        <div style={fieldStyle}>
          <label style={labelStyle}>Memory</label>
          <input
            className="input"
            type="text"
            value={job.memory_per_node ?? ""}
            onChange={(e) =>
              handleTextChange("memory_per_node", e.target.value, false)
            }
            placeholder="32GB"
          />
        </div>

        <div style={gridStyle}>
          <div style={fieldStyle}>
            <label style={labelStyle}>Time Limit</label>
            <input
              className="input"
              type="text"
              value={job.time_limit ?? ""}
              onChange={(e) =>
                handleTextChange("time_limit", e.target.value, false)
              }
              placeholder="4:00:00"
            />
          </div>
          <div style={fieldStyle}>
            <label style={labelStyle}>Partition</label>
            <input
              className="input"
              type="text"
              value={job.partition ?? ""}
              onChange={(e) =>
                handleTextChange("partition", e.target.value, false)
              }
              placeholder="gpu"
            />
          </div>
        </div>

        <div style={fieldStyle}>
          <label style={labelStyle}>Node List</label>
          <input
            className="input"
            type="text"
            value={job.nodelist ?? ""}
            onChange={(e) =>
              handleTextChange("nodelist", e.target.value, false)
            }
            placeholder="node[01-04]"
          />
        </div>
      </div>

      {/* ── Environment ───────────────────────── */}
      <div style={sectionDividerStyle}>
        <div style={sectionTitleStyle}>Environment</div>

        <div style={fieldStyle}>
          <label style={labelStyle}>Conda</label>
          <input
            className="input"
            type="text"
            value={job.conda ?? ""}
            onChange={(e) => handleCondaChange(e.target.value)}
            placeholder="my_env"
            disabled={job.venv !== null}
            style={
              job.venv !== null
                ? { opacity: 0.4, cursor: "not-allowed" }
                : undefined
            }
          />
        </div>

        <div style={fieldStyle}>
          <label style={labelStyle}>Venv</label>
          <input
            className="input"
            type="text"
            value={job.venv ?? ""}
            onChange={(e) => handleVenvChange(e.target.value)}
            placeholder="/path/to/venv"
            disabled={job.conda !== null}
            style={
              job.conda !== null
                ? { opacity: 0.4, cursor: "not-allowed" }
                : undefined
            }
          />
        </div>

        <div style={fieldStyle}>
          <label style={labelStyle}>Env Variables</label>
          <textarea
            className="input"
            rows={3}
            value={job.env_vars}
            onChange={(e) => onUpdate({ env_vars: e.target.value })}
            placeholder={"KEY=value\nPATH=/usr/local/bin"}
            style={{
              fontFamily: "var(--font-mono)",
              fontSize: "0.75rem",
              resize: "vertical",
            }}
          />
        </div>
      </div>

      {/* ── Container ─────────────────────────── */}
      <div style={sectionDividerStyle}>
        <div
          style={{
            display: "flex",
            justifyContent: "space-between",
            alignItems: "center",
          }}
        >
          <div style={sectionTitleStyle}>Container</div>
          <button
            onClick={toggleContainer}
            style={{
              background: hasContainer
                ? "var(--accent-dim)"
                : "var(--bg-overlay)",
              border: "none",
              color: hasContainer ? "var(--accent)" : "var(--text-muted)",
              cursor: "pointer",
              padding: "2px 8px",
              borderRadius: "var(--radius-sm)",
              fontFamily: "var(--font-mono)",
              fontSize: "0.65rem",
              transition: "all var(--duration-fast) var(--ease-out)",
            }}
          >
            {hasContainer ? "Remove" : "Enable"}
          </button>
        </div>

        {hasContainer && job.container && (
          <>
            <div style={gridStyle}>
              <div style={fieldStyle}>
                <label style={labelStyle}>Runtime</label>
                <select
                  className="input"
                  value={job.container.runtime}
                  onChange={(e) => handleContainerRuntime(e.target.value)}
                >
                  <option value="pyxis">Pyxis</option>
                  <option value="apptainer">Apptainer</option>
                  <option value="singularity">Singularity</option>
                </select>
              </div>
              <div style={fieldStyle}>
                <label style={labelStyle}>Workdir</label>
                <input
                  className="input"
                  type="text"
                  value={job.container.workdir}
                  onChange={(e) =>
                    handleContainerField("workdir", e.target.value)
                  }
                  placeholder="/workspace"
                />
              </div>
            </div>

            <div style={fieldStyle}>
              <label style={labelStyle}>Image</label>
              <input
                className="input"
                type="text"
                value={job.container.image}
                onChange={(e) => handleContainerField("image", e.target.value)}
                placeholder="nvcr.io/nvidia/pytorch:24.01-py3"
              />
            </div>

            <div style={fieldStyle}>
              <label style={labelStyle}>Mounts</label>
              <input
                className="input"
                type="text"
                value={job.container.mounts}
                onChange={(e) => handleContainerField("mounts", e.target.value)}
                placeholder="/data:/data, /models:/models"
              />
            </div>
          </>
        )}
      </div>

      {/* ── Retry ─────────────────────────────── */}
      <div style={sectionDividerStyle}>
        <div style={sectionTitleStyle}>Retry</div>

        <div style={gridStyle}>
          <div style={fieldStyle}>
            <label style={labelStyle}>Retries</label>
            <input
              className="input"
              type="number"
              min={0}
              value={job.retry ?? ""}
              onChange={(e) => handleNumberChange("retry", e.target.value)}
              placeholder="0"
            />
          </div>
          <div style={fieldStyle}>
            <label style={labelStyle}>Delay (sec)</label>
            <input
              className="input"
              type="number"
              min={0}
              value={job.retry_delay ?? ""}
              onChange={(e) =>
                handleNumberChange("retry_delay", e.target.value)
              }
              placeholder="60"
            />
          </div>
        </div>
      </div>

      {/* ── File Browser Modal ────────────────── */}
      {browserTarget !== null && (
        <FileBrowser
          mode={browserTarget === "command" ? "file" : "directory"}
          value={
            browserTarget === "command"
              ? job.command
              : browserTarget === "work_dir"
                ? (job.work_dir ?? "")
                : (job.log_dir ?? "")
          }
          onSelect={handleBrowserSelect}
          onClose={() => setBrowserTarget(null)}
          workDir={job.work_dir}
        />
      )}
    </motion.div>
  );
}
