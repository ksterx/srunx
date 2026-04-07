import { useState, useEffect, useCallback } from "react";
import { motion } from "framer-motion";
import {
  FolderOpen,
  Plus,
  Save,
  Check,
  ChevronDown,
  ChevronUp,
} from "lucide-react";
import { config as configApi } from "../../lib/api.ts";
import type {
  ProjectInfo,
  SrunxConfig,
  ResourceDefaultsConfig,
  EnvironmentDefaultsConfig,
} from "../../lib/types.ts";

type FieldRowProps = {
  label: string;
  children: React.ReactNode;
};

function FieldRow({ label, children }: FieldRowProps) {
  return (
    <div
      style={{
        display: "flex",
        alignItems: "center",
        justifyContent: "space-between",
        gap: "var(--sp-6)",
        padding: "var(--sp-2) 0",
        borderBottom: "1px solid var(--border-ghost)",
      }}
    >
      <div
        style={{
          fontSize: "0.85rem",
          fontWeight: 500,
          color: "var(--text-primary)",
        }}
      >
        {label}
      </div>
      <div style={{ flexShrink: 0, minWidth: 200 }}>{children}</div>
    </div>
  );
}

export function ProjectTab() {
  const [projects, setProjects] = useState<ProjectInfo[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [success, setSuccess] = useState<string | null>(null);
  const [expandedMount, setExpandedMount] = useState<string | null>(null);
  const [editConfig, setEditConfig] = useState<SrunxConfig | null>(null);
  const [saving, setSaving] = useState(false);

  const load = useCallback(async () => {
    try {
      setLoading(true);
      const p = await configApi.listProjects();
      setProjects(p);
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to load projects");
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    load();
  }, [load]);

  useEffect(() => {
    if (success) {
      const t = setTimeout(() => setSuccess(null), 3000);
      return () => clearTimeout(t);
    }
  }, [success]);

  const handleExpand = async (mountName: string) => {
    if (expandedMount === mountName) {
      setExpandedMount(null);
      setEditConfig(null);
      return;
    }
    setExpandedMount(mountName);
    try {
      const resp = await configApi.getProject(mountName);
      setEditConfig(resp.config);
    } catch (e) {
      setError(
        e instanceof Error ? e.message : "Failed to load project config",
      );
    }
  };

  const handleInit = async (mountName: string) => {
    try {
      setSaving(true);
      const resp = await configApi.initProject(mountName);
      setEditConfig(resp.config);
      setSuccess(`Initialized srunx.json for "${mountName}"`);
      await load();
      setExpandedMount(mountName);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to initialize");
    } finally {
      setSaving(false);
    }
  };

  const handleSave = async () => {
    if (!expandedMount || !editConfig) return;
    try {
      setSaving(true);
      setError(null);
      const resp = await configApi.updateProject(expandedMount, editConfig);
      setEditConfig(resp.config);
      setSuccess(`Project config saved for "${expandedMount}"`);
      await load();
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to save");
    } finally {
      setSaving(false);
    }
  };

  const updateResources = (patch: Partial<ResourceDefaultsConfig>) => {
    if (!editConfig) return;
    setEditConfig({
      ...editConfig,
      resources: { ...editConfig.resources, ...patch },
    });
  };

  const updateEnvironment = (patch: Partial<EnvironmentDefaultsConfig>) => {
    if (!editConfig) return;
    setEditConfig({
      ...editConfig,
      environment: { ...editConfig.environment, ...patch },
    });
  };

  if (loading) {
    return <div className="panel skeleton" style={{ height: 200 }} />;
  }

  return (
    <div
      style={{ display: "flex", flexDirection: "column", gap: "var(--sp-4)" }}
    >
      {error && (
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
          {error}
        </div>
      )}
      {success && (
        <motion.div
          initial={{ opacity: 0, y: -8 }}
          animate={{ opacity: 1, y: 0 }}
          style={{
            padding: "var(--sp-3) var(--sp-4)",
            background: "var(--st-completed-dim)",
            border: "1px solid rgba(52,211,153,0.3)",
            borderRadius: "var(--radius-md)",
            color: "var(--st-completed)",
            fontFamily: "var(--font-mono)",
            fontSize: "0.8rem",
          }}
        >
          <Check size={14} style={{ verticalAlign: -2, marginRight: 6 }} />
          {success}
        </motion.div>
      )}

      {projects.length === 0 && (
        <div className="panel">
          <div
            className="panel-body"
            style={{ textAlign: "center", padding: "var(--sp-8)" }}
          >
            <p
              style={{
                color: "var(--text-muted)",
                fontSize: "0.85rem",
                marginBottom: "var(--sp-2)",
              }}
            >
              No projects found
            </p>
            <p style={{ color: "var(--text-muted)", fontSize: "0.75rem" }}>
              Projects are derived from mounts in the active SSH profile. Add
              mounts in the SSH Profiles tab first.
            </p>
          </div>
        </div>
      )}

      {projects.map((proj) => {
        const isExpanded = expandedMount === proj.mount_name;

        return (
          <div key={proj.mount_name} className="panel">
            <div
              className="panel-header"
              style={{ cursor: "pointer" }}
              onClick={() => handleExpand(proj.mount_name)}
            >
              <div
                style={{
                  display: "flex",
                  alignItems: "center",
                  gap: "var(--sp-3)",
                }}
              >
                <FolderOpen size={14} />
                <h3 style={{ textTransform: "none", letterSpacing: 0 }}>
                  {proj.mount_name}
                </h3>
                <span
                  className={
                    proj.config_exists
                      ? "badge badge-completed"
                      : "badge badge-cancelled"
                  }
                  style={{ fontSize: "0.6rem" }}
                >
                  {proj.config_exists ? "srunx.json" : "NO CONFIG"}
                </span>
              </div>
              <div
                style={{
                  display: "flex",
                  alignItems: "center",
                  gap: "var(--sp-3)",
                }}
              >
                <span
                  style={{
                    fontFamily: "var(--font-mono)",
                    fontSize: "0.7rem",
                    color: "var(--text-muted)",
                  }}
                >
                  {proj.local_path}
                </span>
                {isExpanded ? (
                  <ChevronUp size={14} />
                ) : (
                  <ChevronDown size={14} />
                )}
              </div>
            </div>

            {isExpanded && (
              <div className="panel-body">
                {/* Path info */}
                <div
                  style={{
                    display: "grid",
                    gridTemplateColumns: "1fr 1fr",
                    gap: "var(--sp-3)",
                    marginBottom: "var(--sp-4)",
                  }}
                >
                  <div>
                    <span
                      style={{
                        fontSize: "0.7rem",
                        color: "var(--text-muted)",
                        textTransform: "uppercase",
                        letterSpacing: "0.08em",
                      }}
                    >
                      Local
                    </span>
                    <div
                      style={{
                        fontFamily: "var(--font-mono)",
                        fontSize: "0.8rem",
                        color: "var(--text-secondary)",
                        marginTop: 2,
                      }}
                    >
                      {proj.local_path}
                    </div>
                  </div>
                  <div>
                    <span
                      style={{
                        fontSize: "0.7rem",
                        color: "var(--text-muted)",
                        textTransform: "uppercase",
                        letterSpacing: "0.08em",
                      }}
                    >
                      Remote
                    </span>
                    <div
                      style={{
                        fontFamily: "var(--font-mono)",
                        fontSize: "0.8rem",
                        color: "var(--text-secondary)",
                        marginTop: 2,
                      }}
                    >
                      {proj.remote_path}
                    </div>
                  </div>
                </div>

                {!proj.config_exists && !editConfig ? (
                  <button
                    className="btn btn-primary"
                    onClick={() => handleInit(proj.mount_name)}
                    disabled={saving}
                  >
                    <Plus size={14} />
                    Initialize srunx.json
                  </button>
                ) : editConfig ? (
                  <div>
                    <div
                      style={{
                        display: "flex",
                        justifyContent: "space-between",
                        alignItems: "center",
                        marginBottom: "var(--sp-3)",
                        borderBottom: "1px solid var(--border-ghost)",
                        paddingBottom: "var(--sp-3)",
                      }}
                    >
                      <span
                        style={{
                          fontSize: "0.75rem",
                          fontWeight: 500,
                          textTransform: "uppercase",
                          letterSpacing: "0.08em",
                          color: "var(--text-muted)",
                        }}
                      >
                        Project Config
                      </span>
                      <button
                        className="btn btn-primary"
                        onClick={handleSave}
                        disabled={saving}
                        style={{ fontSize: "0.7rem", padding: "4px 10px" }}
                      >
                        <Save size={12} />
                        {saving ? "Saving..." : "Save"}
                      </button>
                    </div>

                    <FieldRow label="Nodes">
                      <input
                        className="input"
                        type="number"
                        value={editConfig.resources.nodes}
                        min={1}
                        onChange={(e) => {
                          const n = Number(e.target.value);
                          if (!Number.isNaN(n)) updateResources({ nodes: n });
                        }}
                        style={{ width: "100%" }}
                      />
                    </FieldRow>
                    <FieldRow label="GPUs per Node">
                      <input
                        className="input"
                        type="number"
                        value={editConfig.resources.gpus_per_node}
                        min={0}
                        onChange={(e) => {
                          const n = Number(e.target.value);
                          if (!Number.isNaN(n))
                            updateResources({ gpus_per_node: n });
                        }}
                        style={{ width: "100%" }}
                      />
                    </FieldRow>
                    <FieldRow label="Partition">
                      <input
                        className="input"
                        type="text"
                        value={editConfig.resources.partition ?? ""}
                        onChange={(e) =>
                          updateResources({ partition: e.target.value || null })
                        }
                        placeholder="e.g. gpu"
                        style={{ width: "100%" }}
                      />
                    </FieldRow>
                    <FieldRow label="Time Limit">
                      <input
                        className="input"
                        type="text"
                        value={editConfig.resources.time_limit ?? ""}
                        onChange={(e) =>
                          updateResources({
                            time_limit: e.target.value || null,
                          })
                        }
                        placeholder="e.g. 2:00:00"
                        style={{ width: "100%" }}
                      />
                    </FieldRow>
                    <FieldRow label="Memory per Node">
                      <input
                        className="input"
                        type="text"
                        value={editConfig.resources.memory_per_node ?? ""}
                        onChange={(e) =>
                          updateResources({
                            memory_per_node: e.target.value || null,
                          })
                        }
                        placeholder="e.g. 32GB"
                        style={{ width: "100%" }}
                      />
                    </FieldRow>
                    <FieldRow label="Conda">
                      <input
                        className="input"
                        type="text"
                        value={editConfig.environment.conda ?? ""}
                        onChange={(e) =>
                          updateEnvironment({ conda: e.target.value || null })
                        }
                        placeholder="e.g. ml_env"
                        style={{ width: "100%" }}
                      />
                    </FieldRow>
                    <FieldRow label="Log Directory">
                      <input
                        className="input"
                        type="text"
                        value={editConfig.log_dir}
                        onChange={(e) =>
                          setEditConfig({
                            ...editConfig,
                            log_dir: e.target.value || "logs",
                          })
                        }
                        placeholder="logs"
                        style={{ width: "100%" }}
                      />
                    </FieldRow>
                    <FieldRow label="Working Directory">
                      <input
                        className="input"
                        type="text"
                        value={editConfig.work_dir ?? ""}
                        onChange={(e) =>
                          setEditConfig({
                            ...editConfig,
                            work_dir: e.target.value || null,
                          })
                        }
                        placeholder="e.g. /scratch/username"
                        style={{ width: "100%" }}
                      />
                    </FieldRow>
                  </div>
                ) : null}
              </div>
            )}
          </div>
        );
      })}
    </div>
  );
}
