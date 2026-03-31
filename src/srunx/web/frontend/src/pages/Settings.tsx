import { useState, useEffect, useCallback } from "react";
import { motion } from "framer-motion";
import {
  Save,
  RotateCcw,
  FolderOpen,
  Check,
  AlertTriangle,
  Plus,
  X,
  Server,
  Bell,
  Variable,
  Sliders,
} from "lucide-react";
import { config as configApi } from "../lib/api.ts";
import type {
  SrunxConfig,
  ConfigPathInfo,
  ResourceDefaultsConfig,
  EnvironmentDefaultsConfig,
} from "../lib/types.ts";
import { SSHProfilesTab } from "./settings/SSHProfilesTab.tsx";
import { NotificationsTab } from "./settings/NotificationsTab.tsx";
import { EnvironmentTab } from "./settings/EnvironmentTab.tsx";
import { ProjectTab } from "./settings/ProjectTab.tsx";

const EASE = [0.16, 1, 0.3, 1] as [number, number, number, number];

const stagger = (i: number) => ({
  initial: { opacity: 0, y: 12 },
  animate: { opacity: 1, y: 0 },
  transition: { delay: i * 0.06, duration: 0.4, ease: EASE },
});

type FieldRowProps = {
  label: string;
  description?: string;
  children: React.ReactNode;
};

function FieldRow({ label, description, children }: FieldRowProps) {
  return (
    <div
      style={{
        display: "flex",
        alignItems: "flex-start",
        justifyContent: "space-between",
        gap: "var(--sp-6)",
        padding: "var(--sp-3) 0",
        borderBottom: "1px solid var(--border-ghost)",
      }}
    >
      <div style={{ flex: 1, minWidth: 0 }}>
        <div
          style={{
            fontSize: "0.85rem",
            fontWeight: 500,
            color: "var(--text-primary)",
          }}
        >
          {label}
        </div>
        {description && (
          <div
            style={{
              fontSize: "0.75rem",
              color: "var(--text-muted)",
              marginTop: 2,
            }}
          >
            {description}
          </div>
        )}
      </div>
      <div style={{ flexShrink: 0, minWidth: 200 }}>{children}</div>
    </div>
  );
}

function NumberInput({
  value,
  onChange,
  min,
  placeholder,
}: {
  value: number;
  onChange: (v: number) => void;
  min?: number;
  placeholder?: string;
}) {
  return (
    <input
      className="input"
      type="number"
      value={value}
      min={min}
      placeholder={placeholder}
      onChange={(e) => {
        const n = Number(e.target.value);
        if (!Number.isNaN(n)) onChange(n);
      }}
      style={{ width: "100%" }}
    />
  );
}

function TextInput({
  value,
  onChange,
  placeholder,
}: {
  value: string;
  onChange: (v: string) => void;
  placeholder?: string;
}) {
  return (
    <input
      className="input"
      type="text"
      value={value}
      placeholder={placeholder}
      onChange={(e) => onChange(e.target.value)}
      style={{ width: "100%" }}
    />
  );
}

/* ── Tab definitions ─────────────────────────── */

type TabId = "general" | "ssh" | "notifications" | "environment" | "project";

type TabDef = {
  id: TabId;
  label: string;
  icon: React.ReactNode;
};

const TABS: TabDef[] = [
  { id: "general", label: "General", icon: <Sliders size={14} /> },
  { id: "ssh", label: "SSH Profiles", icon: <Server size={14} /> },
  { id: "notifications", label: "Notifications", icon: <Bell size={14} /> },
  { id: "environment", label: "Environment", icon: <Variable size={14} /> },
  { id: "project", label: "Project", icon: <FolderOpen size={14} /> },
];

/* ── Main Settings component ─────────────────── */

export function Settings() {
  const [activeTab, setActiveTab] = useState<TabId>("general");
  const [config, setConfig] = useState<SrunxConfig | null>(null);
  const [paths, setPaths] = useState<ConfigPathInfo[]>([]);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [success, setSuccess] = useState<string | null>(null);

  // env_vars editor state
  const [newEnvKey, setNewEnvKey] = useState("");
  const [newEnvValue, setNewEnvValue] = useState("");

  const loadConfig = useCallback(async () => {
    try {
      setLoading(true);
      const [cfg, p] = await Promise.all([configApi.get(), configApi.paths()]);
      setConfig(cfg);
      setPaths(p);
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to load config");
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    loadConfig();
  }, [loadConfig]);

  useEffect(() => {
    if (success) {
      const timer = setTimeout(() => setSuccess(null), 3000);
      return () => clearTimeout(timer);
    }
  }, [success]);

  const handleSave = async () => {
    if (!config) return;
    try {
      setSaving(true);
      setError(null);
      const updated = await configApi.update(config);
      setConfig(updated);
      setSuccess("Configuration saved successfully");
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to save config");
    } finally {
      setSaving(false);
    }
  };

  const handleReset = async () => {
    if (
      !window.confirm("Reset all settings to defaults? This cannot be undone.")
    )
      return;
    try {
      setSaving(true);
      setError(null);
      const updated = await configApi.reset();
      setConfig(updated);
      setSuccess("Configuration reset to defaults");
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to reset config");
    } finally {
      setSaving(false);
    }
  };

  const updateResources = (patch: Partial<ResourceDefaultsConfig>) => {
    if (!config) return;
    setConfig({
      ...config,
      resources: { ...config.resources, ...patch },
    });
  };

  const updateEnvironment = (patch: Partial<EnvironmentDefaultsConfig>) => {
    if (!config) return;
    setConfig({
      ...config,
      environment: { ...config.environment, ...patch },
    });
  };

  const addEnvVar = () => {
    if (!config || !newEnvKey.trim()) return;
    updateEnvironment({
      env_vars: { ...config.environment.env_vars, [newEnvKey]: newEnvValue },
    });
    setNewEnvKey("");
    setNewEnvValue("");
  };

  const removeEnvVar = (key: string) => {
    if (!config) return;
    const next = { ...config.environment.env_vars };
    delete next[key];
    updateEnvironment({ env_vars: next });
  };

  return (
    <div
      style={{ display: "flex", flexDirection: "column", gap: "var(--sp-6)" }}
    >
      {/* Header */}
      <motion.div {...stagger(0)}>
        <h1 style={{ marginBottom: 4 }}>Settings</h1>
        <p style={{ color: "var(--text-muted)", fontSize: "0.85rem" }}>
          Manage srunx configuration, SSH profiles, and notifications
        </p>
      </motion.div>

      {/* Tab bar */}
      <motion.div
        {...stagger(1)}
        style={{
          display: "flex",
          gap: 2,
          borderBottom: "1px solid var(--border-subtle)",
          paddingBottom: 0,
        }}
      >
        {TABS.map((tab) => (
          <button
            key={tab.id}
            onClick={() => setActiveTab(tab.id)}
            style={{
              display: "flex",
              alignItems: "center",
              gap: "var(--sp-2)",
              padding: "var(--sp-3) var(--sp-4)",
              background: "transparent",
              border: "none",
              borderBottom:
                activeTab === tab.id
                  ? "2px solid var(--accent)"
                  : "2px solid transparent",
              color:
                activeTab === tab.id
                  ? "var(--text-primary)"
                  : "var(--text-muted)",
              fontFamily: "var(--font-display)",
              fontSize: "0.8rem",
              fontWeight: 500,
              letterSpacing: "0.04em",
              cursor: "pointer",
              transition: "all 150ms var(--ease-out)",
            }}
          >
            {tab.icon}
            {tab.label}
          </button>
        ))}
      </motion.div>

      {/* Tab content */}
      {activeTab === "general" && (
        <>
          {/* General tab header actions */}
          <div
            style={{
              display: "flex",
              justifyContent: "flex-end",
              gap: "var(--sp-2)",
            }}
          >
            <button
              className="btn btn-ghost"
              onClick={handleReset}
              disabled={saving}
            >
              <RotateCcw size={14} />
              Reset
            </button>
            <button
              className="btn btn-primary"
              onClick={handleSave}
              disabled={saving}
            >
              <Save size={14} />
              {saving ? "Saving..." : "Save"}
            </button>
          </div>

          {/* Status messages */}
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
                display: "flex",
                alignItems: "center",
                gap: "var(--sp-2)",
              }}
            >
              <AlertTriangle size={14} />
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
                display: "flex",
                alignItems: "center",
                gap: "var(--sp-2)",
              }}
            >
              <Check size={14} />
              {success}
            </motion.div>
          )}

          {loading ? (
            <div className="panel skeleton" style={{ height: 400 }} />
          ) : config ? (
            <>
              {/* Resource Defaults */}
              <motion.div {...stagger(2)} className="panel">
                <div className="panel-header">
                  <h3>Resource Defaults</h3>
                </div>
                <div className="panel-body">
                  <FieldRow
                    label="Nodes"
                    description="Default number of compute nodes"
                  >
                    <NumberInput
                      value={config.resources.nodes}
                      onChange={(v) => updateResources({ nodes: v })}
                      min={1}
                    />
                  </FieldRow>
                  <FieldRow
                    label="GPUs per Node"
                    description="Default GPU allocation per node"
                  >
                    <NumberInput
                      value={config.resources.gpus_per_node}
                      onChange={(v) => updateResources({ gpus_per_node: v })}
                      min={0}
                    />
                  </FieldRow>
                  <FieldRow
                    label="Tasks per Node"
                    description="Default number of tasks per node"
                  >
                    <NumberInput
                      value={config.resources.ntasks_per_node}
                      onChange={(v) => updateResources({ ntasks_per_node: v })}
                      min={1}
                    />
                  </FieldRow>
                  <FieldRow
                    label="CPUs per Task"
                    description="Default CPU cores per task"
                  >
                    <NumberInput
                      value={config.resources.cpus_per_task}
                      onChange={(v) => updateResources({ cpus_per_task: v })}
                      min={1}
                    />
                  </FieldRow>
                  <FieldRow
                    label="Memory per Node"
                    description="e.g. 32GB, 128GB"
                  >
                    <TextInput
                      value={config.resources.memory_per_node ?? ""}
                      onChange={(v) =>
                        updateResources({ memory_per_node: v || null })
                      }
                      placeholder="e.g. 32GB"
                    />
                  </FieldRow>
                  <FieldRow
                    label="Time Limit"
                    description="e.g. 2:00:00, 1-00:00:00"
                  >
                    <TextInput
                      value={config.resources.time_limit ?? ""}
                      onChange={(v) =>
                        updateResources({ time_limit: v || null })
                      }
                      placeholder="e.g. 2:00:00"
                    />
                  </FieldRow>
                  <FieldRow
                    label="Partition"
                    description="Default SLURM partition"
                  >
                    <TextInput
                      value={config.resources.partition ?? ""}
                      onChange={(v) =>
                        updateResources({ partition: v || null })
                      }
                      placeholder="e.g. gpu"
                    />
                  </FieldRow>
                  <FieldRow
                    label="Nodelist"
                    description="Specific nodes to target"
                  >
                    <TextInput
                      value={config.resources.nodelist ?? ""}
                      onChange={(v) => updateResources({ nodelist: v || null })}
                      placeholder="e.g. node[01-04]"
                    />
                  </FieldRow>
                </div>
              </motion.div>

              {/* Environment Defaults */}
              <motion.div {...stagger(3)} className="panel">
                <div className="panel-header">
                  <h3>Environment Defaults</h3>
                </div>
                <div className="panel-body">
                  <FieldRow
                    label="Conda Environment"
                    description="Default conda env to activate"
                  >
                    <TextInput
                      value={config.environment.conda ?? ""}
                      onChange={(v) => updateEnvironment({ conda: v || null })}
                      placeholder="e.g. ml_env"
                    />
                  </FieldRow>
                  <FieldRow
                    label="Virtual Environment"
                    description="Path to venv to activate"
                  >
                    <TextInput
                      value={config.environment.venv ?? ""}
                      onChange={(v) => updateEnvironment({ venv: v || null })}
                      placeholder="e.g. /path/to/venv"
                    />
                  </FieldRow>

                  {/* Environment Variables */}
                  <div style={{ paddingTop: "var(--sp-4)" }}>
                    <div
                      style={{
                        fontSize: "0.85rem",
                        fontWeight: 500,
                        color: "var(--text-primary)",
                        marginBottom: "var(--sp-3)",
                      }}
                    >
                      Environment Variables
                    </div>

                    {Object.entries(config.environment.env_vars).length > 0 && (
                      <div
                        style={{
                          display: "flex",
                          flexDirection: "column",
                          gap: "var(--sp-2)",
                          marginBottom: "var(--sp-3)",
                        }}
                      >
                        {Object.entries(config.environment.env_vars).map(
                          ([key, value]) => (
                            <div
                              key={key}
                              style={{
                                display: "flex",
                                alignItems: "center",
                                gap: "var(--sp-2)",
                                padding: "var(--sp-2) var(--sp-3)",
                                background: "var(--bg-base)",
                                borderRadius: "var(--radius-md)",
                                border: "1px solid var(--border-ghost)",
                              }}
                            >
                              <span
                                style={{
                                  fontFamily: "var(--font-mono)",
                                  fontSize: "0.8rem",
                                  color: "var(--accent)",
                                  minWidth: 120,
                                }}
                              >
                                {key}
                              </span>
                              <span
                                style={{
                                  color: "var(--text-muted)",
                                  fontSize: "0.8rem",
                                }}
                              >
                                =
                              </span>
                              <span
                                style={{
                                  fontFamily: "var(--font-mono)",
                                  fontSize: "0.8rem",
                                  color: "var(--text-secondary)",
                                  flex: 1,
                                }}
                              >
                                {value}
                              </span>
                              <button
                                onClick={() => removeEnvVar(key)}
                                style={{
                                  background: "transparent",
                                  border: "none",
                                  color: "var(--text-muted)",
                                  cursor: "pointer",
                                  padding: 4,
                                  display: "flex",
                                }}
                              >
                                <X size={14} />
                              </button>
                            </div>
                          ),
                        )}
                      </div>
                    )}

                    <div
                      style={{
                        display: "flex",
                        gap: "var(--sp-2)",
                        alignItems: "center",
                      }}
                    >
                      <input
                        className="input"
                        type="text"
                        value={newEnvKey}
                        onChange={(e) => setNewEnvKey(e.target.value)}
                        placeholder="KEY"
                        style={{
                          width: 140,
                          fontFamily: "var(--font-mono)",
                          fontSize: "0.8rem",
                        }}
                        onKeyDown={(e) => e.key === "Enter" && addEnvVar()}
                      />
                      <span
                        style={{
                          color: "var(--text-muted)",
                          fontSize: "0.8rem",
                        }}
                      >
                        =
                      </span>
                      <input
                        className="input"
                        type="text"
                        value={newEnvValue}
                        onChange={(e) => setNewEnvValue(e.target.value)}
                        placeholder="value"
                        style={{
                          flex: 1,
                          fontFamily: "var(--font-mono)",
                          fontSize: "0.8rem",
                        }}
                        onKeyDown={(e) => e.key === "Enter" && addEnvVar()}
                      />
                      <button
                        className="btn btn-ghost"
                        onClick={addEnvVar}
                        disabled={!newEnvKey.trim()}
                        style={{ padding: "var(--sp-2)" }}
                      >
                        <Plus size={14} />
                      </button>
                    </div>
                  </div>
                </div>
              </motion.div>

              {/* General */}
              <motion.div {...stagger(4)} className="panel">
                <div className="panel-header">
                  <h3>General</h3>
                </div>
                <div className="panel-body">
                  <FieldRow
                    label="Log Directory"
                    description="Default directory for SLURM log output"
                  >
                    <TextInput
                      value={config.log_dir}
                      onChange={(v) =>
                        setConfig({ ...config, log_dir: v || "logs" })
                      }
                      placeholder="logs"
                    />
                  </FieldRow>
                  <FieldRow
                    label="Working Directory"
                    description="Default working directory for jobs"
                  >
                    <TextInput
                      value={config.work_dir ?? ""}
                      onChange={(v) =>
                        setConfig({ ...config, work_dir: v || null })
                      }
                      placeholder="e.g. /scratch/username"
                    />
                  </FieldRow>
                </div>
              </motion.div>

              {/* Config File Paths */}
              <motion.div {...stagger(5)} className="panel">
                <div className="panel-header">
                  <h3>
                    <FolderOpen
                      size={14}
                      style={{ marginRight: 8, verticalAlign: -2 }}
                    />
                    Config File Paths
                  </h3>
                </div>
                <div className="panel-body">
                  <div
                    style={{
                      fontSize: "0.75rem",
                      color: "var(--text-muted)",
                      marginBottom: "var(--sp-3)",
                    }}
                  >
                    Files are loaded in order of precedence (lowest to highest).
                    Environment variables override all files.
                  </div>
                  <div
                    style={{
                      display: "flex",
                      flexDirection: "column",
                      gap: "var(--sp-2)",
                    }}
                  >
                    {paths.map((p, i) => (
                      <div
                        key={i}
                        style={{
                          display: "flex",
                          alignItems: "center",
                          gap: "var(--sp-3)",
                          padding: "var(--sp-2) var(--sp-3)",
                          background: "var(--bg-base)",
                          borderRadius: "var(--radius-md)",
                          border: "1px solid var(--border-ghost)",
                        }}
                      >
                        <span
                          style={{
                            fontSize: "0.7rem",
                            fontFamily: "var(--font-display)",
                            textTransform: "uppercase",
                            letterSpacing: "0.08em",
                            color: "var(--text-muted)",
                            minWidth: 100,
                          }}
                        >
                          {p.source}
                        </span>
                        <span
                          style={{
                            fontFamily: "var(--font-mono)",
                            fontSize: "0.8rem",
                            color: "var(--text-secondary)",
                            flex: 1,
                          }}
                        >
                          {p.path}
                        </span>
                        <span
                          style={{
                            fontFamily: "var(--font-mono)",
                            fontSize: "0.7rem",
                            color: p.exists
                              ? "var(--st-completed)"
                              : "var(--text-muted)",
                          }}
                        >
                          {p.exists ? "FOUND" : "NOT FOUND"}
                        </span>
                      </div>
                    ))}
                  </div>
                </div>
              </motion.div>
            </>
          ) : (
            <div
              style={{
                padding: "var(--sp-4)",
                color: "var(--st-failed)",
                fontFamily: "var(--font-mono)",
              }}
            >
              {error ?? "Unable to load configuration"}
            </div>
          )}
        </>
      )}

      {activeTab === "ssh" && <SSHProfilesTab />}
      {activeTab === "notifications" && <NotificationsTab />}
      {activeTab === "environment" && <EnvironmentTab />}
      {activeTab === "project" && <ProjectTab />}
    </div>
  );
}
