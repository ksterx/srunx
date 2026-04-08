import { useState, useEffect, useCallback } from "react";
import { motion } from "framer-motion";
import {
  Server,
  Plus,
  Trash2,
  Check,
  ChevronDown,
  ChevronUp,
  FolderSync,
  X,
  Edit3,
  Plug,
  FlaskConical,
  Loader2,
} from "lucide-react";
import { config as configApi } from "../../lib/api.ts";
import type {
  SSHProfile,
  SSHProfilesResponse,
  SSHMountConfig,
  SSHTestResult,
  SSHConnectionStatus,
} from "../../lib/types.ts";

type ProfileFormData = {
  name: string;
  hostname: string;
  username: string;
  key_filename: string;
  port: number;
  description: string;
  ssh_host: string;
  proxy_jump: string;
};

const EMPTY_FORM: ProfileFormData = {
  name: "",
  hostname: "",
  username: "",
  key_filename: "",
  port: 22,
  description: "",
  ssh_host: "",
  proxy_jump: "",
};

type TabStatusProps = {
  error: string | null;
  success: string | null;
};

function TabStatus({ error, success }: TabStatusProps) {
  return (
    <>
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
    </>
  );
}

export function SSHProfilesTab() {
  const [data, setData] = useState<SSHProfilesResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [success, setSuccess] = useState<string | null>(null);
  const [showAddForm, setShowAddForm] = useState(false);
  const [form, setForm] = useState<ProfileFormData>(EMPTY_FORM);
  const [expandedProfile, setExpandedProfile] = useState<string | null>(null);
  const [editingProfile, setEditingProfile] = useState<string | null>(null);
  const [editForm, setEditForm] = useState<ProfileFormData>(EMPTY_FORM);
  const [newMount, setNewMount] = useState<SSHMountConfig>({
    name: "",
    local: "",
    remote: "",
  });
  const [newMountExcludes, setNewMountExcludes] = useState("");
  const [newEnvKey, setNewEnvKey] = useState("");
  const [newEnvValue, setNewEnvValue] = useState("");
  const [connStatus, setConnStatus] = useState<SSHConnectionStatus | null>(
    null,
  );
  const [connecting, setConnecting] = useState<string | null>(null);
  const [testing, setTesting] = useState<string | null>(null);
  const [testResults, setTestResults] = useState<Record<string, SSHTestResult>>(
    {},
  );

  const load = useCallback(async () => {
    try {
      setLoading(true);
      const [profiles, status] = await Promise.all([
        configApi.sshProfiles(),
        configApi.sshStatus(),
      ]);
      setData(profiles);
      setConnStatus(status);
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to load profiles");
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

  const handleAdd = async () => {
    if (!form.name || !form.hostname || !form.username || !form.key_filename)
      return;
    try {
      await configApi.addSSHProfile({
        name: form.name,
        hostname: form.hostname,
        username: form.username,
        key_filename: form.key_filename,
        port: form.port,
        description: form.description || undefined,
        ssh_host: form.ssh_host || undefined,
        proxy_jump: form.proxy_jump || undefined,
      });
      setForm(EMPTY_FORM);
      setShowAddForm(false);
      setSuccess(`Profile "${form.name}" added`);
      await load();
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to add profile");
    }
  };

  const handleDelete = async (name: string) => {
    if (!window.confirm(`Delete profile "${name}"?`)) return;
    try {
      await configApi.deleteSSHProfile(name);
      setSuccess(`Profile "${name}" deleted`);
      if (expandedProfile === name) setExpandedProfile(null);
      await load();
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to delete profile");
    }
  };

  const handleConnect = async (name: string) => {
    setConnecting(name);
    setError(null);
    try {
      const res = await configApi.connectSSHProfile(name);
      if (res.connected) {
        setSuccess(`Connected to "${name}" (${res.hostname})`);
      } else {
        setError(`Connection failed: ${res.error}`);
      }
      await load();
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to connect");
    } finally {
      setConnecting(null);
    }
  };

  const handleTest = async (name: string) => {
    setTesting(name);
    setError(null);
    try {
      const result = await configApi.testSSHProfile(name);
      setTestResults((prev) => ({ ...prev, [name]: result }));
      if (result.ssh_connected && result.slurm_available) {
        setSuccess(`Test passed: SSH OK, SLURM ${result.slurm_version}`);
      } else if (result.error) {
        setError(`Test failed: ${result.error}`);
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to test profile");
    } finally {
      setTesting(null);
    }
  };

  const startEdit = (name: string, profile: SSHProfile) => {
    setEditingProfile(name);
    setEditForm({
      name,
      hostname: profile.hostname,
      username: profile.username,
      key_filename: profile.key_filename,
      port: profile.port,
      description: profile.description ?? "",
      ssh_host: profile.ssh_host ?? "",
      proxy_jump: profile.proxy_jump ?? "",
    });
  };

  const handleUpdate = async () => {
    if (!editingProfile) return;
    try {
      await configApi.updateSSHProfile(editingProfile, {
        hostname: editForm.hostname,
        username: editForm.username,
        key_filename: editForm.key_filename,
        port: editForm.port,
        description: editForm.description || null,
        ssh_host: editForm.ssh_host || null,
        proxy_jump: editForm.proxy_jump || null,
      });
      setEditingProfile(null);
      setSuccess(`Profile "${editingProfile}" updated`);
      await load();
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to update profile");
    }
  };

  const handleAddMount = async (profileName: string) => {
    if (!newMount.name || !newMount.local || !newMount.remote) return;
    const excludePatterns = newMountExcludes
      .split(",")
      .map((s) => s.trim())
      .filter(Boolean);
    try {
      await configApi.addSSHMount(profileName, {
        ...newMount,
        exclude_patterns:
          excludePatterns.length > 0 ? excludePatterns : undefined,
      });
      setNewMount({ name: "", local: "", remote: "" });
      setNewMountExcludes("");
      await load();
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to add mount");
    }
  };

  const handleRemoveMount = async (profileName: string, mountName: string) => {
    try {
      await configApi.removeSSHMount(profileName, mountName);
      await load();
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to remove mount");
    }
  };

  const handleAddEnvVar = async (profileName: string) => {
    if (!newEnvKey.trim()) return;
    try {
      const profile = data?.profiles[profileName];
      if (!profile) return;
      const envVars = { ...(profile.env_vars ?? {}), [newEnvKey]: newEnvValue };
      await configApi.updateSSHProfile(profileName, { env_vars: envVars });
      setNewEnvKey("");
      setNewEnvValue("");
      await load();
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to add env var");
    }
  };

  const handleRemoveEnvVar = async (profileName: string, key: string) => {
    try {
      const profile = data?.profiles[profileName];
      if (!profile) return;
      const envVars = { ...(profile.env_vars ?? {}) };
      delete envVars[key];
      await configApi.updateSSHProfile(profileName, { env_vars: envVars });
      await load();
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to remove env var");
    }
  };

  if (loading) {
    return <div className="panel skeleton" style={{ height: 200 }} />;
  }

  const profiles = data ? Object.entries(data.profiles) : [];

  return (
    <div
      style={{ display: "flex", flexDirection: "column", gap: "var(--sp-4)" }}
    >
      <TabStatus error={error} success={success} />

      {/* Profile list */}
      {profiles.map(([name, profile]) => {
        const isActive = data?.current === name;
        const isConnected =
          connStatus?.profile_name === name && connStatus.connected;
        const isExpanded = expandedProfile === name;
        const isEditing = editingProfile === name;
        const testResult = testResults[name];

        return (
          <div key={name} className="panel">
            {/* Header */}
            <div
              className="panel-header"
              style={{ cursor: "pointer" }}
              onClick={() => setExpandedProfile(isExpanded ? null : name)}
            >
              <div
                style={{
                  display: "flex",
                  alignItems: "center",
                  gap: "var(--sp-3)",
                }}
              >
                <Server size={14} />
                <h3 style={{ textTransform: "none", letterSpacing: 0 }}>
                  {name}
                </h3>
                {isConnected && (
                  <span
                    className="badge badge-completed"
                    style={{ fontSize: "0.65rem" }}
                  >
                    CONNECTED
                  </span>
                )}
                {isActive && !isConnected && (
                  <span
                    className="badge"
                    style={{
                      fontSize: "0.65rem",
                      background: "var(--st-pending-dim)",
                      color: "var(--st-pending)",
                    }}
                  >
                    ACTIVE
                  </span>
                )}
                <span
                  style={{
                    fontFamily: "var(--font-mono)",
                    fontSize: "0.75rem",
                    color: "var(--text-muted)",
                  }}
                >
                  {profile.username}@{profile.hostname}:{profile.port}
                </span>
              </div>
              <div
                style={{
                  display: "flex",
                  alignItems: "center",
                  gap: "var(--sp-2)",
                }}
              >
                <button
                  className="btn btn-ghost"
                  onClick={(e) => {
                    e.stopPropagation();
                    handleTest(name);
                  }}
                  disabled={testing === name}
                  style={{ padding: "4px 8px", fontSize: "0.7rem" }}
                  title="Test SSH connection"
                >
                  {testing === name ? (
                    <Loader2 size={12} className="spin" />
                  ) : (
                    <FlaskConical size={12} />
                  )}
                  Test
                </button>
                {!isConnected && (
                  <button
                    className="btn btn-primary"
                    onClick={(e) => {
                      e.stopPropagation();
                      handleConnect(name);
                    }}
                    disabled={connecting === name}
                    style={{ padding: "4px 8px", fontSize: "0.7rem" }}
                    title="Connect to this profile"
                  >
                    {connecting === name ? (
                      <Loader2 size={12} className="spin" />
                    ) : (
                      <Plug size={12} />
                    )}
                    Connect
                  </button>
                )}
                <button
                  className="btn btn-ghost"
                  onClick={(e) => {
                    e.stopPropagation();
                    if (isEditing) {
                      setEditingProfile(null);
                    } else {
                      startEdit(name, profile);
                      setExpandedProfile(name);
                    }
                  }}
                  style={{ padding: "4px 8px" }}
                >
                  <Edit3 size={12} />
                </button>
                <button
                  className="btn btn-danger"
                  onClick={(e) => {
                    e.stopPropagation();
                    handleDelete(name);
                  }}
                  style={{ padding: "4px 8px" }}
                >
                  <Trash2 size={12} />
                </button>
                {isExpanded ? (
                  <ChevronUp size={14} />
                ) : (
                  <ChevronDown size={14} />
                )}
              </div>
            </div>

            {/* Expanded content */}
            {isExpanded && (
              <div className="panel-body">
                {/* Test result */}
                {testResult && (
                  <div
                    style={{
                      padding: "var(--sp-3) var(--sp-4)",
                      background:
                        testResult.ssh_connected && testResult.slurm_available
                          ? "var(--st-completed-dim)"
                          : "var(--st-failed-dim)",
                      border: `1px solid ${
                        testResult.ssh_connected && testResult.slurm_available
                          ? "rgba(52,211,153,0.3)"
                          : "rgba(244,63,94,0.3)"
                      }`,
                      borderRadius: "var(--radius-md)",
                      marginBottom: "var(--sp-4)",
                      fontFamily: "var(--font-mono)",
                      fontSize: "0.8rem",
                      display: "flex",
                      justifyContent: "space-between",
                      alignItems: "center",
                    }}
                  >
                    <div>
                      <span
                        style={{
                          color: testResult.ssh_connected
                            ? "var(--st-completed)"
                            : "var(--st-failed)",
                        }}
                      >
                        SSH: {testResult.ssh_connected ? "OK" : "FAIL"}
                      </span>
                      {" / "}
                      <span
                        style={{
                          color: testResult.slurm_available
                            ? "var(--st-completed)"
                            : "var(--st-failed)",
                        }}
                      >
                        SLURM:{" "}
                        {testResult.slurm_available
                          ? testResult.slurm_version || "OK"
                          : "FAIL"}
                      </span>
                      {testResult.user && (
                        <span
                          style={{ color: "var(--text-muted)", marginLeft: 8 }}
                        >
                          ({testResult.user}@{testResult.hostname})
                        </span>
                      )}
                      {testResult.error && (
                        <span
                          style={{ color: "var(--st-failed)", marginLeft: 8 }}
                        >
                          {testResult.error}
                        </span>
                      )}
                    </div>
                    <button
                      onClick={() =>
                        setTestResults((prev) => {
                          const next = { ...prev };
                          delete next[name];
                          return next;
                        })
                      }
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
                )}

                {/* Edit form */}
                {isEditing ? (
                  <div
                    style={{
                      display: "grid",
                      gridTemplateColumns: "1fr 1fr",
                      gap: "var(--sp-3)",
                      marginBottom: "var(--sp-4)",
                    }}
                  >
                    {(
                      [
                        ["hostname", "Hostname"],
                        ["username", "Username"],
                        ["key_filename", "Key File"],
                        ["port", "Port"],
                        ["description", "Description"],
                        ["ssh_host", "SSH Host"],
                        ["proxy_jump", "ProxyJump"],
                      ] as const
                    ).map(([field, label]) => (
                      <div key={field}>
                        <label
                          style={{
                            fontSize: "0.7rem",
                            color: "var(--text-muted)",
                            textTransform: "uppercase",
                            letterSpacing: "0.08em",
                          }}
                        >
                          {label}
                        </label>
                        <input
                          className="input"
                          type={field === "port" ? "number" : "text"}
                          value={editForm[field]}
                          onChange={(e) =>
                            setEditForm({
                              ...editForm,
                              [field]:
                                field === "port"
                                  ? Number(e.target.value)
                                  : e.target.value,
                            })
                          }
                          style={{ width: "100%", marginTop: 4 }}
                        />
                      </div>
                    ))}
                    <div
                      style={{
                        gridColumn: "1 / -1",
                        display: "flex",
                        gap: "var(--sp-2)",
                        justifyContent: "flex-end",
                      }}
                    >
                      <button
                        className="btn btn-ghost"
                        onClick={() => setEditingProfile(null)}
                      >
                        Cancel
                      </button>
                      <button
                        className="btn btn-primary"
                        onClick={handleUpdate}
                      >
                        Save Changes
                      </button>
                    </div>
                  </div>
                ) : (
                  <div
                    style={{
                      display: "grid",
                      gridTemplateColumns: "1fr 1fr",
                      gap: "var(--sp-2)",
                      marginBottom: "var(--sp-4)",
                    }}
                  >
                    {[
                      ["Key File", profile.key_filename],
                      ["Description", profile.description],
                      ["SSH Host", profile.ssh_host],
                      ["ProxyJump", profile.proxy_jump],
                    ]
                      .filter(([, v]) => v)
                      .map(([label, value]) => (
                        <div key={label}>
                          <span
                            style={{
                              fontSize: "0.7rem",
                              color: "var(--text-muted)",
                              textTransform: "uppercase",
                              letterSpacing: "0.08em",
                            }}
                          >
                            {label}
                          </span>
                          <div
                            style={{
                              fontFamily: "var(--font-mono)",
                              fontSize: "0.8rem",
                              color: "var(--text-secondary)",
                              marginTop: 2,
                            }}
                          >
                            {value}
                          </div>
                        </div>
                      ))}
                  </div>
                )}

                {/* Mounts */}
                <div
                  style={{
                    borderTop: "1px solid var(--border-ghost)",
                    paddingTop: "var(--sp-4)",
                  }}
                >
                  <div
                    style={{
                      display: "flex",
                      alignItems: "center",
                      gap: "var(--sp-2)",
                      marginBottom: "var(--sp-3)",
                    }}
                  >
                    <FolderSync size={14} color="var(--text-muted)" />
                    <span
                      style={{
                        fontSize: "0.75rem",
                        fontWeight: 500,
                        textTransform: "uppercase",
                        letterSpacing: "0.08em",
                        color: "var(--text-muted)",
                      }}
                    >
                      Mounts
                    </span>
                  </div>
                  {profile.mounts.map((m) => (
                    <div
                      key={m.name}
                      style={{
                        padding: "var(--sp-2) var(--sp-3)",
                        background: "var(--bg-base)",
                        borderRadius: "var(--radius-md)",
                        border: "1px solid var(--border-ghost)",
                        marginBottom: "var(--sp-2)",
                      }}
                    >
                      <div
                        style={{
                          display: "flex",
                          alignItems: "center",
                          gap: "var(--sp-2)",
                        }}
                      >
                        <span
                          style={{
                            fontFamily: "var(--font-mono)",
                            fontSize: "0.8rem",
                            color: "var(--accent)",
                            minWidth: 80,
                          }}
                        >
                          {m.name}
                        </span>
                        <span
                          style={{
                            fontFamily: "var(--font-mono)",
                            fontSize: "0.75rem",
                            color: "var(--text-secondary)",
                            flex: 1,
                          }}
                        >
                          {m.local} &rarr; {m.remote}
                        </span>
                        <button
                          onClick={() => handleRemoveMount(name, m.name)}
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
                      {m.exclude_patterns && m.exclude_patterns.length > 0 && (
                        <div
                          style={{
                            marginTop: "var(--sp-1)",
                            fontFamily: "var(--font-mono)",
                            fontSize: "0.7rem",
                            color: "var(--text-muted)",
                            paddingLeft: 80,
                          }}
                        >
                          exclude: {m.exclude_patterns.join(", ")}
                        </div>
                      )}
                    </div>
                  ))}
                  <div
                    style={{
                      display: "flex",
                      flexDirection: "column",
                      gap: "var(--sp-2)",
                    }}
                  >
                    <div
                      style={{
                        display: "flex",
                        gap: "var(--sp-2)",
                        alignItems: "center",
                      }}
                    >
                      <input
                        className="input"
                        placeholder="name"
                        value={newMount.name}
                        onChange={(e) =>
                          setNewMount({ ...newMount, name: e.target.value })
                        }
                        style={{
                          width: 100,
                          fontFamily: "var(--font-mono)",
                          fontSize: "0.8rem",
                        }}
                      />
                      <input
                        className="input"
                        placeholder="local path"
                        value={newMount.local}
                        onChange={(e) =>
                          setNewMount({ ...newMount, local: e.target.value })
                        }
                        style={{
                          flex: 1,
                          fontFamily: "var(--font-mono)",
                          fontSize: "0.8rem",
                        }}
                      />
                      <input
                        className="input"
                        placeholder="remote path"
                        value={newMount.remote}
                        onChange={(e) =>
                          setNewMount({ ...newMount, remote: e.target.value })
                        }
                        style={{
                          flex: 1,
                          fontFamily: "var(--font-mono)",
                          fontSize: "0.8rem",
                        }}
                      />
                      <button
                        className="btn btn-primary"
                        onClick={() => handleAddMount(name)}
                        disabled={
                          !newMount.name || !newMount.local || !newMount.remote
                        }
                        style={{
                          padding: "var(--sp-2) var(--sp-3)",
                          fontSize: "0.75rem",
                          gap: 4,
                        }}
                      >
                        <Plus size={14} />
                        Add
                      </button>
                    </div>
                    <input
                      className="input"
                      placeholder="exclude patterns (comma-separated, e.g. data/,logs/,*.bin)"
                      value={newMountExcludes}
                      onChange={(e) => setNewMountExcludes(e.target.value)}
                      style={{
                        fontFamily: "var(--font-mono)",
                        fontSize: "0.8rem",
                      }}
                    />
                  </div>
                </div>

                {/* Profile Env Vars */}
                <div
                  style={{
                    borderTop: "1px solid var(--border-ghost)",
                    paddingTop: "var(--sp-4)",
                    marginTop: "var(--sp-4)",
                  }}
                >
                  <span
                    style={{
                      fontSize: "0.75rem",
                      fontWeight: 500,
                      textTransform: "uppercase",
                      letterSpacing: "0.08em",
                      color: "var(--text-muted)",
                      marginBottom: "var(--sp-3)",
                      display: "block",
                    }}
                  >
                    Environment Variables
                  </span>
                  {Object.entries(profile.env_vars ?? {}).map(([k, v]) => (
                    <div
                      key={k}
                      style={{
                        display: "flex",
                        alignItems: "center",
                        gap: "var(--sp-2)",
                        padding: "var(--sp-2) var(--sp-3)",
                        background: "var(--bg-base)",
                        borderRadius: "var(--radius-md)",
                        border: "1px solid var(--border-ghost)",
                        marginBottom: "var(--sp-2)",
                      }}
                    >
                      <span
                        style={{
                          fontFamily: "var(--font-mono)",
                          fontSize: "0.8rem",
                          color: "var(--accent)",
                          minWidth: 100,
                        }}
                      >
                        {k}
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
                        {v}
                      </span>
                      <button
                        onClick={() => handleRemoveEnvVar(name, k)}
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
                  ))}
                  <div
                    style={{
                      display: "flex",
                      gap: "var(--sp-2)",
                      alignItems: "center",
                    }}
                  >
                    <input
                      className="input"
                      placeholder="KEY"
                      value={newEnvKey}
                      onChange={(e) => setNewEnvKey(e.target.value)}
                      style={{
                        width: 120,
                        fontFamily: "var(--font-mono)",
                        fontSize: "0.8rem",
                      }}
                      onKeyDown={(e) =>
                        e.key === "Enter" && handleAddEnvVar(name)
                      }
                    />
                    <span
                      style={{ color: "var(--text-muted)", fontSize: "0.8rem" }}
                    >
                      =
                    </span>
                    <input
                      className="input"
                      placeholder="value"
                      value={newEnvValue}
                      onChange={(e) => setNewEnvValue(e.target.value)}
                      style={{
                        flex: 1,
                        fontFamily: "var(--font-mono)",
                        fontSize: "0.8rem",
                      }}
                      onKeyDown={(e) =>
                        e.key === "Enter" && handleAddEnvVar(name)
                      }
                    />
                    <button
                      className="btn btn-ghost"
                      onClick={() => handleAddEnvVar(name)}
                      disabled={!newEnvKey.trim()}
                      style={{ padding: "var(--sp-2)" }}
                    >
                      <Plus size={14} />
                    </button>
                  </div>
                </div>
              </div>
            )}
          </div>
        );
      })}

      {profiles.length === 0 && !showAddForm && (
        <div
          style={{
            textAlign: "center",
            padding: "var(--sp-8)",
            color: "var(--text-muted)",
            fontSize: "0.85rem",
          }}
        >
          No SSH profiles configured
        </div>
      )}

      {/* Add profile form */}
      {showAddForm ? (
        <div className="panel">
          <div className="panel-header">
            <h3>New SSH Profile</h3>
          </div>
          <div className="panel-body">
            <div
              style={{
                display: "grid",
                gridTemplateColumns: "1fr 1fr",
                gap: "var(--sp-3)",
              }}
            >
              {(
                [
                  ["name", "Profile Name", "text", true],
                  ["hostname", "Hostname", "text", true],
                  ["username", "Username", "text", true],
                  ["key_filename", "Key File", "text", true],
                  ["port", "Port", "number", false],
                  ["description", "Description", "text", false],
                  ["ssh_host", "SSH Host", "text", false],
                  ["proxy_jump", "ProxyJump", "text", false],
                ] as const
              ).map(([field, label, type, required]) => (
                <div key={field}>
                  <label
                    style={{
                      fontSize: "0.7rem",
                      color: "var(--text-muted)",
                      textTransform: "uppercase",
                      letterSpacing: "0.08em",
                    }}
                  >
                    {label}
                    {required && (
                      <span style={{ color: "var(--st-failed)" }}> *</span>
                    )}
                  </label>
                  <input
                    className="input"
                    type={type}
                    value={form[field]}
                    onChange={(e) =>
                      setForm({
                        ...form,
                        [field]:
                          type === "number"
                            ? Number(e.target.value)
                            : e.target.value,
                      })
                    }
                    style={{ width: "100%", marginTop: 4 }}
                  />
                </div>
              ))}
            </div>
            <div
              style={{
                display: "flex",
                gap: "var(--sp-2)",
                justifyContent: "flex-end",
                marginTop: "var(--sp-4)",
              }}
            >
              <button
                className="btn btn-ghost"
                onClick={() => {
                  setShowAddForm(false);
                  setForm(EMPTY_FORM);
                }}
              >
                Cancel
              </button>
              <button
                className="btn btn-primary"
                onClick={handleAdd}
                disabled={
                  !form.name ||
                  !form.hostname ||
                  !form.username ||
                  !form.key_filename
                }
              >
                <Plus size={14} />
                Add Profile
              </button>
            </div>
          </div>
        </div>
      ) : (
        <button
          className="btn btn-ghost"
          onClick={() => setShowAddForm(true)}
          style={{ alignSelf: "flex-start" }}
        >
          <Plus size={14} />
          Add Profile
        </button>
      )}
    </div>
  );
}
