import { useCallback, useEffect, useMemo, useState } from "react";
import { motion } from "framer-motion";
import { FolderOpen, Save, Undo2, Eraser } from "lucide-react";
import { config as configApi, ConfigConflictError } from "../../lib/api.ts";
import { useSelection } from "../../lib/selection.ts";
import type {
  ConfigFile,
  ConfigPathInfo,
  ConfigScope,
  EffectiveConfig,
} from "../../lib/types.ts";
import { ErrorBanner } from "../../components/ErrorBanner.tsx";
import { FieldRow, SuccessBanner, getIn, monoSmall } from "./shared.tsx";

const EASE = [0.16, 1, 0.3, 1] as [number, number, number, number];

const stagger = (i: number) => ({
  initial: { opacity: 0, y: 12 },
  animate: { opacity: 1, y: 0 },
  transition: { delay: i * 0.06, duration: 0.4, ease: EASE },
});

/* ── Editable scopes ─────────────────────────── */

type ScopeId = "user" | "user-cluster" | "project" | "project-cluster";

type ScopeDef = {
  id: ScopeId;
  label: string;
  file: ConfigScope;
  perCluster: boolean;
};

const SCOPES: ScopeDef[] = [
  { id: "user", label: "User (all clusters)", file: "user", perCluster: false },
  {
    id: "user-cluster",
    label: "User · this cluster",
    file: "user",
    perCluster: true,
  },
  { id: "project", label: "Project", file: "project", perCluster: false },
  {
    id: "project-cluster",
    label: "Project · this cluster",
    file: "project",
    perCluster: true,
  },
];

/* ── Editable fields ─────────────────────────── */

type FieldKind = "int" | "str";

type FieldDef = {
  key: string;
  label: string;
  description: string;
  kind: FieldKind;
  placeholder?: string;
};

const SECTIONS: { title: string; fields: FieldDef[] }[] = [
  {
    title: "Resource Defaults",
    fields: [
      {
        key: "resources.nodes",
        label: "Nodes",
        description: "Default number of compute nodes",
        kind: "int",
      },
      {
        key: "resources.gpus_per_node",
        label: "GPUs per Node",
        description: "Default GPU allocation per node",
        kind: "int",
      },
      {
        key: "resources.ntasks_per_node",
        label: "Tasks per Node",
        description: "Default number of tasks per node",
        kind: "int",
      },
      {
        key: "resources.cpus_per_task",
        label: "CPUs per Task",
        description: "Default CPU cores per task",
        kind: "int",
      },
      {
        key: "resources.memory_per_node",
        label: "Memory per Node",
        description: "e.g. 32GB, 128GB",
        kind: "str",
        placeholder: "e.g. 32GB",
      },
      {
        key: "resources.time_limit",
        label: "Time Limit",
        description: "e.g. 2:00:00, 1-00:00:00",
        kind: "str",
        placeholder: "e.g. 2:00:00",
      },
      {
        key: "resources.partition",
        label: "Partition",
        description: "Default SLURM partition",
        kind: "str",
        placeholder: "e.g. gpu",
      },
      {
        key: "resources.nodelist",
        label: "Nodelist",
        description: "Specific nodes to target",
        kind: "str",
        placeholder: "e.g. node[01-04]",
      },
    ],
  },
  {
    title: "Environment Defaults",
    fields: [
      {
        key: "environment.conda",
        label: "Conda Environment",
        description: "Default conda env to activate",
        kind: "str",
        placeholder: "e.g. ml_env",
      },
      {
        key: "environment.venv",
        label: "Virtual Environment",
        description: "Path to venv to activate",
        kind: "str",
        placeholder: "e.g. /path/to/venv",
      },
    ],
  },
  {
    title: "Paths",
    fields: [
      {
        key: "log_dir",
        label: "Log Directory",
        description: "Default directory for SLURM log output",
        kind: "str",
        placeholder: "logs",
      },
      {
        key: "work_dir",
        label: "Working Directory",
        description: "Default working directory for jobs",
        kind: "str",
        placeholder: "e.g. /scratch/username",
      },
    ],
  },
];

function toDraft(value: unknown): string {
  if (value === undefined || value === null) return "";
  return String(value);
}

function formatValue(value: unknown): string {
  if (value === undefined || value === null || value === "") return "—";
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
}

function SourceTag({ source }: { source: string | undefined }) {
  return (
    <span
      title={source ?? "built-in default"}
      style={{
        ...monoSmall,
        display: "inline-block",
        maxWidth: 360,
        overflow: "hidden",
        textOverflow: "ellipsis",
        whiteSpace: "nowrap",
        verticalAlign: "bottom",
        color: source ? "var(--accent)" : "var(--text-muted)",
      }}
    >
      {source ?? "built-in default"}
    </span>
  );
}

/* ── Component ───────────────────────────────── */

export function GeneralTab() {
  const { cluster, project } = useSelection();
  const [scopeId, setScopeId] = useState<ScopeId>("user");
  const scope = SCOPES.find((s) => s.id === scopeId)!;

  const [effective, setEffective] = useState<EffectiveConfig | null>(null);
  const [file, setFile] = useState<ConfigFile | null>(null);
  const [paths, setPaths] = useState<ConfigPathInfo[]>([]);
  const [drafts, setDrafts] = useState<Record<string, string>>({});
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [success, setSuccess] = useState<string | null>(null);
  const clearSuccess = useCallback(() => setSuccess(null), []);

  const scopeAvailable = (s: ScopeDef) =>
    (s.file !== "project" || project !== null) &&
    (!s.perCluster || cluster !== null);

  // The key prefix inside the scope's file for `defaults` tables.
  const prefix = scope.perCluster ? `clusters.${cluster}.defaults` : "defaults";

  const load = useCallback(async () => {
    try {
      setLoading(true);
      const [eff, f, p] = await Promise.all([
        configApi.get(),
        configApi.getFile(scope.file),
        configApi.paths(),
      ]);
      setEffective(eff);
      setFile(f);
      setPaths(p);
      setDrafts({});
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to load config");
    } finally {
      setLoading(false);
    }
  }, [scope.file]);

  useEffect(() => {
    if (!scopeAvailable(scope)) {
      setScopeId("user");
      return;
    }
    load();
  }, [load, scopeId]); // eslint-disable-line react-hooks/exhaustive-deps

  const fileValue = useCallback(
    (key: string) => getIn(file?.data, `${prefix}.${key}`),
    [file, prefix],
  );

  const isUnset = useCallback(
    (key: string) => {
      const parts = key.split(".");
      const leaf = parts.pop()!;
      const table = getIn(
        file?.data,
        [prefix, ...parts].join("."),
      ) as Record<string, unknown> | undefined;
      const list = table?.unset;
      return Array.isArray(list) && list.includes(leaf);
    },
    [file, prefix],
  );

  const draftOf = (key: string) =>
    key in drafts ? drafts[key] : toDraft(fileValue(key));

  const dirtyKeys = useMemo(
    () =>
      Object.keys(drafts).filter((k) => drafts[k] !== toDraft(fileValue(k))),
    [drafts, fileValue],
  );

  const kindOf = (key: string): FieldKind =>
    SECTIONS.flatMap((s) => s.fields).find((f) => f.key === key)?.kind ??
    "str";

  const patch = async (
    body: { set?: Record<string, unknown>; delete?: string[]; unset?: string[] },
    message: string,
  ) => {
    if (!file) return;
    try {
      setSaving(true);
      setError(null);
      await configApi.patchFile({
        scope: scope.file,
        version: file.version,
        ...body,
      });
      setSuccess(message);
      await load();
    } catch (e) {
      if (e instanceof ConfigConflictError) {
        // Reload first: ``load`` clears the error banner on success.
        await load();
        setError(
          "The config file changed on disk since it was loaded. Reloaded — please re-apply your edits.",
        );
      } else {
        setError(e instanceof Error ? e.message : "Failed to save config");
      }
    } finally {
      setSaving(false);
    }
  };

  const handleSave = () => {
    const set: Record<string, unknown> = {};
    const del: string[] = [];
    for (const key of dirtyKeys) {
      const raw = drafts[key].trim();
      const full = `${prefix}.${key}`;
      if (raw === "") {
        del.push(full);
      } else if (kindOf(key) === "int") {
        const n = Number(raw);
        if (!Number.isInteger(n)) {
          setError(`${key}: "${raw}" is not an integer`);
          return;
        }
        set[full] = n;
      } else {
        set[full] = raw;
      }
    }
    void patch({ set, delete: del }, `Saved ${dirtyKeys.length} change(s)`);
  };

  const handleInherit = (key: string) =>
    patch(
      { delete: [`${prefix}.${key}`] },
      `${key} now inherited from lower layers`,
    );

  const handleUnset = (key: string) =>
    patch({ unset: [`${prefix}.${key}`] }, `${key} explicitly cleared`);

  const scopePath =
    scope.file === "user"
      ? paths.find((p) => p.source === "user")?.path
      : paths.find((p) => p.source === "project")?.path;

  return (
    <div
      style={{ display: "flex", flexDirection: "column", gap: "var(--sp-4)" }}
    >
      {/* Scope picker + actions */}
      <div
        style={{
          display: "flex",
          alignItems: "center",
          gap: "var(--sp-3)",
          flexWrap: "wrap",
        }}
      >
        <span style={{ fontSize: "0.8rem", color: "var(--text-muted)" }}>
          Edit defaults at
        </span>
        <select
          className="input"
          value={scopeId}
          onChange={(e) => setScopeId(e.target.value as ScopeId)}
          style={{ width: 240 }}
          aria-label="Config scope"
        >
          {SCOPES.map((s) => (
            <option key={s.id} value={s.id} disabled={!scopeAvailable(s)}>
              {s.label}
              {s.perCluster && cluster ? ` (${cluster})` : ""}
              {s.file === "project" && project ? ` — ${project}` : ""}
            </option>
          ))}
        </select>
        <span style={monoSmall}>
          {scopePath ?? file?.path}
          {scope.perCluster ? ` [clusters.${cluster}.defaults]` : " [defaults]"}
        </span>
        <div style={{ flex: 1 }} />
        <button
          className="btn btn-ghost"
          onClick={() => setDrafts({})}
          disabled={saving || dirtyKeys.length === 0}
        >
          <Undo2 size={14} />
          Discard
        </button>
        <button
          className="btn btn-primary"
          onClick={handleSave}
          disabled={saving || dirtyKeys.length === 0}
        >
          <Save size={14} />
          {saving ? "Saving..." : "Save"}
        </button>
      </div>

      <div style={{ fontSize: "0.75rem", color: "var(--text-muted)" }}>
        Effective values below are resolved for cluster{" "}
        <code>{effective?.cluster ?? cluster ?? "—"}</code>
        {effective?.project ? (
          <>
            {" "}
            and project <code>{effective.project}</code>
          </>
        ) : null}
        . Leave a field empty to inherit it from a lower layer.
      </div>

      <ErrorBanner error={error} />
      <SuccessBanner message={success} onDone={clearSuccess} />

      {loading ? (
        <div className="panel skeleton" style={{ height: 400 }} />
      ) : effective && file ? (
        <>
          {SECTIONS.map((section, i) => (
            <motion.div key={section.title} {...stagger(i)} className="panel">
              <div className="panel-header">
                <h3>{section.title}</h3>
              </div>
              <div className="panel-body">
                {section.fields.map((f) => {
                  const atScope = fileValue(f.key);
                  const unset = isUnset(f.key);
                  return (
                    <FieldRow
                      key={f.key}
                      label={f.label}
                      description={
                        <>
                          {f.description}
                          <div style={{ marginTop: 4 }}>
                            <span style={monoSmall}>effective: </span>
                            <span
                              style={{
                                ...monoSmall,
                                color: "var(--text-secondary)",
                              }}
                            >
                              {formatValue(getIn(effective.values, f.key))}
                            </span>
                            <span style={monoSmall}> · from </span>
                            <SourceTag source={effective.sources[f.key]} />
                          </div>
                        </>
                      }
                    >
                      <div
                        style={{
                          display: "flex",
                          gap: "var(--sp-1)",
                          alignItems: "center",
                        }}
                      >
                        <input
                          className="input"
                          aria-label={f.label}
                          type={f.kind === "int" ? "number" : "text"}
                          value={draftOf(f.key)}
                          placeholder={
                            unset ? "(cleared)" : (f.placeholder ?? "inherit")
                          }
                          onChange={(e) =>
                            setDrafts((d) => ({
                              ...d,
                              [f.key]: e.target.value,
                            }))
                          }
                          style={{ flex: 1, minWidth: 0 }}
                        />
                        <button
                          className="btn btn-ghost"
                          title="Inherit (remove from this scope)"
                          disabled={saving || (atScope === undefined && !unset)}
                          onClick={() => handleInherit(f.key)}
                          style={{ padding: "var(--sp-2)" }}
                        >
                          <Undo2 size={13} />
                        </button>
                        {f.kind === "str" && (
                          <button
                            className="btn btn-ghost"
                            title="Explicitly clear (ignore inherited value)"
                            disabled={saving || unset}
                            onClick={() => handleUnset(f.key)}
                            style={{ padding: "var(--sp-2)" }}
                          >
                            <Eraser size={13} />
                          </button>
                        )}
                      </div>
                    </FieldRow>
                  );
                })}
              </div>
            </motion.div>
          ))}

          {/* Environment variables (read-only summary of effective values) */}
          {Object.keys(effective.values.environment.env_vars).length > 0 && (
            <motion.div {...stagger(SECTIONS.length)} className="panel">
              <div className="panel-header">
                <h3>Effective Environment Variables</h3>
              </div>
              <div className="panel-body">
                {Object.entries(effective.values.environment.env_vars).map(
                  ([k, v]) => (
                    <div
                      key={k}
                      style={{
                        display: "flex",
                        gap: "var(--sp-3)",
                        padding: "var(--sp-1) 0",
                        fontFamily: "var(--font-mono)",
                        fontSize: "0.8rem",
                      }}
                    >
                      <span style={{ color: "var(--accent)", minWidth: 160 }}>
                        {k}
                      </span>
                      <span style={{ color: "var(--text-secondary)" }}>
                        {v}
                      </span>
                      <div style={{ flex: 1 }} />
                      <SourceTag
                        source={effective.sources[`environment.env_vars.${k}`]}
                      />
                    </div>
                  ),
                )}
                <div style={{ ...monoSmall, marginTop: "var(--sp-2)" }}>
                  Edit <code>environment.env_vars</code> in the config file.
                </div>
              </div>
            </motion.div>
          )}

          {/* Config File Paths */}
          <motion.div {...stagger(SECTIONS.length + 1)} className="panel">
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
    </div>
  );
}
