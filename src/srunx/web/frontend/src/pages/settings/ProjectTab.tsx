import { useCallback, useEffect, useState } from "react";
import { motion } from "framer-motion";
import { FolderOpen, Save, Server, HardDrive } from "lucide-react";
import {
  config as configApi,
  projects as projectsApi,
  ConfigConflictError,
} from "../../lib/api.ts";
import { refreshSelection, useSelection } from "../../lib/selection.ts";
import type { ConfigFile, ProjectDetail } from "../../lib/types.ts";
import { ErrorBanner } from "../../components/ErrorBanner.tsx";
import {
  FieldRow,
  NoticeBox,
  SuccessBanner,
  getIn,
  monoSmall,
} from "./shared.tsx";

type ClusterDraft = {
  remote_dir: string;
  sync: "" | "true" | "false";
  exclude: string;
};

function draftFrom(file: ConfigFile | null, cluster: string): ClusterDraft {
  const table = getIn(file?.data, `clusters.${cluster}`) as
    | Record<string, unknown>
    | undefined;
  const sync = table?.sync;
  const exclude = table?.exclude;
  return {
    remote_dir: typeof table?.remote_dir === "string" ? table.remote_dir : "",
    sync: sync === true ? "true" : sync === false ? "false" : "",
    exclude: Array.isArray(exclude) ? exclude.join("\n") : "",
  };
}

function sameDraft(a: ClusterDraft, b: ClusterDraft): boolean {
  return (
    a.remote_dir === b.remote_dir && a.sync === b.sync && a.exclude === b.exclude
  );
}

export function ProjectTab() {
  const { project, clusters } = useSelection();
  const [detail, setDetail] = useState<ProjectDetail | null>(null);
  const [file, setFile] = useState<ConfigFile | null>(null);
  const [pinned, setPinned] = useState("");
  const [drafts, setDrafts] = useState<Record<string, ClusterDraft>>({});
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [success, setSuccess] = useState<string | null>(null);
  const clearSuccess = useCallback(() => setSuccess(null), []);

  const load = useCallback(async () => {
    if (!project) {
      setLoading(false);
      return;
    }
    try {
      setLoading(true);
      const [d, f] = await Promise.all([
        projectsApi.get(project),
        configApi.getFile("project"),
      ]);
      setDetail(d);
      setFile(f);
      setPinned(typeof f.data.cluster === "string" ? f.data.cluster : "");
      setDrafts({});
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to load project");
    } finally {
      setLoading(false);
    }
  }, [project]);

  useEffect(() => {
    load();
  }, [load]);

  const patch = async (
    key: string,
    body: { set?: Record<string, unknown>; delete?: string[] },
    message: string,
  ) => {
    if (!file) return;
    try {
      setSaving(key);
      setError(null);
      await configApi.patchFile({
        scope: "project",
        version: file.version,
        ...body,
      });
      setSuccess(message);
      await load();
      if (key === "cluster") await refreshSelection();
    } catch (e) {
      if (e instanceof ConfigConflictError) {
        // Reload first: ``load`` clears the error banner on success.
        await load();
        setError(
          "The project config changed on disk since it was loaded. Reloaded — please re-apply your edits.",
        );
      } else {
        setError(e instanceof Error ? e.message : "Failed to save");
      }
    } finally {
      setSaving(null);
    }
  };

  if (!project) {
    return (
      <NoticeBox>
        No project is selected. Start <code>srunx ui</code> inside a project
        (a directory with <code>.srunx/config.toml</code>) or pass{" "}
        <code>--project</code>.
      </NoticeBox>
    );
  }

  if (loading && !detail) {
    return <div className="panel skeleton" style={{ height: 300 }} />;
  }

  const savedPinned =
    typeof file?.data.cluster === "string" ? file.data.cluster : "";

  const saveCluster = (name: string) => {
    const draft = drafts[name];
    if (!draft) return;
    const base = `clusters.${name}`;
    const set: Record<string, unknown> = {};
    const del: string[] = [];
    if (draft.remote_dir.trim()) set[`${base}.remote_dir`] = draft.remote_dir.trim();
    else del.push(`${base}.remote_dir`);
    if (draft.sync) set[`${base}.sync`] = draft.sync === "true";
    else del.push(`${base}.sync`);
    const exclude = draft.exclude
      .split(/[\n,]/)
      .map((s) => s.trim())
      .filter(Boolean);
    if (exclude.length > 0) set[`${base}.exclude`] = exclude;
    else del.push(`${base}.exclude`);
    // Only delete keys that actually exist to keep the file tidy.
    const existing = draftFrom(file, name);
    const filteredDel = del.filter((k) => {
      const leaf = k.split(".").pop() as keyof ClusterDraft;
      return existing[leaf] !== "";
    });
    void patch(
      `cluster:${name}`,
      { set, delete: filteredDel },
      `Saved settings for cluster "${name}"`,
    );
  };

  return (
    <div
      style={{ display: "flex", flexDirection: "column", gap: "var(--sp-4)" }}
    >
      <ErrorBanner error={error} />
      <SuccessBanner message={success} onDone={clearSuccess} />

      {/* Project overview */}
      <motion.div className="panel">
        <div className="panel-header">
          <h3>
            <FolderOpen size={14} style={{ marginRight: 8, verticalAlign: -2 }} />
            Project "{project}"
          </h3>
        </div>
        <div className="panel-body">
          <FieldRow label="Root">
            <span style={monoSmall}>{detail?.root}</span>
          </FieldRow>
          <FieldRow label="Config file">
            <span style={monoSmall}>
              {file?.path}
              {file && !file.exists ? " (will be created)" : ""}
            </span>
          </FieldRow>
          <FieldRow
            label="Cluster"
            description="Cluster this project uses by default"
          >
            <div style={{ display: "flex", gap: "var(--sp-2)" }}>
              <select
                className="input"
                aria-label="Project cluster"
                value={pinned}
                onChange={(e) => setPinned(e.target.value)}
                style={{ flex: 1 }}
              >
                <option value="">(user default)</option>
                {clusters.map((c) => (
                  <option key={c.name} value={c.name}>
                    {c.name}
                  </option>
                ))}
                {pinned && !clusters.some((c) => c.name === pinned) && (
                  <option value={pinned}>{pinned} (unknown)</option>
                )}
              </select>
              <button
                className="btn btn-primary"
                disabled={saving !== null || pinned === savedPinned}
                onClick={() =>
                  patch(
                    "cluster",
                    pinned ? { set: { cluster: pinned } } : { delete: ["cluster"] },
                    pinned
                      ? `Project cluster set to "${pinned}"`
                      : "Project cluster cleared",
                  )
                }
                style={{ padding: "var(--sp-2)" }}
              >
                <Save size={14} />
              </button>
            </div>
          </FieldRow>
        </div>
      </motion.div>

      {/* Per-cluster settings */}
      {clusters.map((c) => {
        const saved = draftFrom(file, c.name);
        const draft = drafts[c.name] ?? saved;
        const dirty = !sameDraft(draft, saved);
        const setDraft = (p: Partial<ClusterDraft>) =>
          setDrafts((d) => ({ ...d, [c.name]: { ...draft, ...p } }));
        const mounts = detail?.mounts[c.name] ?? [];
        return (
          <motion.div
            key={c.name}
            className="panel"
            data-testid={`project-cluster-${c.name}`}
          >
            <div
              className="panel-header"
              style={{ display: "flex", justifyContent: "space-between" }}
            >
              <h3>
                <Server size={14} style={{ marginRight: 8, verticalAlign: -2 }} />
                On cluster "{c.name}"
              </h3>
              <button
                className="btn btn-primary"
                disabled={saving !== null || !dirty}
                onClick={() => saveCluster(c.name)}
              >
                <Save size={14} />
                {saving === `cluster:${c.name}` ? "Saving..." : "Save"}
              </button>
            </div>
            <div className="panel-body">
              <FieldRow
                label="Remote directory"
                description="Where the project root is synced on this cluster"
              >
                <input
                  className="input"
                  type="text"
                  aria-label={`Remote directory (${c.name})`}
                  value={draft.remote_dir}
                  placeholder="e.g. /home/me/projects/foo"
                  onChange={(e) => setDraft({ remote_dir: e.target.value })}
                  style={{ width: "100%" }}
                />
              </FieldRow>
              <FieldRow label="Sync" description="Rsync the project before runs">
                <select
                  className="input"
                  value={draft.sync}
                  onChange={(e) =>
                    setDraft({ sync: e.target.value as ClusterDraft["sync"] })
                  }
                  style={{ width: "100%" }}
                >
                  <option value="">(default: on)</option>
                  <option value="true">on</option>
                  <option value="false">off</option>
                </select>
              </FieldRow>
              <FieldRow
                label="Exclude"
                description="Rsync exclude patterns, one per line"
              >
                <textarea
                  className="input"
                  value={draft.exclude}
                  rows={3}
                  onChange={(e) => setDraft({ exclude: e.target.value })}
                  style={{
                    width: "100%",
                    fontFamily: "var(--font-mono)",
                    fontSize: "0.8rem",
                  }}
                />
              </FieldRow>

              <div style={{ paddingTop: "var(--sp-4)" }}>
                <div
                  style={{
                    fontSize: "0.85rem",
                    fontWeight: 500,
                    color: "var(--text-primary)",
                    marginBottom: "var(--sp-2)",
                  }}
                >
                  Resolved mounts
                </div>
                {mounts.length === 0 ? (
                  <div style={monoSmall}>
                    No mounts on this cluster (set a remote directory or add{" "}
                    <code>[[clusters.{c.name}.mounts]]</code> to the project
                    config).
                  </div>
                ) : (
                  mounts.map((m) => (
                    <div
                      key={m.name}
                      style={{
                        display: "flex",
                        alignItems: "center",
                        gap: "var(--sp-3)",
                        padding: "var(--sp-2) var(--sp-3)",
                        marginBottom: "var(--sp-1)",
                        background: "var(--bg-base)",
                        borderRadius: "var(--radius-md)",
                        border: "1px solid var(--border-ghost)",
                        fontFamily: "var(--font-mono)",
                        fontSize: "0.78rem",
                      }}
                    >
                      <HardDrive size={13} style={{ flexShrink: 0 }} />
                      <span style={{ color: "var(--accent)", minWidth: 100 }}>
                        {m.name}
                      </span>
                      <span
                        style={{ color: "var(--text-secondary)", flex: 1 }}
                      >
                        {m.local} → {m.remote}
                      </span>
                      <span style={monoSmall}>
                        {m.sync ? "sync" : "no sync"}
                        {m.exclude.length > 0
                          ? ` · ${m.exclude.length} excludes`
                          : ""}
                      </span>
                    </div>
                  ))
                )}
              </div>
            </div>
          </motion.div>
        );
      })}
    </div>
  );
}
