import { useCallback, useEffect, useState } from "react";
import {
  Server,
  Plus,
  Pencil,
  Trash2,
  Plug,
  Star,
  Loader2,
  X,
  Save,
} from "lucide-react";
import {
  clusters as clustersApi,
  config as configApi,
  ConfigConflictError,
} from "../../lib/api.ts";
import { refreshSelection } from "../../lib/selection.ts";
import type {
  ClusterInfo,
  ClusterList,
  ClusterTestResult,
  ClusterTransport,
} from "../../lib/types.ts";
import { ErrorBanner } from "../../components/ErrorBanner.tsx";
import { FieldRow, SuccessBanner, monoSmall } from "./shared.tsx";

type FormState = {
  name: string;
  transport: ClusterTransport;
  host: string;
  hostname: string;
  username: string;
  key_filename: string;
  port: string;
  proxy_jump: string;
  description: string;
  snapshot_root: string;
  make_default: boolean;
};

const EMPTY_FORM: FormState = {
  name: "",
  transport: "ssh",
  host: "",
  hostname: "",
  username: "",
  key_filename: "",
  port: "22",
  proxy_jump: "",
  description: "",
  snapshot_root: "",
  make_default: false,
};

const OPTIONAL_FIELDS = [
  "host",
  "hostname",
  "username",
  "key_filename",
  "proxy_jump",
  "description",
  "snapshot_root",
] as const;

function toForm(c: ClusterInfo): FormState {
  return {
    name: c.name,
    transport: c.transport,
    host: c.host ?? "",
    hostname: c.hostname ?? "",
    username: c.username ?? "",
    key_filename: c.key_filename ?? "",
    port: String(c.port ?? 22),
    proxy_jump: c.proxy_jump ?? "",
    description: c.description ?? "",
    snapshot_root: c.snapshot_root ?? "",
    make_default: false,
  };
}

function TextField({
  label,
  description,
  value,
  onChange,
  placeholder,
  disabled,
}: {
  label: string;
  description?: string;
  value: string;
  onChange: (v: string) => void;
  placeholder?: string;
  disabled?: boolean;
}) {
  return (
    <FieldRow label={label} description={description}>
      <input
        className="input"
        type="text"
        aria-label={label}
        value={value}
        placeholder={placeholder}
        disabled={disabled}
        onChange={(e) => onChange(e.target.value)}
        style={{ width: "100%" }}
      />
    </FieldRow>
  );
}

export function ClustersTab() {
  const [data, setData] = useState<ClusterList | null>(null);
  const [loading, setLoading] = useState(true);
  const [busy, setBusy] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [success, setSuccess] = useState<string | null>(null);
  const clearSuccess = useCallback(() => setSuccess(null), []);
  // null = closed, "" = adding, otherwise the name being edited
  const [editing, setEditing] = useState<string | null>(null);
  const [form, setForm] = useState<FormState>(EMPTY_FORM);
  const [tests, setTests] = useState<Record<string, ClusterTestResult>>({});

  const load = useCallback(async () => {
    try {
      setLoading(true);
      setData(await clustersApi.list());
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to load clusters");
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    load();
  }, [load]);

  const afterChange = async (message: string) => {
    setSuccess(message);
    await Promise.all([load(), refreshSelection()]);
  };

  const run = async (key: string, fn: () => Promise<void>) => {
    try {
      setBusy(key);
      setError(null);
      await fn();
    } catch (e) {
      setError(e instanceof Error ? e.message : "Operation failed");
    } finally {
      setBusy(null);
    }
  };

  const update = (patch: Partial<FormState>) =>
    setForm((f) => ({ ...f, ...patch }));

  const handleSubmit = () =>
    run("form", async () => {
      const port = Number(form.port || "22");
      if (!Number.isInteger(port) || port <= 0) {
        throw new Error(`Invalid port: ${form.port}`);
      }
      if (editing === "") {
        const body: ClusterInfo & { make_default?: boolean } = {
          name: form.name.trim(),
          transport: form.transport,
          port,
          make_default: form.make_default,
        };
        for (const k of OPTIONAL_FIELDS) {
          const v = form[k].trim();
          if (v) body[k] = v;
        }
        await clustersApi.create(body);
        setEditing(null);
        await afterChange(`Cluster "${body.name}" added`);
      } else if (editing) {
        const body: Partial<Omit<ClusterInfo, "name">> = {
          transport: form.transport,
          port,
        };
        // Empty strings remove the field server-side.
        for (const k of OPTIONAL_FIELDS) body[k] = form[k].trim() || null;
        await clustersApi.update(editing, body);
        const name = editing;
        setEditing(null);
        await afterChange(`Cluster "${name}" updated`);
      }
    });

  const handleDelete = (name: string) => {
    if (!window.confirm(`Delete cluster "${name}" from the user config?`))
      return;
    void run(`delete:${name}`, async () => {
      await clustersApi.delete(name);
      await afterChange(`Cluster "${name}" deleted`);
    });
  };

  const handleTest = (name: string) =>
    run(`test:${name}`, async () => {
      const result = await clustersApi.test(name);
      setTests((t) => ({ ...t, [name]: result }));
    });

  const handleMakeDefault = (name: string) =>
    run(`default:${name}`, async () => {
      const file = await configApi.getFile("user");
      try {
        await configApi.patchFile({
          scope: "user",
          version: file.version,
          set: { default_cluster: name },
        });
      } catch (e) {
        if (e instanceof ConfigConflictError) {
          await load();
          throw new Error(
            "The user config changed on disk; reloaded. Please try again.",
          );
        }
        throw e;
      }
      await afterChange(`"${name}" is now the default cluster`);
    });

  if (loading && !data) {
    return <div className="panel skeleton" style={{ height: 300 }} />;
  }

  return (
    <div
      style={{ display: "flex", flexDirection: "column", gap: "var(--sp-4)" }}
    >
      <ErrorBanner error={error} />
      <SuccessBanner message={success} onDone={clearSuccess} />

      <div className="panel">
        <div
          className="panel-header"
          style={{ display: "flex", justifyContent: "space-between" }}
        >
          <h3>
            <Server size={14} style={{ marginRight: 8, verticalAlign: -2 }} />
            Clusters
          </h3>
          <button
            className="btn btn-primary"
            onClick={() => {
              setForm(EMPTY_FORM);
              setEditing("");
            }}
            disabled={editing !== null}
          >
            <Plus size={14} />
            Add Cluster
          </button>
        </div>
        <div className="panel-body">
          {data && data.clusters.length === 0 && (
            <div style={{ color: "var(--text-muted)", fontSize: "0.85rem" }}>
              No clusters configured. Add one to submit jobs.
            </div>
          )}
          <div
            style={{
              display: "flex",
              flexDirection: "column",
              gap: "var(--sp-2)",
            }}
          >
            {data?.clusters.map((c) => {
              const isDefault = c.name === data.default;
              const test = tests[c.name];
              const target =
                c.transport === "local"
                  ? "local"
                  : (c.host ??
                    `${c.username ? `${c.username}@` : ""}${c.hostname ?? "?"}${
                      c.port && c.port !== 22 ? `:${c.port}` : ""
                    }`);
              return (
                <div
                  key={c.name}
                  data-testid={`cluster-row-${c.name}`}
                  style={{
                    padding: "var(--sp-3)",
                    background: "var(--bg-base)",
                    borderRadius: "var(--radius-md)",
                    border: "1px solid var(--border-ghost)",
                  }}
                >
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
                        fontSize: "0.85rem",
                        color: "var(--text-primary)",
                        fontWeight: 600,
                      }}
                    >
                      {c.name}
                    </span>
                    {isDefault && (
                      <span
                        style={{
                          ...monoSmall,
                          color: "var(--accent)",
                          border: "1px solid var(--accent)",
                          borderRadius: 4,
                          padding: "0 6px",
                        }}
                      >
                        default
                      </span>
                    )}
                    <span style={monoSmall}>{c.transport}</span>
                    <span style={{ ...monoSmall, flex: 1 }}>
                      {target}
                      {c.proxy_jump ? ` via ${c.proxy_jump}` : ""}
                      {c.description ? ` — ${c.description}` : ""}
                    </span>
                    <button
                      className="btn btn-ghost"
                      title="Test connection"
                      onClick={() => handleTest(c.name)}
                      disabled={busy !== null}
                      style={{ padding: "var(--sp-2)" }}
                    >
                      {busy === `test:${c.name}` ? (
                        <Loader2 size={14} className="spin" />
                      ) : (
                        <Plug size={14} />
                      )}
                    </button>
                    <button
                      className="btn btn-ghost"
                      title="Make default"
                      onClick={() => handleMakeDefault(c.name)}
                      disabled={busy !== null || isDefault}
                      style={{ padding: "var(--sp-2)" }}
                    >
                      <Star size={14} />
                    </button>
                    <button
                      className="btn btn-ghost"
                      title="Edit"
                      onClick={() => {
                        setForm(toForm(c));
                        setEditing(c.name);
                      }}
                      disabled={editing !== null}
                      style={{ padding: "var(--sp-2)" }}
                    >
                      <Pencil size={14} />
                    </button>
                    <button
                      className="btn btn-ghost"
                      title="Delete"
                      onClick={() => handleDelete(c.name)}
                      disabled={busy !== null}
                      style={{ padding: "var(--sp-2)" }}
                    >
                      <Trash2 size={14} />
                    </button>
                  </div>
                  {test && (
                    <div
                      style={{
                        ...monoSmall,
                        marginTop: "var(--sp-2)",
                        color: test.ok
                          ? "var(--st-completed)"
                          : "var(--st-failed)",
                      }}
                    >
                      {test.ok ? "OK" : "FAILED"} — {test.detail}
                      {test.home ? ` (home: ${test.home})` : ""}
                    </div>
                  )}
                </div>
              );
            })}
          </div>
        </div>
      </div>

      {editing !== null && (
        <div className="panel">
          <div className="panel-header">
            <h3>{editing === "" ? "Add Cluster" : `Edit "${editing}"`}</h3>
          </div>
          <div className="panel-body">
            <TextField
              label="Name"
              value={form.name}
              onChange={(v) => update({ name: v })}
              placeholder="e.g. gpu"
              disabled={editing !== ""}
            />
            <FieldRow label="Transport">
              <select
                className="input"
                value={form.transport}
                onChange={(e) =>
                  update({ transport: e.target.value as ClusterTransport })
                }
                style={{ width: "100%" }}
              >
                <option value="ssh">ssh</option>
                <option value="local">local</option>
              </select>
            </FieldRow>
            {form.transport === "ssh" && (
              <>
                <TextField
                  label="Host alias"
                  description="Host from ~/.ssh/config (alternative to hostname/username)"
                  value={form.host}
                  onChange={(v) => update({ host: v })}
                  placeholder="e.g. my-cluster"
                />
                <TextField
                  label="Hostname"
                  value={form.hostname}
                  onChange={(v) => update({ hostname: v })}
                  placeholder="e.g. login.example.org"
                />
                <TextField
                  label="Username"
                  value={form.username}
                  onChange={(v) => update({ username: v })}
                />
                <TextField
                  label="Key file"
                  value={form.key_filename}
                  onChange={(v) => update({ key_filename: v })}
                  placeholder="~/.ssh/id_ed25519"
                />
                <TextField
                  label="Port"
                  value={form.port}
                  onChange={(v) => update({ port: v })}
                  placeholder="22"
                />
                <TextField
                  label="Proxy jump"
                  value={form.proxy_jump}
                  onChange={(v) => update({ proxy_jump: v })}
                />
              </>
            )}
            <TextField
              label="Description"
              value={form.description}
              onChange={(v) => update({ description: v })}
            />
            <TextField
              label="Snapshot root"
              description="Remote directory for workflow snapshots"
              value={form.snapshot_root}
              onChange={(v) => update({ snapshot_root: v })}
            />
            {editing === "" && (
              <FieldRow label="Make default">
                <input
                  type="checkbox"
                  checked={form.make_default}
                  onChange={(e) => update({ make_default: e.target.checked })}
                />
              </FieldRow>
            )}
            <div
              style={{
                display: "flex",
                justifyContent: "flex-end",
                gap: "var(--sp-2)",
                marginTop: "var(--sp-4)",
              }}
            >
              <button
                className="btn btn-ghost"
                onClick={() => setEditing(null)}
                disabled={busy === "form"}
              >
                <X size={14} />
                Cancel
              </button>
              <button
                className="btn btn-primary"
                onClick={handleSubmit}
                disabled={busy === "form" || !form.name.trim()}
              >
                <Save size={14} />
                {busy === "form" ? "Saving..." : "Save"}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
