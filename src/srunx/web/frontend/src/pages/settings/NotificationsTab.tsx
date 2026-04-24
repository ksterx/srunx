import { useCallback, useEffect, useId, useState } from "react";
import { motion } from "framer-motion";
import {
  AlertTriangle,
  Bell,
  BellOff,
  Check,
  Plus,
  Power,
  Star,
  Trash2,
  X,
} from "lucide-react";
import { ErrorBanner } from "../../components/ErrorBanner.tsx";
import {
  config as configApi,
  endpoints as endpointsApi,
} from "../../lib/api.ts";
import type {
  Endpoint,
  NotificationPreset,
  SrunxConfig,
} from "../../lib/types.ts";

// Keep in sync with ``ACCEPTED_PRESETS`` in
// ``srunx.observability.notifications.attach``. ``digest`` is
// schema-valid but the backend rejects POSTs under it until an
// aggregator ships.
const PRESET_OPTIONS: ReadonlyArray<{
  value: NotificationPreset;
  label: string;
}> = [
  { value: "terminal", label: "Terminal only" },
  { value: "running_and_terminal", label: "Running + terminal" },
  { value: "all", label: "All state changes" },
];

const SLACK_WEBHOOK_PATTERN =
  /^https:\/\/hooks\.slack\.com\/services\/[A-Za-z0-9_-]+\/[A-Za-z0-9_-]+\/[A-Za-z0-9_-]+$/;

function formatDate(value: string): string {
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return value;
  return d.toLocaleString();
}

type AddFormProps = {
  onCancel: () => void;
  onCreated: (endpoint: Endpoint) => void;
};

function AddEndpointForm({ onCancel, onCreated }: AddFormProps) {
  const [name, setName] = useState("");
  const [webhookUrl, setWebhookUrl] = useState("");
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Stable per-instance ids for label↔input association. Without these
  // the visual <label> elements are not exposed to screen readers.
  const nameId = useId();
  const kindId = useId();
  const webhookId = useId();
  const webhookErrorId = useId();
  const formErrorId = useId();

  const urlValid = SLACK_WEBHOOK_PATTERN.test(webhookUrl);
  const canSubmit = name.trim().length > 0 && urlValid && !saving;

  const handleSubmit = async () => {
    if (!canSubmit) return;
    try {
      setSaving(true);
      setError(null);
      const created = await endpointsApi.create({
        kind: "slack_webhook",
        name: name.trim(),
        config: { webhook_url: webhookUrl },
      });
      onCreated(created);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to create endpoint");
    } finally {
      setSaving(false);
    }
  };

  return (
    <motion.div
      initial={{ opacity: 0, y: -6 }}
      animate={{ opacity: 1, y: 0 }}
      style={{
        padding: "var(--sp-4)",
        background: "var(--bg-base)",
        border: "1px solid var(--border-subtle)",
        borderRadius: "var(--radius-md)",
        display: "flex",
        flexDirection: "column",
        gap: "var(--sp-3)",
      }}
    >
      <div
        style={{
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
        }}
      >
        <span
          style={{
            fontFamily: "var(--font-display)",
            fontSize: "0.75rem",
            fontWeight: 600,
            textTransform: "uppercase",
            letterSpacing: "0.08em",
            color: "var(--text-secondary)",
          }}
        >
          New Slack Endpoint
        </span>
        <button
          onClick={onCancel}
          aria-label="Cancel new endpoint"
          style={{
            background: "none",
            border: "none",
            color: "var(--text-muted)",
            cursor: "pointer",
            padding: 4,
            borderRadius: "var(--radius-sm)",
            display: "flex",
          }}
        >
          <X size={14} aria-hidden="true" />
        </button>
      </div>

      <div>
        <label
          htmlFor={nameId}
          style={{
            fontFamily: "var(--font-mono)",
            fontSize: "0.65rem",
            color: "var(--text-muted)",
            textTransform: "uppercase",
            letterSpacing: "0.06em",
            marginBottom: 4,
            display: "block",
          }}
        >
          Name
        </label>
        <input
          id={nameId}
          className="input"
          type="text"
          value={name}
          onChange={(e) => setName(e.target.value)}
          placeholder="e.g. team-alerts"
          required
          style={{ width: "100%", fontSize: "0.8rem" }}
        />
      </div>

      <div>
        <label
          htmlFor={kindId}
          style={{
            fontFamily: "var(--font-mono)",
            fontSize: "0.65rem",
            color: "var(--text-muted)",
            textTransform: "uppercase",
            letterSpacing: "0.06em",
            marginBottom: 4,
            display: "block",
          }}
        >
          Kind
        </label>
        <input
          id={kindId}
          className="input"
          type="text"
          value="slack_webhook"
          disabled
          aria-readonly="true"
          style={{
            width: "100%",
            fontSize: "0.8rem",
            fontFamily: "var(--font-mono)",
            opacity: 0.6,
          }}
        />
      </div>

      <div>
        <label
          htmlFor={webhookId}
          style={{
            fontFamily: "var(--font-mono)",
            fontSize: "0.65rem",
            color: "var(--text-muted)",
            textTransform: "uppercase",
            letterSpacing: "0.06em",
            marginBottom: 4,
            display: "block",
          }}
        >
          Webhook URL
        </label>
        <input
          id={webhookId}
          className="input"
          type="url"
          value={webhookUrl}
          onChange={(e) => setWebhookUrl(e.target.value)}
          placeholder="https://hooks.slack.com/services/..."
          required
          aria-invalid={webhookUrl !== "" && !urlValid}
          aria-describedby={
            webhookUrl && !urlValid ? webhookErrorId : undefined
          }
          style={{
            width: "100%",
            fontFamily: "var(--font-mono)",
            fontSize: "0.8rem",
            borderColor:
              webhookUrl && !urlValid ? "var(--st-failed)" : undefined,
          }}
        />
        {webhookUrl && !urlValid && (
          <div
            id={webhookErrorId}
            role="alert"
            style={{
              display: "flex",
              alignItems: "center",
              gap: "var(--sp-1)",
              marginTop: "var(--sp-2)",
              color: "var(--st-failed)",
              fontSize: "0.75rem",
            }}
          >
            <AlertTriangle size={12} aria-hidden="true" />
            Must be a valid Slack webhook URL
            (https://hooks.slack.com/services/...)
          </div>
        )}
      </div>

      {error && (
        <div
          id={formErrorId}
          role="alert"
          style={{
            display: "flex",
            alignItems: "center",
            gap: "var(--sp-1)",
            color: "var(--st-failed)",
            fontSize: "0.75rem",
          }}
        >
          <AlertTriangle size={12} aria-hidden="true" />
          {error}
        </div>
      )}

      <div
        style={{
          display: "flex",
          justifyContent: "flex-end",
          gap: "var(--sp-2)",
        }}
      >
        <button className="btn btn-ghost" onClick={onCancel} disabled={saving}>
          Cancel
        </button>
        <button
          className="btn btn-primary"
          onClick={handleSubmit}
          disabled={!canSubmit}
        >
          <Plus size={13} />
          {saving ? "Creating..." : "Create"}
        </button>
      </div>
    </motion.div>
  );
}

type EndpointRowProps = {
  endpoint: Endpoint;
  busy: boolean;
  isDefault: boolean;
  onToggle: () => void;
  onDelete: () => void;
  onSetDefault: () => void;
};

function EndpointRow({
  endpoint,
  busy,
  isDefault,
  onToggle,
  onDelete,
  onSetDefault,
}: EndpointRowProps) {
  const disabled = endpoint.disabled_at !== null;

  return (
    <tr
      style={{
        background: isDefault ? "rgba(129,140,248,0.06)" : undefined,
      }}
    >
      <td
        style={{
          padding: "var(--sp-2) var(--sp-3)",
          fontFamily: "var(--font-mono)",
          fontSize: "0.75rem",
          color: "var(--text-secondary)",
          borderBottom: "1px solid var(--border-ghost)",
        }}
      >
        {endpoint.kind}
      </td>
      <td
        style={{
          padding: "var(--sp-2) var(--sp-3)",
          fontSize: "0.8rem",
          color: "var(--text-primary)",
          borderBottom: "1px solid var(--border-ghost)",
        }}
      >
        {endpoint.name}
      </td>
      <td
        style={{
          padding: "var(--sp-2) var(--sp-3)",
          borderBottom: "1px solid var(--border-ghost)",
        }}
      >
        <span
          role="status"
          aria-label={`Endpoint ${endpoint.name} is ${
            disabled ? "disabled" : "enabled"
          }`}
          style={{
            display: "inline-flex",
            alignItems: "center",
            gap: 4,
            fontFamily: "var(--font-mono)",
            fontSize: "0.7rem",
            textTransform: "uppercase",
            letterSpacing: "0.06em",
            color: disabled ? "var(--text-muted)" : "var(--st-completed)",
          }}
        >
          {disabled ? (
            <BellOff size={11} aria-hidden="true" />
          ) : (
            <Bell size={11} aria-hidden="true" />
          )}
          {disabled ? "Disabled" : "Enabled"}
        </span>
      </td>
      <td
        style={{
          padding: "var(--sp-2) var(--sp-3)",
          fontFamily: "var(--font-mono)",
          fontSize: "0.7rem",
          color: "var(--text-muted)",
          borderBottom: "1px solid var(--border-ghost)",
        }}
      >
        {formatDate(endpoint.created_at)}
      </td>
      <td
        style={{
          padding: "var(--sp-2) var(--sp-3)",
          borderBottom: "1px solid var(--border-ghost)",
          textAlign: "right",
        }}
      >
        <div
          style={{
            display: "inline-flex",
            gap: "var(--sp-2)",
          }}
        >
          <button
            onClick={onSetDefault}
            disabled={busy || disabled || isDefault}
            title={
              isDefault
                ? "This is the current default"
                : disabled
                  ? "Enable the endpoint first"
                  : "Set as default"
            }
            aria-pressed={isDefault}
            style={{
              background: "none",
              border: `1px solid ${
                isDefault ? "var(--accent, #818cf8)" : "var(--border-subtle)"
              }`,
              color: isDefault ? "var(--accent, #818cf8)" : "var(--text-muted)",
              cursor: busy || disabled || isDefault ? "not-allowed" : "pointer",
              padding: "4px 8px",
              borderRadius: "var(--radius-sm)",
              display: "inline-flex",
              alignItems: "center",
              gap: 4,
              fontSize: "0.7rem",
              fontFamily: "var(--font-mono)",
              opacity: disabled && !isDefault ? 0.5 : 1,
            }}
          >
            <Star
              size={11}
              aria-hidden="true"
              fill={isDefault ? "currentColor" : "none"}
            />
            {isDefault ? "Default" : "Set default"}
          </button>
          <button
            onClick={onToggle}
            disabled={busy}
            title={disabled ? "Enable" : "Disable"}
            aria-pressed={!disabled}
            style={{
              background: "none",
              border: "1px solid var(--border-subtle)",
              color: disabled ? "var(--st-completed)" : "var(--text-muted)",
              cursor: busy ? "not-allowed" : "pointer",
              padding: "4px 8px",
              borderRadius: "var(--radius-sm)",
              display: "inline-flex",
              alignItems: "center",
              gap: 4,
              fontSize: "0.7rem",
              fontFamily: "var(--font-mono)",
            }}
          >
            <Power size={11} aria-hidden="true" />
            {disabled ? "Enable" : "Disable"}
          </button>
          <button
            onClick={onDelete}
            disabled={busy}
            title="Delete"
            style={{
              background: "none",
              border: "1px solid var(--border-subtle)",
              color: "var(--st-failed)",
              cursor: busy ? "not-allowed" : "pointer",
              padding: "4px 8px",
              borderRadius: "var(--radius-sm)",
              display: "inline-flex",
              alignItems: "center",
              gap: 4,
              fontSize: "0.7rem",
              fontFamily: "var(--font-mono)",
            }}
          >
            <Trash2 size={11} aria-hidden="true" />
            Delete
          </button>
        </div>
      </td>
    </tr>
  );
}

export function NotificationsTab() {
  const [endpointList, setEndpointList] = useState<Endpoint[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [success, setSuccess] = useState<string | null>(null);
  const [showAdd, setShowAdd] = useState(false);
  const [busyId, setBusyId] = useState<number | null>(null);
  const [srunxConfig, setSrunxConfig] = useState<SrunxConfig | null>(null);
  const [savingConfig, setSavingConfig] = useState(false);
  const presetSelectId = useId();

  const load = useCallback(async () => {
    try {
      setLoading(true);
      const [list, cfg] = await Promise.all([
        endpointsApi.list({ include_disabled: true }),
        configApi.get(),
      ]);
      setEndpointList(list);
      setSrunxConfig(cfg);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to load endpoints");
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

  const handleCreated = (created: Endpoint) => {
    setEndpointList((prev) => [...prev, created]);
    setShowAdd(false);
    setSuccess(`Endpoint "${created.name}" created`);
  };

  const handleToggle = async (ep: Endpoint) => {
    try {
      setBusyId(ep.id);
      setError(null);
      const updated =
        ep.disabled_at === null
          ? await endpointsApi.disable(ep.id)
          : await endpointsApi.enable(ep.id);
      setEndpointList((prev) =>
        prev.map((e) => (e.id === updated.id ? updated : e)),
      );
      setSuccess(
        updated.disabled_at === null
          ? `Endpoint "${updated.name}" enabled`
          : `Endpoint "${updated.name}" disabled`,
      );
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to update endpoint");
    } finally {
      setBusyId(null);
    }
  };

  const updateNotificationConfig = async (
    patch: Partial<SrunxConfig["notifications"]>,
    successMsg: string,
  ) => {
    if (!srunxConfig) return;
    try {
      setSavingConfig(true);
      setError(null);
      const next: SrunxConfig = {
        ...srunxConfig,
        notifications: { ...srunxConfig.notifications, ...patch },
      };
      const updated = await configApi.update(next);
      setSrunxConfig(updated);
      setSuccess(successMsg);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to update config");
    } finally {
      setSavingConfig(false);
    }
  };

  const handleSetDefault = (ep: Endpoint) =>
    updateNotificationConfig(
      { default_endpoint_name: ep.name },
      `"${ep.name}" set as default endpoint`,
    );

  const handleClearDefault = () =>
    updateNotificationConfig(
      { default_endpoint_name: null },
      "Default endpoint cleared",
    );

  const handleChangePreset = (preset: NotificationPreset) =>
    updateNotificationConfig(
      { default_preset: preset },
      `Default preset set to "${preset}"`,
    );

  const handleDelete = async (ep: Endpoint) => {
    if (
      !window.confirm(
        `Delete endpoint "${ep.name}"? Existing subscriptions referencing it may be affected.`,
      )
    ) {
      return;
    }
    try {
      setBusyId(ep.id);
      setError(null);
      await endpointsApi.delete(ep.id);
      setEndpointList((prev) => prev.filter((e) => e.id !== ep.id));
      setSuccess(`Endpoint "${ep.name}" deleted`);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to delete endpoint");
    } finally {
      setBusyId(null);
    }
  };

  if (loading) {
    return <div className="panel skeleton" style={{ height: 200 }} />;
  }

  return (
    <div
      style={{ display: "flex", flexDirection: "column", gap: "var(--sp-4)" }}
    >
      <ErrorBanner error={error} />
      {success && (
        <motion.div
          role="status"
          aria-live="polite"
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
          <Check
            size={14}
            aria-hidden="true"
            style={{ verticalAlign: -2, marginRight: 6 }}
          />
          {success}
        </motion.div>
      )}

      {/* Default delivery (drives the Bell quick-toggle on Jobs rows) */}
      <div className="panel">
        <div
          className="panel-header"
          style={{
            display: "flex",
            alignItems: "center",
            justifyContent: "space-between",
          }}
        >
          <h3>
            <Star size={14} style={{ marginRight: 8, verticalAlign: -2 }} />
            Default Delivery
          </h3>
        </div>
        <div className="panel-body">
          <p
            style={{
              fontSize: "0.75rem",
              color: "var(--text-muted)",
              marginBottom: "var(--sp-3)",
            }}
          >
            The Jobs-page Bell toggle creates a watch against this endpoint and
            preset. Clear the endpoint to force explicit selection per job (from
            the job's Notifications tab).
          </p>

          <div
            style={{
              display: "grid",
              gridTemplateColumns: "repeat(auto-fit, minmax(220px, 1fr))",
              gap: "var(--sp-3)",
            }}
          >
            <div>
              <label
                style={{
                  fontFamily: "var(--font-mono)",
                  fontSize: "0.65rem",
                  color: "var(--text-muted)",
                  textTransform: "uppercase",
                  letterSpacing: "0.06em",
                  marginBottom: 4,
                  display: "block",
                }}
              >
                Default endpoint
              </label>
              <div
                style={{
                  display: "flex",
                  alignItems: "center",
                  gap: "var(--sp-2)",
                  fontFamily: "var(--font-mono)",
                  fontSize: "0.82rem",
                }}
              >
                <span
                  style={{
                    color: srunxConfig?.notifications.default_endpoint_name
                      ? "var(--text-primary)"
                      : "var(--text-muted)",
                  }}
                >
                  {srunxConfig?.notifications.default_endpoint_name ?? "—"}
                </span>
                {srunxConfig?.notifications.default_endpoint_name && (
                  <button
                    type="button"
                    className="btn btn-ghost"
                    onClick={handleClearDefault}
                    disabled={savingConfig}
                    title="Clear default"
                    style={{
                      padding: "2px 8px",
                      fontSize: "0.72rem",
                    }}
                  >
                    Clear
                  </button>
                )}
              </div>
              <p
                style={{
                  fontSize: "0.7rem",
                  color: "var(--text-muted)",
                  marginTop: 4,
                }}
              >
                Use the ★ button on an active endpoint below to set it.
              </p>
            </div>

            <div>
              <label
                htmlFor={presetSelectId}
                style={{
                  fontFamily: "var(--font-mono)",
                  fontSize: "0.65rem",
                  color: "var(--text-muted)",
                  textTransform: "uppercase",
                  letterSpacing: "0.06em",
                  marginBottom: 4,
                  display: "block",
                }}
              >
                Default preset
              </label>
              <select
                id={presetSelectId}
                className="select"
                value={srunxConfig?.notifications.default_preset ?? "terminal"}
                onChange={(e) =>
                  handleChangePreset(e.target.value as NotificationPreset)
                }
                disabled={savingConfig}
                style={{ width: "100%", fontSize: "0.82rem" }}
              >
                {PRESET_OPTIONS.map((opt) => (
                  <option key={opt.value} value={opt.value}>
                    {opt.label}
                  </option>
                ))}
              </select>
            </div>
          </div>
        </div>
      </div>

      <div className="panel">
        <div
          className="panel-header"
          style={{
            display: "flex",
            alignItems: "center",
            justifyContent: "space-between",
          }}
        >
          <h3>
            <Bell size={14} style={{ marginRight: 8, verticalAlign: -2 }} />
            Notification Endpoints
          </h3>
          {!showAdd && (
            <button
              className="btn btn-primary"
              onClick={() => setShowAdd(true)}
            >
              <Plus size={13} />
              Add endpoint
            </button>
          )}
        </div>
        <div className="panel-body">
          <p
            style={{
              fontSize: "0.75rem",
              color: "var(--text-muted)",
              marginBottom: "var(--sp-3)",
            }}
          >
            Configure delivery targets for job state notifications. Jobs can
            subscribe to individual endpoints at submit time.
          </p>

          {showAdd && (
            <div style={{ marginBottom: "var(--sp-4)" }}>
              <AddEndpointForm
                onCancel={() => setShowAdd(false)}
                onCreated={handleCreated}
              />
            </div>
          )}

          {endpointList.length === 0 ? (
            <div
              style={{
                padding: "var(--sp-4)",
                textAlign: "center",
                fontSize: "0.8rem",
                color: "var(--text-muted)",
                fontStyle: "italic",
                border: "1px dashed var(--border-subtle)",
                borderRadius: "var(--radius-md)",
              }}
            >
              No endpoints configured yet. Click "Add endpoint" to create one.
            </div>
          ) : (
            <div style={{ overflowX: "auto" }}>
              <table
                style={{
                  width: "100%",
                  borderCollapse: "collapse",
                  fontSize: "0.8rem",
                }}
              >
                <thead>
                  <tr>
                    {["Kind", "Name", "Status", "Created", ""].map((h, i) => (
                      <th
                        key={h || `col-${i}`}
                        style={{
                          padding: "var(--sp-2) var(--sp-3)",
                          textAlign: i === 4 ? "right" : "left",
                          fontFamily: "var(--font-display)",
                          fontSize: "0.65rem",
                          fontWeight: 600,
                          textTransform: "uppercase",
                          letterSpacing: "0.08em",
                          color: "var(--text-muted)",
                          borderBottom: "1px solid var(--border-default)",
                        }}
                      >
                        {h}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {endpointList.map((ep) => (
                    <EndpointRow
                      key={ep.id}
                      endpoint={ep}
                      busy={busyId === ep.id || savingConfig}
                      isDefault={
                        srunxConfig?.notifications.default_endpoint_name ===
                        ep.name
                      }
                      onToggle={() => handleToggle(ep)}
                      onDelete={() => handleDelete(ep)}
                      onSetDefault={() => handleSetDefault(ep)}
                    />
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
