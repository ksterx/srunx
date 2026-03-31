import { useState, useEffect, useCallback } from "react";
import { motion } from "framer-motion";
import { Bell, Save, Check, AlertTriangle } from "lucide-react";
import { config as configApi } from "../../lib/api.ts";
import type { SrunxConfig } from "../../lib/types.ts";

const SLACK_WEBHOOK_PATTERN =
  /^https:\/\/hooks\.slack\.com\/services\/[A-Za-z0-9_-]+\/[A-Za-z0-9_-]+\/[A-Za-z0-9_-]+$/;

export function NotificationsTab() {
  const [cfg, setCfg] = useState<SrunxConfig | null>(null);
  const [webhookUrl, setWebhookUrl] = useState("");
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [success, setSuccess] = useState<string | null>(null);

  const load = useCallback(async () => {
    try {
      setLoading(true);
      const c = await configApi.get();
      setCfg(c);
      setWebhookUrl(c.notifications?.slack_webhook_url ?? "");
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to load config");
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

  const isValid = !webhookUrl || SLACK_WEBHOOK_PATTERN.test(webhookUrl);

  const handleSave = async () => {
    if (!cfg || !isValid) return;
    try {
      setSaving(true);
      setError(null);
      const updated = await configApi.update({
        ...cfg,
        notifications: {
          slack_webhook_url: webhookUrl || null,
        },
      });
      setCfg(updated);
      setSuccess("Notification settings saved");
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to save");
    } finally {
      setSaving(false);
    }
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

      <div className="panel">
        <div className="panel-header">
          <h3>
            <Bell size={14} style={{ marginRight: 8, verticalAlign: -2 }} />
            Slack
          </h3>
        </div>
        <div className="panel-body">
          <div style={{ marginBottom: "var(--sp-3)" }}>
            <label
              style={{
                fontSize: "0.85rem",
                fontWeight: 500,
                color: "var(--text-primary)",
                display: "block",
                marginBottom: 4,
              }}
            >
              Webhook URL
            </label>
            <p
              style={{
                fontSize: "0.75rem",
                color: "var(--text-muted)",
                marginBottom: "var(--sp-3)",
              }}
            >
              Used for job completion/failure notifications and scheduled
              reports.
            </p>
            <input
              className="input"
              type="url"
              value={webhookUrl}
              onChange={(e) => setWebhookUrl(e.target.value)}
              placeholder="https://hooks.slack.com/services/..."
              style={{
                width: "100%",
                fontFamily: "var(--font-mono)",
                fontSize: "0.8rem",
                borderColor:
                  webhookUrl && !isValid ? "var(--st-failed)" : undefined,
              }}
            />
            {webhookUrl && !isValid && (
              <div
                style={{
                  display: "flex",
                  alignItems: "center",
                  gap: "var(--sp-1)",
                  marginTop: "var(--sp-2)",
                  color: "var(--st-failed)",
                  fontSize: "0.75rem",
                }}
              >
                <AlertTriangle size={12} />
                Must be a valid Slack webhook URL
                (https://hooks.slack.com/services/...)
              </div>
            )}
          </div>

          <div style={{ display: "flex", justifyContent: "flex-end" }}>
            <button
              className="btn btn-primary"
              onClick={handleSave}
              disabled={saving || !isValid}
            >
              <Save size={14} />
              {saving ? "Saving..." : "Save"}
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}
