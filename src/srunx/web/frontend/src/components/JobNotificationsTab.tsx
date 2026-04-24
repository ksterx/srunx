import { useCallback, useEffect, useId, useMemo, useState } from "react";
import { motion } from "framer-motion";
import { BellOff, Loader2, X } from "lucide-react";
import {
  endpoints as endpointsApi,
  subscriptions as subscriptionsApi,
  watches as watchesApi,
} from "../lib/api.ts";
import type {
  Endpoint,
  NotificationPreset,
  Subscription,
  Watch,
} from "../lib/types.ts";

type JobNotificationsTabProps = {
  jobId: number;
};

type WatchRow = {
  watch: Watch;
  subscriptions: Subscription[];
};

// Presets that the backend accepts for new subscriptions. ``digest`` is
// schema-valid but has no delivery implementation yet — the backend
// rejects it with 422. Keep the UI list in lock-step with
// ``ACCEPTED_PRESETS`` in ``srunx.observability.notifications.attach``.
const PRESET_OPTIONS: ReadonlyArray<{
  value: NotificationPreset;
  label: string;
  description: string;
}> = [
  {
    value: "terminal",
    label: "Terminal only",
    description:
      "One message when the job finishes (completed / failed / cancelled / timeout).",
  },
  {
    value: "running_and_terminal",
    label: "Running + terminal",
    description:
      "One message when the job starts running, and one when it finishes.",
  },
  {
    value: "all",
    label: "All state changes",
    description: "Notify on every state transition observed.",
  },
];

export function JobNotificationsTab({ jobId }: JobNotificationsTabProps) {
  const endpointSelectId = useId();
  const presetSelectId = useId();

  const [endpoints, setEndpoints] = useState<Endpoint[] | null>(null);
  const [rows, setRows] = useState<WatchRow[] | null>(null);
  const [selectedEndpointId, setSelectedEndpointId] = useState<number | "">("");
  const [selectedPreset, setSelectedPreset] =
    useState<NotificationPreset>("terminal");
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [success, setSuccess] = useState<string | null>(null);

  const reload = useCallback(async () => {
    try {
      const [eps, allLocalWatches, allSshWatches] = await Promise.all([
        endpointsApi.list({ include_disabled: false }),
        watchesApi.list({
          kind: "job",
          target_ref: `job:local:${jobId}`,
          open: true,
        }),
        // The frontend doesn't know which ``scheduler_key`` the job lives
        // under. The scoped fetch above covers ``job:local:<id>``; this
        // second fetch pulls every open ``kind=job`` watch and client-
        // filters to SSH watches whose ``target_ref`` ends with ``:<id>``.
        // The leading colon prevents ``:12`` from matching ``:512``.
        watchesApi
          .list({ kind: "job", open: true })
          .then((all) =>
            all.filter(
              (w) =>
                /^job:ssh:[^:]+:/.test(w.target_ref) &&
                w.target_ref.endsWith(`:${jobId}`),
            ),
          ),
      ]);
      const jobWatches = [...allLocalWatches, ...allSshWatches];
      const subsByWatch = await Promise.all(
        jobWatches.map((w) =>
          subscriptionsApi.list({ watch_id: w.id }).then((subs) => ({
            watch: w,
            subscriptions: subs,
          })),
        ),
      );
      setEndpoints(eps);
      setRows(subsByWatch);
      setError(null);
    } catch (err) {
      setError(
        err instanceof Error ? err.message : "Failed to load notifications",
      );
    }
  }, [jobId]);

  useEffect(() => {
    reload();
  }, [reload]);

  const enabledEndpoints = useMemo(
    () => (endpoints ?? []).filter((e) => e.disabled_at === null),
    [endpoints],
  );

  const handleEnable = async () => {
    if (selectedEndpointId === "") return;
    setSaving(true);
    setError(null);
    setSuccess(null);
    try {
      const result = await watchesApi.createForJob({
        job_id: jobId,
        endpoint_id: selectedEndpointId,
        preset: selectedPreset,
      });
      setSuccess(
        result.created
          ? "Notification enabled."
          : "Already enabled — reusing the existing subscription.",
      );
      await reload();
    } catch (err) {
      setError(
        err instanceof Error ? err.message : "Failed to enable notification",
      );
    } finally {
      setSaving(false);
    }
  };

  const handleUnsubscribe = async (subscriptionId: number) => {
    if (
      !window.confirm(
        "Remove this notification subscription? The watch will remain open for any other subscriptions.",
      )
    ) {
      return;
    }
    setSaving(true);
    setError(null);
    setSuccess(null);
    try {
      await subscriptionsApi.delete(subscriptionId);
      setSuccess("Subscription removed.");
      await reload();
    } catch (err) {
      setError(
        err instanceof Error ? err.message : "Failed to remove subscription",
      );
    } finally {
      setSaving(false);
    }
  };

  const handleCloseWatch = async (watchId: number) => {
    if (
      !window.confirm(
        "Close this watch? All subscriptions on it will stop producing deliveries.",
      )
    ) {
      return;
    }
    setSaving(true);
    setError(null);
    setSuccess(null);
    try {
      await watchesApi.close(watchId);
      setSuccess("Watch closed.");
      await reload();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to close watch");
    } finally {
      setSaving(false);
    }
  };

  const endpointNameById = useMemo(() => {
    const map = new Map<number, string>();
    for (const e of endpoints ?? []) map.set(e.id, e.name);
    return map;
  }, [endpoints]);

  const totalSubscriptions = (rows ?? []).reduce(
    (acc, r) => acc + r.subscriptions.length,
    0,
  );

  return (
    <motion.div
      initial={{ opacity: 0, y: 8 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.2 }}
      className="panel"
    >
      <div
        className="panel-body"
        style={{
          display: "flex",
          flexDirection: "column",
          gap: "var(--sp-4)",
        }}
      >
        <p
          style={{
            margin: 0,
            color: "var(--text-muted)",
            fontSize: "0.78rem",
          }}
        >
          {totalSubscriptions === 0
            ? "No notifications configured for this job yet."
            : `${totalSubscriptions} active subscription${
                totalSubscriptions === 1 ? "" : "s"
              }.`}
        </p>

        {error && (
          <div
            style={{
              padding: "var(--sp-2) var(--sp-3)",
              background: "var(--st-failed-dim)",
              border: "1px solid rgba(244,63,94,0.3)",
              borderRadius: "var(--radius-md)",
              color: "var(--st-failed)",
              fontSize: "0.8rem",
            }}
          >
            {error}
          </div>
        )}
        {success && (
          <div
            style={{
              padding: "var(--sp-2) var(--sp-3)",
              background: "var(--st-completed-dim)",
              border: "1px solid rgba(34,197,94,0.3)",
              borderRadius: "var(--radius-md)",
              color: "var(--st-completed)",
              fontSize: "0.8rem",
            }}
          >
            {success}
          </div>
        )}

        <section
          style={{
            display: "flex",
            flexDirection: "column",
            gap: "var(--sp-2)",
          }}
        >
          <h3
            style={{
              margin: 0,
              fontSize: "0.82rem",
              color: "var(--text-muted)",
            }}
          >
            Active subscriptions
          </h3>
          {rows === null ? (
            <div style={{ color: "var(--text-muted)", fontSize: "0.8rem" }}>
              Loading…
            </div>
          ) : totalSubscriptions === 0 ? (
            <div style={{ color: "var(--text-muted)", fontSize: "0.8rem" }}>
              None yet — pick an endpoint + preset below and click Enable.
            </div>
          ) : (
            <ul
              style={{
                margin: 0,
                padding: 0,
                listStyle: "none",
                display: "flex",
                flexDirection: "column",
                gap: "var(--sp-1)",
              }}
            >
              {rows.flatMap((row) =>
                row.subscriptions.map((sub) => (
                  <li
                    key={sub.id}
                    style={{
                      display: "flex",
                      alignItems: "center",
                      justifyContent: "space-between",
                      gap: "var(--sp-2)",
                      padding: "var(--sp-2) var(--sp-3)",
                      border: "1px solid var(--border-subtle)",
                      borderRadius: "var(--radius-md)",
                    }}
                  >
                    <div style={{ display: "flex", flexDirection: "column" }}>
                      <span style={{ fontWeight: 500, fontSize: "0.85rem" }}>
                        {endpointNameById.get(sub.endpoint_id) ??
                          `endpoint #${sub.endpoint_id}`}
                      </span>
                      <span
                        style={{
                          color: "var(--text-muted)",
                          fontSize: "0.72rem",
                          fontFamily: "var(--font-mono)",
                        }}
                      >
                        preset: {sub.preset} · watch #{row.watch.id}
                      </span>
                    </div>
                    <div style={{ display: "flex", gap: 4 }}>
                      <button
                        type="button"
                        className="btn btn-ghost"
                        onClick={() => handleUnsubscribe(sub.id)}
                        disabled={saving}
                        title="Remove this subscription"
                        style={{ padding: "4px 8px" }}
                      >
                        <X size={12} />
                      </button>
                      <button
                        type="button"
                        className="btn btn-ghost"
                        onClick={() => handleCloseWatch(row.watch.id)}
                        disabled={saving}
                        title="Close the whole watch (stops every subscription under it)"
                        style={{ padding: "4px 8px" }}
                      >
                        <BellOff size={12} />
                      </button>
                    </div>
                  </li>
                )),
              )}
            </ul>
          )}
        </section>

        <section
          style={{
            display: "flex",
            flexDirection: "column",
            gap: "var(--sp-3)",
            paddingTop: "var(--sp-3)",
            borderTop: "1px solid var(--border-ghost)",
          }}
        >
          <h3
            style={{
              margin: 0,
              fontSize: "0.82rem",
              color: "var(--text-muted)",
            }}
          >
            Enable notifications
          </h3>
          {enabledEndpoints.length === 0 ? (
            <div
              style={{
                color: "var(--text-muted)",
                fontSize: "0.8rem",
              }}
            >
              No enabled endpoints configured. Add one in Settings →
              Notifications.
            </div>
          ) : (
            <>
              <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
                <label
                  htmlFor={endpointSelectId}
                  style={{ fontSize: "0.75rem", color: "var(--text-muted)" }}
                >
                  Endpoint
                </label>
                <select
                  id={endpointSelectId}
                  className="select"
                  value={selectedEndpointId}
                  onChange={(e) =>
                    setSelectedEndpointId(
                      e.target.value === "" ? "" : Number(e.target.value),
                    )
                  }
                  disabled={saving}
                >
                  <option value="">— select an endpoint —</option>
                  {enabledEndpoints.map((ep) => (
                    <option key={ep.id} value={ep.id}>
                      {ep.name} ({ep.kind})
                    </option>
                  ))}
                </select>
              </div>

              <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
                <label
                  htmlFor={presetSelectId}
                  style={{ fontSize: "0.75rem", color: "var(--text-muted)" }}
                >
                  Preset
                </label>
                <select
                  id={presetSelectId}
                  className="select"
                  value={selectedPreset}
                  onChange={(e) =>
                    setSelectedPreset(e.target.value as NotificationPreset)
                  }
                  disabled={saving}
                >
                  {PRESET_OPTIONS.map((opt) => (
                    <option key={opt.value} value={opt.value}>
                      {opt.label}
                    </option>
                  ))}
                </select>
                <span
                  style={{
                    fontSize: "0.72rem",
                    color: "var(--text-muted)",
                  }}
                >
                  {
                    PRESET_OPTIONS.find((p) => p.value === selectedPreset)
                      ?.description
                  }
                </span>
              </div>

              <div style={{ display: "flex", justifyContent: "flex-end" }}>
                <button
                  type="button"
                  className="btn btn-primary"
                  onClick={handleEnable}
                  disabled={saving || selectedEndpointId === ""}
                  style={{ display: "flex", alignItems: "center", gap: 6 }}
                >
                  {saving && <Loader2 size={12} className="spin" />}
                  Enable
                </button>
              </div>
            </>
          )}
        </section>
      </div>
    </motion.div>
  );
}
