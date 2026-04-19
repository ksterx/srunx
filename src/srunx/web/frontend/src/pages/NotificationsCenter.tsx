import { motion } from "framer-motion";
import {
  Bell,
  AlertTriangle,
  CheckCircle2,
  XCircle,
  Clock,
} from "lucide-react";
import { useMemo, useState } from "react";
import { useApi } from "../hooks/use-api.ts";
import {
  deliveries as deliveriesApi,
  endpoints as endpointsApi,
  subscriptions as subscriptionsApi,
  watches as watchesApi,
} from "../lib/api.ts";
import type {
  Delivery,
  DeliveryStatus,
  Endpoint,
  Subscription,
  Watch,
} from "../lib/types.ts";
import { ErrorBanner } from "../components/ErrorBanner.tsx";

const EASE = [0.16, 1, 0.3, 1] as [number, number, number, number];

const stagger = (i: number) => ({
  initial: { opacity: 0, y: 12 },
  animate: { opacity: 1, y: 0 },
  transition: { delay: i * 0.06, duration: 0.4, ease: EASE },
});

const STATUS_FILTERS: (DeliveryStatus | "all")[] = [
  "all",
  "pending",
  "sending",
  "delivered",
  "abandoned",
];

const STATUS_COLOR: Record<DeliveryStatus, string> = {
  pending: "var(--st-pending)",
  sending: "var(--st-running)",
  delivered: "var(--st-completed)",
  abandoned: "var(--st-failed)",
};

const STATUS_ICON: Record<DeliveryStatus, React.ReactNode> = {
  pending: <Clock size={12} />,
  sending: <Clock size={12} />,
  delivered: <CheckCircle2 size={12} />,
  abandoned: <XCircle size={12} />,
};

function formatIso(iso: string | null): string {
  if (!iso) return "—";
  const dt = new Date(iso);
  if (Number.isNaN(dt.getTime())) return iso;
  return dt.toLocaleString(undefined, {
    year: "numeric",
    month: "short",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
  });
}

export function NotificationsCenter() {
  const [statusFilter, setStatusFilter] = useState<DeliveryStatus | "all">(
    "all",
  );

  const { data: deliveries, error: deliveriesError } = useApi<Delivery[]>(
    () =>
      deliveriesApi.listRecent({
        status: statusFilter === "all" ? undefined : statusFilter,
        limit: 100,
      }),
    [statusFilter],
    { pollInterval: 10000 },
  );

  const { data: stuck } = useApi(() => deliveriesApi.countStuck(), [], {
    pollInterval: 10000,
  });

  const { data: watches, error: watchesError } = useApi<Watch[]>(
    () => watchesApi.list({ open: true }),
    [],
    { pollInterval: 15000 },
  );

  const { data: endpoints } = useApi<Endpoint[]>(
    () => endpointsApi.list(),
    [],
    {},
  );

  const { data: subscriptions, error: subscriptionsError } = useApi<
    Subscription[]
  >(() => subscriptionsApi.list({}), [], { pollInterval: 30000 });

  const endpointById = useMemo(() => {
    const m = new Map<number, Endpoint>();
    for (const e of endpoints ?? []) m.set(e.id, e);
    return m;
  }, [endpoints]);

  const watchById = useMemo(() => {
    const m = new Map<number, Watch>();
    for (const w of watches ?? []) m.set(w.id, w);
    return m;
  }, [watches]);

  const anyError = deliveriesError || watchesError || subscriptionsError;

  return (
    <div
      style={{ display: "flex", flexDirection: "column", gap: "var(--sp-6)" }}
    >
      <motion.div {...stagger(0)}>
        <h1 style={{ marginBottom: 4 }}>
          <Bell size={20} style={{ verticalAlign: -3, marginRight: 8 }} />
          Notifications
        </h1>
        <p style={{ color: "var(--text-muted)", fontSize: "0.85rem" }}>
          Real-time view of the notification outbox, open watches, and
          subscription fan-out.
        </p>
      </motion.div>

      <ErrorBanner error={anyError ?? null} />

      {/* Top stats row */}
      <motion.div
        {...stagger(1)}
        style={{
          display: "grid",
          gridTemplateColumns: "repeat(auto-fill, minmax(220px, 1fr))",
          gap: "var(--sp-4)",
        }}
      >
        <StatCard
          label="Open watches"
          value={watches?.length ?? "—"}
          icon={<Bell size={16} />}
        />
        <StatCard
          label="Active subscriptions"
          value={subscriptions?.length ?? "—"}
          icon={<Bell size={16} />}
        />
        <StatCard
          label="Stuck pending (>5 min)"
          value={stuck?.count ?? "—"}
          icon={<AlertTriangle size={16} />}
          accent={
            stuck && stuck.count > 0 ? "var(--st-failed)" : "var(--text-muted)"
          }
        />
      </motion.div>

      {/* Recent deliveries */}
      <motion.section {...stagger(2)} className="panel">
        <div
          className="panel-header"
          style={{
            display: "flex",
            alignItems: "center",
            justifyContent: "space-between",
          }}
        >
          <h3>Recent deliveries</h3>
          <div style={{ display: "flex", gap: 6 }}>
            {STATUS_FILTERS.map((s) => {
              const active = s === statusFilter;
              return (
                <button
                  key={s}
                  onClick={() => setStatusFilter(s)}
                  style={{
                    padding: "4px 10px",
                    borderRadius: 4,
                    fontSize: "0.72rem",
                    fontFamily: "var(--font-mono)",
                    border: `1px solid ${active ? "var(--accent)" : "var(--border-subtle)"}`,
                    background: active ? "var(--accent-dim)" : "transparent",
                    color: active ? "var(--accent)" : "var(--text-secondary)",
                    cursor: "pointer",
                    textTransform: "lowercase",
                  }}
                >
                  {s}
                </button>
              );
            })}
          </div>
        </div>

        <DeliveriesTable
          deliveries={deliveries ?? []}
          endpointById={endpointById}
        />
      </motion.section>

      {/* Open watches */}
      <motion.section {...stagger(3)} className="panel">
        <div className="panel-header">
          <h3>Open watches ({watches?.length ?? 0})</h3>
        </div>
        <WatchesTable watches={watches ?? []} />
      </motion.section>

      {/* Subscriptions */}
      <motion.section {...stagger(4)} className="panel">
        <div className="panel-header">
          <h3>Subscriptions ({subscriptions?.length ?? 0})</h3>
        </div>
        <SubscriptionsTable
          subscriptions={subscriptions ?? []}
          endpointById={endpointById}
          watchById={watchById}
        />
      </motion.section>
    </div>
  );
}

type StatCardProps = {
  label: string;
  value: number | string;
  icon: React.ReactNode;
  accent?: string;
};

function StatCard({ label, value, icon, accent }: StatCardProps) {
  return (
    <div
      className="panel"
      style={{
        display: "flex",
        flexDirection: "column",
        gap: 4,
        padding: "var(--sp-4)",
      }}
    >
      <div
        style={{
          display: "flex",
          alignItems: "center",
          gap: 6,
          color: accent ?? "var(--text-muted)",
          fontSize: "0.72rem",
          textTransform: "uppercase",
          letterSpacing: "0.08em",
        }}
      >
        {icon}
        <span>{label}</span>
      </div>
      <div
        style={{
          fontFamily: "var(--font-display)",
          fontSize: "1.6rem",
          color: accent ?? "var(--text-primary)",
        }}
      >
        {value}
      </div>
    </div>
  );
}

type DeliveriesTableProps = {
  deliveries: Delivery[];
  endpointById: Map<number, Endpoint>;
};

function DeliveriesTable({ deliveries, endpointById }: DeliveriesTableProps) {
  if (deliveries.length === 0) {
    return <EmptyState message="No deliveries yet." />;
  }
  return (
    <div style={{ overflowX: "auto" }}>
      <table
        style={{
          width: "100%",
          borderCollapse: "collapse",
          fontSize: "0.8rem",
        }}
      >
        <thead>
          <tr style={{ color: "var(--text-muted)", textAlign: "left" }}>
            <Th>Status</Th>
            <Th>Endpoint</Th>
            <Th>Attempts</Th>
            <Th>Created</Th>
            <Th>Delivered / next try</Th>
            <Th>Last error</Th>
          </tr>
        </thead>
        <tbody>
          {deliveries.map((d) => {
            const endpoint = endpointById.get(d.endpoint_id);
            return (
              <tr
                key={d.id}
                style={{ borderTop: "1px solid var(--border-ghost)" }}
              >
                <Td>
                  <StatusChip status={d.status} />
                </Td>
                <Td>
                  <span style={{ fontFamily: "var(--font-mono)" }}>
                    {endpoint
                      ? `${endpoint.kind}:${endpoint.name}`
                      : `#${d.endpoint_id}`}
                  </span>
                </Td>
                <Td>{d.attempt_count}</Td>
                <Td>{formatIso(d.created_at)}</Td>
                <Td>
                  {d.delivered_at
                    ? formatIso(d.delivered_at)
                    : formatIso(d.next_attempt_at)}
                </Td>
                <Td>
                  <span
                    title={d.last_error ?? ""}
                    style={{
                      color: d.last_error
                        ? "var(--st-failed)"
                        : "var(--text-muted)",
                      maxWidth: 320,
                      display: "inline-block",
                      overflow: "hidden",
                      textOverflow: "ellipsis",
                      whiteSpace: "nowrap",
                    }}
                  >
                    {d.last_error ?? "—"}
                  </span>
                </Td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

type WatchesTableProps = { watches: Watch[] };

function WatchesTable({ watches }: WatchesTableProps) {
  if (watches.length === 0) {
    return <EmptyState message="No open watches." />;
  }
  return (
    <div style={{ overflowX: "auto" }}>
      <table
        style={{
          width: "100%",
          borderCollapse: "collapse",
          fontSize: "0.8rem",
        }}
      >
        <thead>
          <tr style={{ color: "var(--text-muted)", textAlign: "left" }}>
            <Th>Kind</Th>
            <Th>Target</Th>
            <Th>Created</Th>
          </tr>
        </thead>
        <tbody>
          {watches.map((w) => (
            <tr
              key={w.id}
              style={{ borderTop: "1px solid var(--border-ghost)" }}
            >
              <Td>
                <span style={{ fontFamily: "var(--font-mono)" }}>{w.kind}</span>
              </Td>
              <Td>
                <span style={{ fontFamily: "var(--font-mono)" }}>
                  {w.target_ref}
                </span>
              </Td>
              <Td>{formatIso(w.created_at)}</Td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

type SubscriptionsTableProps = {
  subscriptions: Subscription[];
  endpointById: Map<number, Endpoint>;
  watchById: Map<number, Watch>;
};

function SubscriptionsTable({
  subscriptions,
  endpointById,
  watchById,
}: SubscriptionsTableProps) {
  if (subscriptions.length === 0) {
    return <EmptyState message="No subscriptions configured." />;
  }
  return (
    <div style={{ overflowX: "auto" }}>
      <table
        style={{
          width: "100%",
          borderCollapse: "collapse",
          fontSize: "0.8rem",
        }}
      >
        <thead>
          <tr style={{ color: "var(--text-muted)", textAlign: "left" }}>
            <Th>Watch</Th>
            <Th>Target</Th>
            <Th>Endpoint</Th>
            <Th>Preset</Th>
            <Th>Created</Th>
          </tr>
        </thead>
        <tbody>
          {subscriptions.map((s) => {
            const endpoint = endpointById.get(s.endpoint_id);
            const watch = watchById.get(s.watch_id);
            return (
              <tr
                key={s.id}
                style={{ borderTop: "1px solid var(--border-ghost)" }}
              >
                <Td>
                  <span style={{ fontFamily: "var(--font-mono)" }}>
                    {watch ? watch.kind : `#${s.watch_id}`}
                  </span>
                </Td>
                <Td>
                  <span style={{ fontFamily: "var(--font-mono)" }}>
                    {watch ? watch.target_ref : "—"}
                  </span>
                </Td>
                <Td>
                  <span style={{ fontFamily: "var(--font-mono)" }}>
                    {endpoint
                      ? `${endpoint.kind}:${endpoint.name}`
                      : `#${s.endpoint_id}`}
                  </span>
                </Td>
                <Td>
                  <span
                    style={{
                      padding: "2px 8px",
                      borderRadius: 4,
                      fontSize: "0.7rem",
                      fontFamily: "var(--font-mono)",
                      background: "var(--bg-base)",
                      border: "1px solid var(--border-subtle)",
                    }}
                  >
                    {s.preset}
                  </span>
                </Td>
                <Td>{formatIso(s.created_at)}</Td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

function StatusChip({ status }: { status: DeliveryStatus }) {
  return (
    <span
      style={{
        display: "inline-flex",
        alignItems: "center",
        gap: 4,
        padding: "2px 8px",
        borderRadius: 4,
        fontSize: "0.72rem",
        fontFamily: "var(--font-mono)",
        color: STATUS_COLOR[status],
        background: "var(--bg-base)",
        border: `1px solid ${STATUS_COLOR[status]}`,
      }}
    >
      {STATUS_ICON[status]}
      {status}
    </span>
  );
}

function Th({ children }: { children: React.ReactNode }) {
  return (
    <th
      style={{
        padding: "8px 10px",
        fontWeight: 500,
        fontSize: "0.72rem",
        textTransform: "uppercase",
        letterSpacing: "0.08em",
      }}
    >
      {children}
    </th>
  );
}

function Td({ children }: { children: React.ReactNode }) {
  return (
    <td
      style={{
        padding: "8px 10px",
        color: "var(--text-secondary)",
      }}
    >
      {children}
    </td>
  );
}

function EmptyState({ message }: { message: string }) {
  return (
    <div
      style={{
        padding: "var(--sp-6)",
        textAlign: "center",
        color: "var(--text-muted)",
        fontSize: "0.85rem",
      }}
    >
      {message}
    </div>
  );
}
