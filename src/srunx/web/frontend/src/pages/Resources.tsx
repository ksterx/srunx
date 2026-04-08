import { motion } from "framer-motion";
import { ErrorBanner } from "../components/ErrorBanner.tsx";
import {
  AreaChart,
  Area,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
} from "recharts";
import { Cpu, Server, HardDrive } from "lucide-react";
import { useApi } from "../hooks/use-api.ts";
import { resources as resourcesApi } from "../lib/api.ts";
import { ResourceGauge } from "../components/ResourceGauge.tsx";

const EASE = [0.16, 1, 0.3, 1] as [number, number, number, number];

const stagger = (i: number) => ({
  initial: { opacity: 0, y: 12 },
  animate: { opacity: 1, y: 0 },
  transition: { delay: i * 0.06, duration: 0.4, ease: EASE },
});

export function Resources() {
  const { data: snapshots, error } = useApi(() => resourcesApi.snapshot(), [], {
    pollInterval: 10000,
  });

  /* Chart data from current polling snapshot */
  const chartData = snapshots
    ? snapshots.map((s) => ({
        time: s.partition ?? "all",
        used: s.gpus_in_use,
        available: s.gpus_available,
        total: s.total_gpus,
        utilization: Math.round(s.gpu_utilization * 100),
      }))
    : [];

  return (
    <div
      style={{ display: "flex", flexDirection: "column", gap: "var(--sp-6)" }}
    >
      <motion.div {...stagger(0)}>
        <h1 style={{ marginBottom: 4 }}>Resources</h1>
        <p style={{ color: "var(--text-muted)", fontSize: "0.85rem" }}>
          Real-time GPU and node availability across partitions
        </p>
      </motion.div>

      <ErrorBanner error={error} />

      {/* Partition cards */}
      <div
        style={{
          display: "grid",
          gridTemplateColumns: "repeat(auto-fill, minmax(280px, 1fr))",
          gap: "var(--sp-4)",
        }}
      >
        {snapshots?.map((snap, i) => (
          <motion.div
            key={snap.partition ?? `partition-${i}`}
            {...stagger(i + 1)}
            className="panel"
          >
            <div className="panel-header">
              <h3>
                <HardDrive
                  size={14}
                  style={{ marginRight: 8, verticalAlign: -2 }}
                />
                {snap.partition ?? "all"}
              </h3>
              <span
                style={{
                  fontFamily: "var(--font-mono)",
                  fontSize: "0.7rem",
                  color: snap.has_available_gpus
                    ? "var(--st-completed)"
                    : "var(--st-failed)",
                }}
              >
                {snap.has_available_gpus ? "AVAILABLE" : "FULL"}
              </span>
            </div>
            <div
              className="panel-body"
              style={{ display: "flex", flexDirection: "column", gap: 20 }}
            >
              {/* GPU gauge */}
              <ResourceGauge
                label="GPUs"
                used={snap.gpus_in_use}
                total={snap.total_gpus}
                unit="GPU"
              />

              {/* Node stats */}
              <div
                style={{
                  display: "grid",
                  gridTemplateColumns: "1fr 1fr 1fr",
                  gap: 8,
                }}
              >
                {[
                  {
                    label: "Total Nodes",
                    value: snap.nodes_total,
                    icon: <Server size={12} />,
                    color: "var(--text-primary)",
                  },
                  {
                    label: "Idle",
                    value: snap.nodes_idle,
                    icon: <Cpu size={12} />,
                    color: "var(--st-completed)",
                  },
                  {
                    label: "Down",
                    value: snap.nodes_down,
                    icon: <Cpu size={12} />,
                    color:
                      snap.nodes_down > 0
                        ? "var(--st-failed)"
                        : "var(--text-muted)",
                  },
                ].map((stat) => (
                  <div
                    key={stat.label}
                    style={{
                      padding: "10px 8px",
                      background: "var(--bg-base)",
                      borderRadius: 6,
                      border: "1px solid var(--border-ghost)",
                      textAlign: "center",
                    }}
                  >
                    <div
                      style={{
                        color: "var(--text-muted)",
                        marginBottom: 4,
                        display: "flex",
                        alignItems: "center",
                        justifyContent: "center",
                        gap: 4,
                      }}
                    >
                      {stat.icon}
                      <span
                        style={{
                          fontSize: "0.6rem",
                          textTransform: "uppercase",
                          letterSpacing: "0.08em",
                        }}
                      >
                        {stat.label}
                      </span>
                    </div>
                    <span
                      style={{
                        fontFamily: "var(--font-mono)",
                        fontSize: "1.1rem",
                        fontWeight: 600,
                        color: stat.color,
                      }}
                    >
                      {stat.value}
                    </span>
                  </div>
                ))}
              </div>

              {/* Utilization percentage */}
              <div
                style={{
                  display: "flex",
                  justifyContent: "space-between",
                  alignItems: "baseline",
                  paddingTop: 8,
                  borderTop: "1px solid var(--border-ghost)",
                }}
              >
                <span className="metric-label">Utilization</span>
                <span
                  style={{
                    fontFamily: "var(--font-mono)",
                    fontSize: "1.3rem",
                    fontWeight: 600,
                    color: "var(--resource)",
                  }}
                >
                  {Math.round(snap.gpu_utilization * 100)}%
                </span>
              </div>
            </div>
          </motion.div>
        ))}

        {!snapshots &&
          Array.from({ length: 2 }).map((_, i) => (
            <div key={i} className="panel skeleton" style={{ height: 300 }} />
          ))}
      </div>

      {/* GPU Utilization chart */}
      {chartData.length > 1 && (
        <motion.div
          {...stagger(5)}
          className="panel"
          style={{ overflow: "hidden" }}
        >
          <div className="panel-header">
            <h3>GPU Utilization Over Time</h3>
          </div>
          <div className="panel-body" style={{ height: 280 }}>
            <ResponsiveContainer width="100%" height="100%">
              <AreaChart data={chartData}>
                <defs>
                  <linearGradient id="gpuGradient" x1="0" y1="0" x2="0" y2="1">
                    <stop
                      offset="0%"
                      stopColor="var(--resource)"
                      stopOpacity={0.3}
                    />
                    <stop
                      offset="100%"
                      stopColor="var(--resource)"
                      stopOpacity={0}
                    />
                  </linearGradient>
                </defs>
                <CartesianGrid
                  strokeDasharray="3 3"
                  stroke="var(--border-ghost)"
                />
                <XAxis
                  dataKey="time"
                  tick={{ fontSize: 10, fill: "var(--text-muted)" }}
                  stroke="var(--border-subtle)"
                />
                <YAxis
                  tick={{ fontSize: 10, fill: "var(--text-muted)" }}
                  stroke="var(--border-subtle)"
                  domain={[0, "dataMax"]}
                />
                <Tooltip
                  contentStyle={{
                    background: "var(--bg-raised)",
                    border: "1px solid var(--border-default)",
                    borderRadius: 6,
                    fontFamily: "var(--font-mono)",
                    fontSize: "0.75rem",
                  }}
                  labelStyle={{ color: "var(--text-secondary)" }}
                />
                <Area
                  type="monotone"
                  dataKey="used"
                  stroke="var(--resource)"
                  strokeWidth={2}
                  fill="url(#gpuGradient)"
                  name="GPUs In Use"
                />
              </AreaChart>
            </ResponsiveContainer>
          </div>
        </motion.div>
      )}
    </div>
  );
}
