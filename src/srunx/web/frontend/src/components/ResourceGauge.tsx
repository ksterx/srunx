import { motion } from "framer-motion";

type ResourceGaugeProps = {
  label: string;
  used: number;
  total: number;
  unit?: string;
  color?: string;
};

export function ResourceGauge({
  label,
  used,
  total,
  unit = "",
  color = "var(--resource)",
}: ResourceGaugeProps) {
  const pct = total > 0 ? (used / total) * 100 : 0;
  const available = total - used;

  /* Color shifts at thresholds */
  const barColor =
    pct > 90 ? "var(--st-failed)" : pct > 70 ? "var(--st-timeout)" : color;

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
      <div
        style={{
          display: "flex",
          justifyContent: "space-between",
          alignItems: "baseline",
        }}
      >
        <span className="metric-label" style={{ fontSize: "0.72rem" }}>
          {label}
        </span>
        <span
          style={{
            fontFamily: "var(--font-mono)",
            fontSize: "0.8rem",
            color: "var(--text-secondary)",
          }}
        >
          {used}
          <span style={{ color: "var(--text-muted)" }}>/</span>
          {total}
          {unit && (
            <span
              style={{
                color: "var(--text-muted)",
                fontSize: "0.7rem",
                marginLeft: 2,
              }}
            >
              {unit}
            </span>
          )}
        </span>
      </div>

      {/* Track */}
      <div
        style={{
          width: "100%",
          height: 6,
          background: "var(--bg-base)",
          borderRadius: 3,
          overflow: "hidden",
          position: "relative",
        }}
      >
        <motion.div
          initial={{ width: 0 }}
          animate={{ width: `${pct}%` }}
          transition={{ duration: 0.8, ease: [0.16, 1, 0.3, 1] }}
          style={{
            height: "100%",
            background: barColor,
            borderRadius: 3,
            boxShadow: `0 0 8px ${barColor}40`,
          }}
        />
      </div>

      {/* Available count */}
      <div
        style={{
          fontFamily: "var(--font-mono)",
          fontSize: "0.7rem",
          color: available > 0 ? "var(--st-completed)" : "var(--st-failed)",
        }}
      >
        {available} available
      </div>
    </div>
  );
}
