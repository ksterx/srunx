import { motion } from "framer-motion";
import type { JobStatus } from "../lib/types.ts";

type StatusBadgeProps = {
  status: JobStatus;
  size?: "sm" | "md";
};

const STATUS_CONFIG: Record<JobStatus, { label: string; className: string }> = {
  UNKNOWN: { label: "Unknown", className: "badge-cancelled" },
  PENDING: { label: "Pending", className: "badge-pending" },
  RUNNING: { label: "Running", className: "badge-running" },
  COMPLETED: { label: "Completed", className: "badge-completed" },
  FAILED: { label: "Failed", className: "badge-failed" },
  CANCELLED: { label: "Cancelled", className: "badge-cancelled" },
  TIMEOUT: { label: "Timeout", className: "badge-timeout" },
};

export function StatusBadge({ status, size = "md" }: StatusBadgeProps) {
  const config = STATUS_CONFIG[status];
  const isRunning = status === "RUNNING";

  return (
    <span
      className={`badge ${config.className}`}
      style={{
        fontSize: size === "sm" ? "0.6rem" : undefined,
        padding: size === "sm" ? "1px 6px" : undefined,
      }}
    >
      {isRunning ? (
        <motion.span
          style={{
            width: 6,
            height: 6,
            borderRadius: "50%",
            background: "currentColor",
            display: "inline-block",
          }}
          animate={{ opacity: [1, 0.3, 1] }}
          transition={{ duration: 1.5, repeat: Infinity, ease: "easeInOut" }}
        />
      ) : (
        <span
          style={{
            width: 5,
            height: 5,
            borderRadius: "50%",
            background: "currentColor",
            display: "inline-block",
          }}
        />
      )}
      {config.label}
    </span>
  );
}
