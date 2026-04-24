import { useCallback, useEffect, useRef, useState } from "react";
import { motion } from "framer-motion";
import { Download } from "lucide-react";
import { jobs as jobsApi } from "../lib/api.ts";
import { LogStream } from "./LogStream.tsx";

const MAX_LOG_LINES = 10000;

type JobLogsTabProps = {
  jobId: number;
  isRunning: boolean;
};

// Offset-based log streaming extracted from the old standalone
// ``LogViewer`` page so both the job detail page and any future re-use
// site share one implementation.
export function JobLogsTab({ jobId, isRunning }: JobLogsTabProps) {
  const [activeStream, setActiveStream] = useState<"stdout" | "stderr">(
    "stdout",
  );
  const [stdoutLines, setStdoutLines] = useState<string[]>([]);
  const [stderrLines, setStderrLines] = useState<string[]>([]);
  const [initialLoading, setInitialLoading] = useState(true);
  const offsetRef = useRef({ stdout: 0, stderr: 0 });
  const mountedRef = useRef(true);

  const fetchLogs = useCallback(async () => {
    try {
      const data = await jobsApi.logs(jobId, {
        stdout_offset: offsetRef.current.stdout,
        stderr_offset: offsetRef.current.stderr,
      });
      if (!mountedRef.current) return;

      if (data.stdout) {
        const newLines = data.stdout.split("\n").filter(Boolean);
        setStdoutLines((prev) => [...prev, ...newLines].slice(-MAX_LOG_LINES));
      }
      if (data.stderr) {
        const newLines = data.stderr.split("\n").filter(Boolean);
        setStderrLines((prev) => [...prev, ...newLines].slice(-MAX_LOG_LINES));
      }
      offsetRef.current.stdout = data.stdout_offset;
      offsetRef.current.stderr = data.stderr_offset;
    } catch {
      // Ignore transient errors during polling
    } finally {
      if (mountedRef.current) setInitialLoading(false);
    }
  }, [jobId]);

  useEffect(() => {
    mountedRef.current = true;
    setInitialLoading(true);
    // Reset streams when jobId changes (tab re-mount).
    setStdoutLines([]);
    setStderrLines([]);
    offsetRef.current = { stdout: 0, stderr: 0 };
    fetchLogs();
    return () => {
      mountedRef.current = false;
    };
  }, [fetchLogs]);

  useEffect(() => {
    if (!isRunning) return;
    const timer = setInterval(fetchLogs, 3000);
    return () => clearInterval(timer);
  }, [isRunning, fetchLogs]);

  const downloadActive = () => {
    const lines = activeStream === "stdout" ? stdoutLines : stderrLines;
    const blob = new Blob([lines.join("\n")], { type: "text/plain" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `job-${jobId}-${activeStream}.log`;
    a.click();
    setTimeout(() => URL.revokeObjectURL(url), 100);
  };

  return (
    <div
      style={{
        display: "flex",
        flexDirection: "column",
        gap: "var(--sp-3)",
        height: "100%",
        minHeight: 400,
      }}
    >
      <div
        style={{
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
        }}
      >
        <div
          style={{
            display: "flex",
            borderBottom: "1px solid var(--border-subtle)",
            gap: 0,
          }}
        >
          {(["stdout", "stderr"] as const).map((tab) => {
            const lines = tab === "stdout" ? stdoutLines : stderrLines;
            const active = activeStream === tab;
            return (
              <button
                key={tab}
                onClick={() => setActiveStream(tab)}
                style={{
                  padding: "8px 16px",
                  background: "transparent",
                  border: "none",
                  borderBottom: active
                    ? `2px solid ${tab === "stderr" ? "var(--st-failed)" : "var(--st-completed)"}`
                    : "2px solid transparent",
                  color: active ? "var(--text-primary)" : "var(--text-muted)",
                  cursor: "pointer",
                  fontFamily: "var(--font-mono)",
                  fontSize: "0.78rem",
                  display: "flex",
                  alignItems: "center",
                  gap: 8,
                  transition: "all 150ms",
                }}
              >
                <span style={{ textTransform: "uppercase" }}>{tab}</span>
                <span
                  style={{
                    fontFamily: "var(--font-mono)",
                    fontSize: "0.65rem",
                    padding: "1px 6px",
                    borderRadius: 3,
                    background: active
                      ? "var(--bg-overlay)"
                      : "var(--bg-raised)",
                    color: "var(--text-secondary)",
                  }}
                >
                  {lines.length}
                </span>
              </button>
            );
          })}
        </div>

        <button
          type="button"
          className="btn btn-ghost"
          onClick={downloadActive}
          style={{ padding: "4px 10px", fontSize: "0.75rem" }}
        >
          <Download size={12} />
          Download
        </button>
      </div>

      <motion.div
        initial={{ opacity: 0 }}
        animate={{ opacity: 1 }}
        className="panel"
        style={{ flex: 1, overflow: "hidden" }}
      >
        <LogStream
          lines={activeStream === "stdout" ? stdoutLines : stderrLines}
          stream={activeStream}
          loading={isRunning || initialLoading}
        />
      </motion.div>
    </div>
  );
}
