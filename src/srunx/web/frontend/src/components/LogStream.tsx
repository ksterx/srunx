import { useEffect, useRef, useState } from "react";
import { motion, AnimatePresence } from "framer-motion";
import { ArrowDown } from "lucide-react";

type LogStreamProps = {
  lines: string[];
  stream?: "stdout" | "stderr";
  loading?: boolean;
};

export function LogStream({
  lines,
  stream = "stdout",
  loading,
}: LogStreamProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const [autoScroll, setAutoScroll] = useState(true);
  const [showScrollBtn, setShowScrollBtn] = useState(false);

  /* Auto-scroll to bottom on new lines */
  useEffect(() => {
    if (autoScroll && containerRef.current) {
      containerRef.current.scrollTop = containerRef.current.scrollHeight;
    }
  }, [lines, autoScroll]);

  const handleScroll = () => {
    if (!containerRef.current) return;
    const { scrollTop, scrollHeight, clientHeight } = containerRef.current;
    const atBottom = scrollHeight - scrollTop - clientHeight < 40;
    setAutoScroll(atBottom);
    setShowScrollBtn(!atBottom);
  };

  const scrollToBottom = () => {
    containerRef.current?.scrollTo({
      top: containerRef.current.scrollHeight,
      behavior: "smooth",
    });
    setAutoScroll(true);
  };

  return (
    <div
      style={{
        position: "relative",
        height: "100%",
        display: "flex",
        flexDirection: "column",
      }}
    >
      {/* Stream indicator */}
      <div
        style={{
          display: "flex",
          alignItems: "center",
          gap: 8,
          padding: "8px 16px",
          borderBottom: "1px solid var(--border-ghost)",
          background: "var(--bg-surface)",
        }}
      >
        <span
          style={{
            fontFamily: "var(--font-mono)",
            fontSize: "0.7rem",
            color:
              stream === "stderr" ? "var(--st-failed)" : "var(--st-completed)",
            textTransform: "uppercase",
            letterSpacing: "0.1em",
          }}
        >
          {stream}
        </span>
        <span
          style={{
            fontFamily: "var(--font-mono)",
            fontSize: "0.65rem",
            color: "var(--text-muted)",
          }}
        >
          {lines.length} lines
        </span>
        {loading && (
          <motion.span
            animate={{ opacity: [0.3, 1, 0.3] }}
            transition={{ duration: 1.5, repeat: Infinity }}
            style={{
              width: 6,
              height: 6,
              borderRadius: "50%",
              background: "var(--st-running)",
              marginLeft: "auto",
            }}
          />
        )}
      </div>

      {/* Log content */}
      <div
        ref={containerRef}
        onScroll={handleScroll}
        style={{
          flex: 1,
          overflow: "auto",
          padding: "12px 0",
          background: "var(--bg-base)",
          fontFamily: "var(--font-mono)",
          fontSize: "0.78rem",
          lineHeight: 1.7,
          counterReset: "line",
        }}
      >
        {lines.length === 0 && !loading && (
          <div
            style={{
              padding: "40px 16px",
              textAlign: "center",
              color: "var(--text-muted)",
              fontFamily: "var(--font-body)",
              fontSize: "0.85rem",
            }}
          >
            No output yet
          </div>
        )}
        {lines.map((line, i) => (
          <div
            key={i}
            style={{
              display: "flex",
              padding: "0 16px",
              transition: "background 100ms",
            }}
            onMouseEnter={(e) =>
              (e.currentTarget.style.background = "var(--bg-hover)")
            }
            onMouseLeave={(e) =>
              (e.currentTarget.style.background = "transparent")
            }
          >
            <span
              style={{
                width: 48,
                flexShrink: 0,
                color: "var(--text-ghost)",
                textAlign: "right",
                paddingRight: 16,
                userSelect: "none",
                borderRight: "1px solid var(--border-ghost)",
                marginRight: 16,
              }}
            >
              {i + 1}
            </span>
            <span
              style={{
                color:
                  stream === "stderr"
                    ? "var(--st-failed)"
                    : "var(--text-secondary)",
                whiteSpace: "pre-wrap",
                wordBreak: "break-all",
              }}
            >
              {line}
            </span>
          </div>
        ))}
      </div>

      {/* Scroll-to-bottom button */}
      <AnimatePresence>
        {showScrollBtn && (
          <motion.button
            initial={{ opacity: 0, y: 10 }}
            animate={{ opacity: 1, y: 0 }}
            exit={{ opacity: 0, y: 10 }}
            onClick={scrollToBottom}
            style={{
              position: "absolute",
              bottom: 16,
              right: 24,
              width: 36,
              height: 36,
              borderRadius: "50%",
              background: "var(--accent)",
              border: "none",
              color: "#fff",
              cursor: "pointer",
              display: "flex",
              alignItems: "center",
              justifyContent: "center",
              boxShadow: "var(--glow-accent)",
            }}
          >
            <ArrowDown size={16} />
          </motion.button>
        )}
      </AnimatePresence>
    </div>
  );
}
