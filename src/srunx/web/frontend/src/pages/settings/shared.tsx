import { useEffect } from "react";
import { motion } from "framer-motion";
import { Check } from "lucide-react";

type FieldRowProps = {
  label: string;
  description?: React.ReactNode;
  children: React.ReactNode;
};

export function FieldRow({ label, description, children }: FieldRowProps) {
  return (
    <div
      style={{
        display: "flex",
        alignItems: "flex-start",
        justifyContent: "space-between",
        gap: "var(--sp-6)",
        padding: "var(--sp-3) 0",
        borderBottom: "1px solid var(--border-ghost)",
      }}
    >
      <div style={{ flex: 1, minWidth: 0 }}>
        <div
          style={{
            fontSize: "0.85rem",
            fontWeight: 500,
            color: "var(--text-primary)",
          }}
        >
          {label}
        </div>
        {description && (
          <div
            style={{
              fontSize: "0.75rem",
              color: "var(--text-muted)",
              marginTop: 2,
            }}
          >
            {description}
          </div>
        )}
      </div>
      <div style={{ flexShrink: 0, minWidth: 260 }}>{children}</div>
    </div>
  );
}

export function SuccessBanner({
  message,
  onDone,
}: {
  message: string | null;
  onDone: () => void;
}) {
  useEffect(() => {
    if (!message) return;
    const t = setTimeout(onDone, 3000);
    return () => clearTimeout(t);
  }, [message, onDone]);

  if (!message) return null;
  return (
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
        display: "flex",
        alignItems: "center",
        gap: "var(--sp-2)",
      }}
    >
      <Check size={14} />
      {message}
    </motion.div>
  );
}

export function NoticeBox({ children }: { children: React.ReactNode }) {
  return (
    <div
      className="panel"
      style={{
        padding: "var(--sp-6)",
        color: "var(--text-muted)",
        fontSize: "0.85rem",
        textAlign: "center",
      }}
    >
      {children}
    </div>
  );
}

/** Read a dotted path (``a.b.c``) out of a nested object. */
export function getIn(obj: unknown, path: string): unknown {
  let node: unknown = obj;
  for (const part of path.split(".")) {
    if (node === null || typeof node !== "object") return undefined;
    node = (node as Record<string, unknown>)[part];
  }
  return node;
}

export const monoSmall: React.CSSProperties = {
  fontFamily: "var(--font-mono)",
  fontSize: "0.72rem",
  color: "var(--text-muted)",
};
