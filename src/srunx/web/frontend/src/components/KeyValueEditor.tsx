import { useCallback } from "react";
import { motion, AnimatePresence } from "framer-motion";
import { Plus, X } from "lucide-react";

/* ── Types ───────────────────────────────────── */

export type KVEntry = { key: string; value: string };

type KeyValueEditorProps = {
  entries: KVEntry[];
  onChange: (entries: KVEntry[]) => void;
  keyPlaceholder?: string;
  valuePlaceholder?: string;
  addLabel?: string;
  compact?: boolean;
  hint?: string;
};

/* ── Component ───────────────────────────────── */

export function KeyValueEditor({
  entries,
  onChange,
  keyPlaceholder = "KEY",
  valuePlaceholder = "value",
  addLabel = "Add",
  compact = false,
  hint,
}: KeyValueEditorProps) {
  const update = useCallback(
    (idx: number, field: "key" | "value", val: string) => {
      const next = entries.map((e, i) =>
        i === idx ? { ...e, [field]: val } : e,
      );
      onChange(next);
    },
    [entries, onChange],
  );

  const remove = useCallback(
    (idx: number) => {
      onChange(entries.filter((_, i) => i !== idx));
    },
    [entries, onChange],
  );

  const add = useCallback(() => {
    onChange([...entries, { key: "", value: "" }]);
  }, [entries, onChange]);

  const gap = compact ? 6 : 8;
  const inputH = compact ? 28 : 32;
  const fontSize = compact ? "0.72rem" : "0.78rem";

  return (
    <div style={{ display: "flex", flexDirection: "column", gap }}>
      <AnimatePresence initial={false}>
        {entries.map((entry, idx) => (
          <motion.div
            key={idx}
            initial={{ opacity: 0, height: 0 }}
            animate={{ opacity: 1, height: "auto" }}
            exit={{ opacity: 0, height: 0 }}
            transition={{ duration: 0.15, ease: [0.16, 1, 0.3, 1] }}
            style={{ overflow: "hidden" }}
          >
            <div
              style={{
                display: "flex",
                alignItems: "center",
                gap: compact ? 4 : 6,
              }}
            >
              {/* Key input */}
              <input
                className="input"
                type="text"
                value={entry.key}
                onChange={(e) => update(idx, "key", e.target.value)}
                placeholder={keyPlaceholder}
                spellCheck={false}
                style={{
                  flex: compact ? "0 0 38%" : "0 0 35%",
                  height: inputH,
                  fontFamily: "var(--font-mono)",
                  fontSize,
                  color: "var(--accent)",
                  letterSpacing: "0.02em",
                  padding: "var(--sp-1) var(--sp-2)",
                }}
              />

              {/* Separator */}
              <span
                style={{
                  fontFamily: "var(--font-mono)",
                  fontSize: compact ? "0.75rem" : "0.85rem",
                  color: "var(--text-muted)",
                  flexShrink: 0,
                  userSelect: "none",
                }}
              >
                =
              </span>

              {/* Value input */}
              <input
                className="input"
                type="text"
                value={entry.value}
                onChange={(e) => update(idx, "value", e.target.value)}
                placeholder={valuePlaceholder}
                spellCheck={false}
                style={{
                  flex: 1,
                  minWidth: 0,
                  height: inputH,
                  fontFamily: "var(--font-mono)",
                  fontSize,
                  color: "var(--text-primary)",
                  letterSpacing: "0.01em",
                  padding: "var(--sp-1) var(--sp-2)",
                }}
              />

              {/* Remove button */}
              <button
                onClick={() => remove(idx)}
                style={{
                  flexShrink: 0,
                  width: 22,
                  height: 22,
                  display: "flex",
                  alignItems: "center",
                  justifyContent: "center",
                  background: "none",
                  border: "none",
                  color: "var(--text-muted)",
                  cursor: "pointer",
                  borderRadius: "var(--radius-sm)",
                  transition:
                    "color var(--duration-fast) var(--ease-out), background var(--duration-fast) var(--ease-out)",
                }}
                onMouseEnter={(e) => {
                  e.currentTarget.style.color = "var(--st-failed)";
                  e.currentTarget.style.background = "var(--st-failed-dim)";
                }}
                onMouseLeave={(e) => {
                  e.currentTarget.style.color = "var(--text-muted)";
                  e.currentTarget.style.background = "none";
                }}
              >
                <X size={12} />
              </button>
            </div>
          </motion.div>
        ))}
      </AnimatePresence>

      {/* Add button */}
      <button
        onClick={add}
        style={{
          display: "flex",
          alignItems: "center",
          gap: 4,
          padding: compact ? "4px 8px" : "5px 10px",
          background: "none",
          border: "1px dashed var(--border-subtle)",
          borderRadius: "var(--radius-sm)",
          color: "var(--text-muted)",
          fontFamily: "var(--font-mono)",
          fontSize: compact ? "0.65rem" : "0.7rem",
          letterSpacing: "0.04em",
          textTransform: "uppercase",
          cursor: "pointer",
          transition:
            "color var(--duration-fast) var(--ease-out), border-color var(--duration-fast) var(--ease-out), background var(--duration-fast) var(--ease-out)",
          alignSelf: "flex-start",
        }}
        onMouseEnter={(e) => {
          e.currentTarget.style.color = "var(--accent)";
          e.currentTarget.style.borderColor = "var(--accent)";
          e.currentTarget.style.background = "var(--accent-dim)";
        }}
        onMouseLeave={(e) => {
          e.currentTarget.style.color = "var(--text-muted)";
          e.currentTarget.style.borderColor = "var(--border-subtle)";
          e.currentTarget.style.background = "none";
        }}
      >
        <Plus size={12} />
        {addLabel}
      </button>

      {/* Hint text */}
      {hint && (
        <div
          style={{
            fontFamily: "var(--font-mono)",
            fontSize: "0.6rem",
            color: "var(--text-muted)",
            lineHeight: 1.4,
          }}
        >
          {hint}
        </div>
      )}
    </div>
  );
}
