import { useState } from "react";
import { Eye, Loader2, Copy, Check } from "lucide-react";
import { jobs } from "../lib/api.ts";
import { ErrorBanner } from "./ErrorBanner.tsx";
import type { ScriptPreviewRequest } from "../lib/types.ts";

type ScriptPreviewProps = {
  getRequest: () => ScriptPreviewRequest;
  disabled?: boolean;
  onValidationError?: () => void;
};

export function ScriptPreview({
  getRequest,
  disabled,
  onValidationError,
}: ScriptPreviewProps) {
  const [script, setScript] = useState<string | null>(null);
  const [templateUsed, setTemplateUsed] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [copied, setCopied] = useState(false);

  const handlePreview = async () => {
    if (disabled) {
      onValidationError?.();
      return;
    }
    setLoading(true);
    setError(null);
    try {
      const req = getRequest();
      const res = await jobs.preview(req);
      setScript(res.script);
      setTemplateUsed(res.template_used);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Preview failed");
      setScript(null);
    } finally {
      setLoading(false);
    }
  };

  const handleCopy = async () => {
    if (!script) return;
    try {
      await navigator.clipboard.writeText(script);
    } catch {
      return;
    }
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  return (
    <div
      style={{ display: "flex", flexDirection: "column", gap: "var(--sp-3)" }}
    >
      <button
        className="btn btn-ghost"
        onClick={handlePreview}
        disabled={loading}
        style={{ alignSelf: "flex-start", gap: 6 }}
      >
        {loading ? <Loader2 size={14} className="spin" /> : <Eye size={14} />}
        Preview Script
      </button>

      <ErrorBanner error={error} />

      {script && (
        <div
          style={{
            background: "var(--bg-base)",
            border: "1px solid var(--border-subtle)",
            borderRadius: "var(--radius-md)",
            overflow: "hidden",
          }}
        >
          <div
            style={{
              display: "flex",
              alignItems: "center",
              justifyContent: "space-between",
              padding: "var(--sp-2) var(--sp-3)",
              borderBottom: "1px solid var(--border-ghost)",
              fontSize: "0.7rem",
              color: "var(--text-muted)",
              textTransform: "uppercase",
              letterSpacing: "0.08em",
            }}
          >
            <span>Template: {templateUsed}</span>
            <button
              onClick={handleCopy}
              style={{
                background: "transparent",
                border: "none",
                color: "var(--text-muted)",
                cursor: "pointer",
                padding: 4,
                display: "flex",
                alignItems: "center",
                gap: 4,
                fontSize: "0.7rem",
              }}
            >
              {copied ? <Check size={12} /> : <Copy size={12} />}
              {copied ? "Copied" : "Copy"}
            </button>
          </div>
          <pre
            style={{
              padding: "var(--sp-3) var(--sp-4)",
              margin: 0,
              fontFamily: "var(--font-mono)",
              fontSize: "0.75rem",
              lineHeight: 1.6,
              color: "var(--text-secondary)",
              overflow: "auto",
              maxHeight: 400,
              whiteSpace: "pre",
            }}
          >
            {script}
          </pre>
        </div>
      )}
    </div>
  );
}
