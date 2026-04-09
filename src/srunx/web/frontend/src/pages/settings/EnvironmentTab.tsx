import { useEffect, useState, useCallback } from "react";
import { Variable } from "lucide-react";
import { config as configApi } from "../../lib/api.ts";
import type { EnvVarInfo } from "../../lib/types.ts";
import { ErrorBanner } from "../../components/ErrorBanner.tsx";

export function EnvironmentTab() {
  const [vars, setVars] = useState<EnvVarInfo[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const load = useCallback(async () => {
    try {
      setLoading(true);
      const data = await configApi.envVars();
      setVars(data);
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to load env vars");
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    load();
  }, [load]);

  if (loading) {
    return <div className="panel skeleton" style={{ height: 200 }} />;
  }

  return (
    <div
      style={{ display: "flex", flexDirection: "column", gap: "var(--sp-4)" }}
    >
      <ErrorBanner error={error} />

      <div className="panel">
        <div className="panel-header">
          <h3>
            <Variable size={14} style={{ marginRight: 8, verticalAlign: -2 }} />
            Active Environment Variables
          </h3>
        </div>
        <div className="panel-body">
          <p
            style={{
              fontSize: "0.75rem",
              color: "var(--text-muted)",
              marginBottom: "var(--sp-4)",
            }}
          >
            Environment variables override config file values. These are
            currently set in the server process.
          </p>

          {vars.length === 0 ? (
            <div
              style={{
                textAlign: "center",
                padding: "var(--sp-6)",
                color: "var(--text-muted)",
                fontSize: "0.85rem",
              }}
            >
              No SRUNX_* environment variables are currently set
            </div>
          ) : (
            <div
              style={{
                display: "flex",
                flexDirection: "column",
                gap: "var(--sp-2)",
              }}
            >
              {vars.map((v) => (
                <div
                  key={v.name}
                  style={{
                    display: "flex",
                    alignItems: "center",
                    gap: "var(--sp-3)",
                    padding: "var(--sp-3) var(--sp-4)",
                    background: "var(--bg-base)",
                    borderRadius: "var(--radius-md)",
                    border: "1px solid var(--border-ghost)",
                  }}
                >
                  <span
                    style={{
                      fontFamily: "var(--font-mono)",
                      fontSize: "0.8rem",
                      color: "var(--accent)",
                      minWidth: 240,
                      flexShrink: 0,
                    }}
                  >
                    {v.name}
                  </span>
                  <span
                    style={{
                      fontFamily: "var(--font-mono)",
                      fontSize: "0.8rem",
                      color: "var(--text-primary)",
                      flex: 1,
                    }}
                  >
                    {v.value}
                  </span>
                  <span
                    style={{
                      fontSize: "0.7rem",
                      color: "var(--text-muted)",
                      flexShrink: 0,
                      maxWidth: 200,
                    }}
                  >
                    {v.description}
                  </span>
                </div>
              ))}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
