type ErrorBannerProps = {
  error: string | null;
};

export function ErrorBanner({ error }: ErrorBannerProps) {
  if (!error) return null;

  return (
    <div
      style={{
        padding: "var(--sp-3) var(--sp-4)",
        background: "var(--st-failed-dim)",
        border: "1px solid rgba(244,63,94,0.3)",
        borderRadius: "var(--radius-md)",
        color: "var(--st-failed)",
        fontFamily: "var(--font-mono)",
        fontSize: "0.8rem",
      }}
    >
      {error}
    </div>
  );
}
