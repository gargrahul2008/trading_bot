import type { ReactNode } from "react";

export function Card({ children, className = "" }: { children: ReactNode; className?: string }) {
  return (
    <div
      className={`rounded-lg border bg-[var(--surface)] ${className}`}
      style={{ borderColor: "var(--border)" }}
    >
      {children}
    </div>
  );
}

export function PageHeader({
  title,
  subtitle,
  actions,
}: {
  title: string;
  subtitle?: ReactNode;
  actions?: ReactNode;
}) {
  return (
    <div className="mb-5 flex flex-wrap items-end justify-between gap-3">
      <div>
        <h1 className="text-xl font-semibold tracking-tight">{title}</h1>
        {subtitle && (
          <p className="mt-1 text-sm text-[var(--ink-secondary)]">{subtitle}</p>
        )}
      </div>
      {actions}
    </div>
  );
}

export function Button({
  children,
  onClick,
  type = "button",
  disabled,
}: {
  children: ReactNode;
  onClick?: () => void;
  type?: "button" | "submit";
  disabled?: boolean;
}) {
  return (
    <button
      type={type}
      onClick={onClick}
      disabled={disabled}
      className="rounded border px-3 py-1.5 text-sm font-medium transition hover:bg-black/5 disabled:opacity-50 dark:hover:bg-white/10"
      style={{ borderColor: "var(--border)" }}
    >
      {children}
    </button>
  );
}

export function Loading({ what }: { what: string }) {
  return (
    <p className="py-10 text-center text-sm text-[var(--ink-muted)]">Loading {what}…</p>
  );
}

export function ErrorNote({ error }: { error: unknown }) {
  const message = error instanceof Error ? error.message : String(error);
  return (
    <div
      className="rounded border p-3 text-sm"
      style={{ borderColor: "var(--status-critical)", color: "var(--status-critical)" }}
    >
      {message}
    </div>
  );
}

/** A headline figure. The label sits above so the number is what the eye lands
 *  on; `note` carries the qualifier that stops it being misread. */
export function Stat({
  label,
  value,
  note,
  tone,
}: {
  label: string;
  value: ReactNode;
  note?: ReactNode;
  tone?: string;
}) {
  return (
    <Card className="p-4">
      <div className="text-xs uppercase tracking-wide text-[var(--ink-muted)]">{label}</div>
      <div className={`mt-1 text-2xl font-semibold ${tone ?? ""}`}>{value}</div>
      {note && <div className="mt-1 text-xs text-[var(--ink-secondary)]">{note}</div>}
    </Card>
  );
}
