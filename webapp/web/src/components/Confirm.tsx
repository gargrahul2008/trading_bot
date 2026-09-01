import type { ReactNode } from "react";

/** The step between meaning to trade and trading.
 *
 *  It exists to be read, so it states the action in words rather than showing
 *  the form again — a second look at the same boxes is not a check. The account
 *  is the largest thing on it, because placing into the wrong one of six is the
 *  mistake this dashboard makes easiest.
 */
export function Confirm({
  title,
  account,
  lines,
  warning,
  confirmLabel,
  danger,
  busy,
  onConfirm,
  onCancel,
}: {
  title: string;
  account: string;
  lines: { label: string; value: ReactNode }[];
  warning?: ReactNode;
  confirmLabel: string;
  danger?: boolean;
  busy?: boolean;
  onConfirm: () => void;
  onCancel: () => void;
}) {
  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center p-4"
      style={{ background: "rgba(0,0,0,0.45)" }}
      onClick={onCancel}
    >
      <div
        className="w-full max-w-md rounded-lg border p-5"
        style={{ background: "var(--surface)", borderColor: "var(--border)" }}
        onClick={(event) => event.stopPropagation()}
      >
        <h2 className="text-base font-semibold">{title}</h2>

        <div
          className="mt-3 rounded px-3 py-2"
          style={{ background: "color-mix(in srgb, var(--ink) 5%, var(--surface))" }}
        >
          <div className="text-xs uppercase tracking-wide text-[var(--ink-muted)]">
            Account
          </div>
          <div className="text-lg font-semibold">{account}</div>
        </div>

        <dl className="mt-3 space-y-1.5 text-sm">
          {lines.map((line) => (
            <div key={line.label} className="flex justify-between gap-4">
              <dt className="text-[var(--ink-secondary)]">{line.label}</dt>
              <dd className="tnum text-right font-medium">{line.value}</dd>
            </div>
          ))}
        </dl>

        {warning && (
          <div
            className="mt-3 rounded border px-3 py-2 text-sm"
            style={{ borderColor: "var(--status-warning)" }}
          >
            {warning}
          </div>
        )}

        <div className="mt-5 flex gap-2">
          <button
            onClick={onCancel}
            disabled={busy}
            className="flex-1 rounded border px-3 py-2 text-sm"
            style={{ borderColor: "var(--border)" }}
          >
            Cancel
          </button>
          <button
            onClick={onConfirm}
            disabled={busy}
            className="flex-1 rounded px-3 py-2 text-sm font-semibold text-white disabled:opacity-50"
            style={{ background: danger ? "var(--status-critical)" : "var(--accent)" }}
          >
            {busy ? "Sending…" : confirmLabel}
          </button>
        </div>
      </div>
    </div>
  );
}
