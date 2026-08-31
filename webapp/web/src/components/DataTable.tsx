import type { ReactNode } from "react";

/** The table shell both Positions and Trades use.
 *
 *  Wide content scrolls inside its own container rather than making the page
 *  scroll sideways — on a screen used to compare rows, losing the account
 *  column off the left edge defeats the point.
 */
export function Th({ children, align = "right", help }: {
  children: ReactNode;
  align?: "left" | "right";
  help?: string;
}) {
  return (
    <th
      title={help}
      className={`whitespace-nowrap px-3 py-2 text-xs font-semibold uppercase tracking-wide text-[var(--ink-muted)] ${
        align === "right" ? "text-right" : "text-left"
      }`}
    >
      {children}
    </th>
  );
}

export function Td({ children, align = "right", muted, tone }: {
  children: ReactNode;
  align?: "left" | "right";
  muted?: boolean;
  tone?: string;
}) {
  return (
    <td
      className={`tnum whitespace-nowrap px-3 py-2 text-sm ${
        align === "right" ? "text-right" : "text-left"
      } ${muted ? "text-[var(--ink-muted)]" : ""} ${tone ?? ""}`}
    >
      {children}
    </td>
  );
}

export function Tag({ children, title }: { children: ReactNode; title?: string }) {
  return (
    <span
      title={title}
      className="ml-1.5 rounded px-1.5 py-0.5 text-[10px] uppercase tracking-wide text-[var(--ink-muted)]"
      style={{ border: "1px solid var(--border)" }}
    >
      {children}
    </span>
  );
}

export function Empty({ what }: { what: string }) {
  return <p className="py-10 text-center text-sm text-[var(--ink-muted)]">No {what}.</p>;
}
