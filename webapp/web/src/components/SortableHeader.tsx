import { useMemo, useState } from "react";

export type SortDir = "asc" | "desc";
export interface SortState {
  key: string | null;
  dir: SortDir;
}

/** Sorting for a table of rows keyed by column name.
 *
 *  Nulls always sort last regardless of direction — a row with no position in
 *  an account should not jump to the top because you sorted by that account's
 *  P&L.
 */
export function useSort<T>(
  rows: T[],
  value: (row: T, key: string) => string | number | null | undefined,
  initial: SortState = { key: null, dir: "asc" },
) {
  const [sort, setSort] = useState<SortState>(initial);

  const sorted = useMemo(() => {
    if (!sort.key) return rows;
    const key = sort.key;
    const factor = sort.dir === "asc" ? 1 : -1;
    return [...rows].sort((a, b) => {
      const av = value(a, key);
      const bv = value(b, key);
      const aMissing = av === null || av === undefined;
      const bMissing = bv === null || bv === undefined;
      if (aMissing && bMissing) return 0;
      if (aMissing) return 1;
      if (bMissing) return -1;
      if (typeof av === "string" || typeof bv === "string") {
        return String(av).localeCompare(String(bv)) * factor;
      }
      return (Number(av) - Number(bv)) * factor;
    });
  }, [rows, sort, value]);

  function toggle(key: string) {
    setSort((s) =>
      s.key === key ? { key, dir: s.dir === "asc" ? "desc" : "asc" } : { key, dir: "desc" },
    );
  }

  return { sorted, sort, toggle };
}

export function SortHeader({
  label,
  sortKey,
  sort,
  onToggle,
  align = "right",
  sub,
  sticky,
}: {
  label: string;
  sortKey: string;
  sort: SortState;
  onToggle: (key: string) => void;
  align?: "left" | "right";
  sub?: string;
  sticky?: boolean;
}) {
  const active = sort.key === sortKey;
  return (
    <th
      className={`whitespace-nowrap px-3 py-2 text-xs font-semibold ${
        align === "left" ? "text-left" : "text-right"
      } ${sticky ? "sticky left-0 z-20" : ""}`}
      style={sticky ? { background: "var(--surface)" } : undefined}
    >
      <button
        onClick={() => onToggle(sortKey)}
        className={`group inline-flex items-center gap-1 ${
          align === "left" ? "" : "flex-row-reverse"
        } ${active ? "text-[var(--ink)]" : "text-[var(--ink-muted)] hover:text-[var(--ink-secondary)]"}`}
      >
        <span
          aria-hidden
          className={`text-[9px] leading-none ${
            active ? "opacity-100" : "opacity-0 group-hover:opacity-40"
          }`}
        >
          {active && sort.dir === "desc" ? "▼" : "▲"}
        </span>
        <span className="uppercase tracking-wide">{label}</span>
      </button>
      {sub && (
        <div className="text-[10px] font-normal normal-case text-[var(--ink-muted)]">{sub}</div>
      )}
    </th>
  );
}
