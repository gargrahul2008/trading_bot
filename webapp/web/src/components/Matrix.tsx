import type { ReactNode } from "react";

import { SortHeader, useSort } from "./SortableHeader";
import type { SortState } from "./SortableHeader";
import { money, pnlClass, signed } from "../lib/format";
import { usePrivacy } from "../lib/privacy";

/** A symbol × account grid.
 *
 *  The layout the client's P&L page uses, and the right one for this dashboard:
 *  its whole purpose is reading several accounts at once, and a row per position
 *  buries that. One row per symbol, one column per account, so a name held in
 *  three places is one line rather than three.
 *
 *  Three details carried over from that page, each earning its place:
 *
 *  - **Totals sit above the rows, not below.** They are the figures you came for;
 *    a footer puts them past a scroll on a long list.
 *  - **The symbol column is sticky.** Scrolling right to reach the sixth account
 *    must not take the name off screen, or the row means nothing.
 *  - **An empty cell is `·`, not `0.00`.** Zero is a real value — a closed
 *    position, a trade that broke even. Nothing held is not, and they should not
 *    look alike.
 */
export interface MatrixRow {
  key: string;
  label: string;
  note?: string;
  /** account -> value. Missing means the account has no such row at all. */
  cells: Record<string, number | null | undefined>;
  total: number;
  /** account -> the same figure as a percentage of what it was measured
   *  against. An amount alone cannot be judged: ₹12,000 up is a good day on
   *  ₹2 lakh and a rounding error on ₹40 lakh. */
  percents?: Record<string, number | null | undefined>;
  totalPercent?: number | null;
  title?: string;
}

export interface TotalRow {
  label: string;
  hint?: string;
  values: Record<string, number | null>;
  total: number | null;
  /** Colour by sign. Off for figures that are neither good nor bad, like cost. */
  tone?: boolean;
  percent?: boolean;
}

export function Matrix({
  accounts,
  rows,
  totals,
  labelHeader,
  emptyCell = "·",
  initialSort,
}: {
  accounts: string[];
  rows: MatrixRow[];
  totals: TotalRow[];
  labelHeader: string;
  emptyCell?: string;
  initialSort?: SortState;
}) {
  const { hidden } = usePrivacy();
  const mask = (text: string) => (hidden ? <span className="masked">{text}</span> : text);

  const { sorted, sort, toggle } = useSort<MatrixRow>(
    rows,
    (row, key) => (key === "__label" ? row.label : key === "__total" ? row.total : row.cells[key]),
    initialSort ?? { key: "__total", dir: "desc" },
  );

  const fmt = (value: number | null | undefined, percent?: boolean) => {
    if (value === null || value === undefined) return "—";
    return percent ? `${value > 0 ? "+" : value < 0 ? "−" : ""}${Math.abs(value).toFixed(2)}%` : signed(value);
  };

  return (
    <div className="overflow-x-auto">
      <table className="w-full text-sm">
        <thead>
          <tr className="border-b" style={{ borderColor: "var(--hairline)" }}>
            <SortHeader
              label={labelHeader}
              sortKey="__label"
              sort={sort}
              onToggle={toggle}
              align="left"
              sticky
            />
            {accounts.map((account) => (
              <SortHeader
                key={account}
                label={account}
                sortKey={account}
                sort={sort}
                onToggle={toggle}
              />
            ))}
            <SortHeader label="Total" sortKey="__total" sort={sort} onToggle={toggle} />
          </tr>

          {totals.map((row, index) => (
            <tr
              key={row.label}
              className={index === totals.length - 1 ? "border-b-2" : "border-b"}
              style={{
                borderColor: index === totals.length - 1 ? "var(--border)" : "var(--hairline)",
                background: "color-mix(in srgb, var(--ink) 4%, var(--surface))",
              }}
            >
              <td
                className="sticky left-0 z-10 whitespace-nowrap px-3 py-1.5 font-medium"
                style={{ background: "color-mix(in srgb, var(--ink) 4%, var(--surface))" }}
              >
                {row.label}
                {row.hint && (
                  <span className="ml-2 text-xs font-normal text-[var(--ink-muted)]">
                    {row.hint}
                  </span>
                )}
              </td>
              {accounts.map((account) => (
                <td
                  key={account}
                  className={`tnum px-3 py-1.5 text-right ${
                    row.tone ? pnlClass(row.values[account]) : ""
                  }`}
                >
                  {mask(
                    row.tone || row.percent
                      ? fmt(row.values[account], row.percent)
                      : money(row.values[account]),
                  )}
                </td>
              ))}
              <td
                className={`tnum border-l px-3 py-1.5 text-right font-semibold ${
                  row.tone ? pnlClass(row.total) : ""
                }`}
                style={{ borderColor: "var(--border)" }}
              >
                {mask(row.tone || row.percent ? fmt(row.total, row.percent) : money(row.total))}
              </td>
            </tr>
          ))}
        </thead>

        <tbody>
          {sorted.map((row) => (
            <tr
              key={row.key}
              className="border-b hover:bg-black/[0.03] dark:hover:bg-white/[0.04]"
              style={{ borderColor: "var(--hairline)" }}
            >
              <td
                className="sticky left-0 z-10 whitespace-nowrap px-3 py-1.5 font-medium"
                style={{ background: "var(--surface)" }}
                title={row.title}
              >
                {row.label}
                {row.note && (
                  <span className="ml-2 text-[10px] uppercase tracking-wide text-[var(--ink-muted)]">
                    {row.note}
                  </span>
                )}
              </td>
              {accounts.map((account) => {
                const value = row.cells[account];
                // Undefined means this account has no such row at all; zero is a
                // real value and must not look the same.
                if (value === null || value === undefined) {
                  return (
                    <td key={account} className="px-3 py-1.5 text-right text-[var(--ink-muted)]">
                      {emptyCell}
                    </td>
                  );
                }
                const pct = row.percents?.[account];
                return (
                  <td
                    key={account}
                    className={`tnum px-3 py-1.5 text-right ${pnlClass(value)}`}
                  >
                    {mask(signed(value))}
                    {pct !== null && pct !== undefined && (
                      <span className="ml-1 text-xs opacity-70">
                        ({fmt(pct, true)})
                      </span>
                    )}
                  </td>
                );
              })}
              <td
                className={`tnum border-l px-3 py-1.5 text-right font-medium ${pnlClass(row.total)}`}
                style={{ borderColor: "var(--border)" }}
              >
                {mask(signed(row.total))}
                {row.totalPercent !== null && row.totalPercent !== undefined && (
                  <span className="ml-1 text-xs font-normal opacity-70">
                    ({fmt(row.totalPercent, true)})
                  </span>
                )}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export function MatrixEmpty({ children }: { children: ReactNode }) {
  return <p className="py-10 text-center text-sm text-[var(--ink-muted)]">{children}</p>;
}
