import { useState } from "react";
import { useQuery } from "@tanstack/react-query";

import { Card, ErrorNote, Loading, PageHeader } from "../components/ui";
import { Empty, Tag, Td, Th } from "../components/DataTable";
import { Matrix, MatrixEmpty } from "../components/Matrix";
import type { MatrixRow, TotalRow } from "../components/Matrix";
import { api } from "../lib/api";
import { money, num, pnlClass, qty as fmtQty, signed } from "../lib/format";
import { Money, usePrivacy } from "../lib/privacy";
import type { RealisedPayload, RealisedScrip, Trade, TradesPayload } from "../lib/types";

function Row({ trade }: { trade: Trade }) {
  const gross = num(trade.gross);
  const net = num(trade.net);
  const short = trade.direction === "SHORT";

  return (
    <tr className="border-t" style={{ borderColor: "var(--hairline)" }}>
      <Td align="left" muted>
        {trade.closed_day}
      </Td>
      <Td align="left" muted>
        {trade.account}
      </Td>
      <Td align="left">
        <span className="font-medium">{trade.symbol}</span>
        <Tag title={trade.product_type}>{trade.product_type}</Tag>
      </Td>
      <Td align="left">
        <span
          className={`text-xs font-semibold ${
            short ? "text-[var(--loss)]" : "text-[var(--gain)]"
          }`}
        >
          {trade.direction}
        </span>
        <span className="ml-1.5 text-xs text-[var(--ink-muted)]">{trade.kind}</span>
      </Td>
      <Td>{fmtQty(num(trade.qty))}</Td>
      <Td muted>
        <Money>{money(num(trade.entry_price))}</Money>
      </Td>
      <Td muted>
        <Money>{money(num(trade.exit_price))}</Money>
      </Td>
      <Td tone={pnlClass(gross)}>
        <Money>{signed(gross)}</Money>
      </Td>
      <Td muted>
        {trade.charges === null ? (
          <span title="No charges recorded for the days this trade touched">—</span>
        ) : (
          <Money>{money(num(trade.charges))}</Money>
        )}
      </Td>
      <Td tone={pnlClass(net)}>
        <strong>
          {net === null ? (
            <span className="text-[var(--ink-muted)]" title="Cannot be netted without its charges">
              —
            </span>
          ) : (
            <Money>{signed(net)}</Money>
          )}
        </strong>
      </Td>
    </tr>
  );
}

/** Realised P&L by symbol and account.
 *
 *  Cells are net where the charges are known and gross where they are not — and
 *  the totals row says how many of each, so a cell is never quietly a different
 *  kind of number from its neighbour.
 */
function toMatrix(trades: Trade[], accounts: string[]) {
  const bySymbol = new Map<string, Trade[]>();
  for (const trade of trades) {
    const list = bySymbol.get(trade.symbol) ?? [];
    list.push(trade);
    bySymbol.set(trade.symbol, list);
  }
  const figure = (trade: Trade) => num(trade.net) ?? num(trade.gross) ?? 0;

  const rows: MatrixRow[] = [];
  for (const [symbol, made] of bySymbol) {
    const cells: Record<string, number | null | undefined> = {};
    for (const trade of made) {
      cells[trade.account] = (cells[trade.account] ?? 0) + figure(trade);
    }
    const uncosted = made.filter((t) => t.net === null).length;
    rows.push({
      key: symbol,
      label: symbol,
      note: uncosted ? "gross" : undefined,
      cells,
      total: made.reduce((sum, t) => sum + figure(t), 0),
      title: `${made.length} trade${made.length === 1 ? "" : "s"}${
        uncosted ? ` · ${uncosted} without charges, shown gross` : ""
      }`,
    });
  }

  const per = (pick: (t: Trade) => number) => {
    const values: Record<string, number | null> = {};
    for (const account of accounts) {
      const mine = trades.filter((t) => t.account === account);
      values[account] = mine.length ? mine.reduce((s, t) => s + pick(t), 0) : null;
    }
    return values;
  };

  const totals: TotalRow[] = [
    {
      label: "Gross", values: per((t) => num(t.gross) ?? 0),
      total: trades.reduce((s, t) => s + (num(t.gross) ?? 0), 0), tone: true,
    },
    {
      // An account with nothing costed shows a dash, not 0.00. A zero here
      // would claim its trading was free — the same confident-zero mistake the
      // apportioning code refuses to make.
      label: "Charges", hint: "estimated",
      values: Object.fromEntries(accounts.map((account) => {
        const costed = trades.filter((t) => t.account === account && t.charges !== null);
        return [account, costed.length
          ? costed.reduce((sum, t) => sum + (num(t.charges) ?? 0), 0)
          : null];
      })),
      total: trades.reduce((s, t) => s + (num(t.charges) ?? 0), 0),
    },
    {
      label: "Net", values: per(figure),
      total: trades.reduce((s, t) => s + figure(t), 0), tone: true,
    },
  ];

  return { rows, totals };
}

/** Realised P&L per scrip for the year, from the broker's own history.
 *
 *  Complete where matched trades are not: it includes shares bought years ago
 *  and sold in May, and everything bought and sold within the year. Our FIFO
 *  matching is the other view — per-trade detail the broker never provides,
 *  but only from the day the agents started recording.
 */
function realisedMatrix(scrips: RealisedScrip[], accounts: string[]) {
  const bySymbol = new Map<string, RealisedScrip[]>();
  for (const scrip of scrips) {
    const list = bySymbol.get(scrip.symbol) ?? [];
    list.push(scrip);
    bySymbol.set(scrip.symbol, list);
  }
  const figure = (s: RealisedScrip) => num(s.net) ?? num(s.gross) ?? 0;

  const rows: MatrixRow[] = [];
  for (const [symbol, entries] of bySymbol) {
    const cells: Record<string, number | null | undefined> = {};
    for (const entry of entries) cells[entry.account] = figure(entry);
    const uncosted = entries.filter((e) => e.net === null).length;
    rows.push({
      key: symbol,
      label: symbol,
      note: uncosted ? "gross" : undefined,
      cells,
      total: entries.reduce((sum, e) => sum + figure(e), 0),
      title: entries
        .map((e) => `${e.account}: traded on ${e.days} day${e.days === 1 ? "" : "s"}`)
        .join("\n"),
    });
  }

  const per = (pick: (s: RealisedScrip) => number, onlyCosted = false) =>
    Object.fromEntries(accounts.map((account) => {
      const mine = scrips.filter(
        (s) => s.account === account && (!onlyCosted || s.charges !== null));
      return [account, mine.length ? mine.reduce((sum, s) => sum + pick(s), 0) : null];
    }));

  const totals: TotalRow[] = [
    { label: "Gross", values: per((s) => num(s.gross) ?? 0),
      total: scrips.reduce((t, s) => t + (num(s.gross) ?? 0), 0), tone: true },
    { label: "Charges", hint: "estimated",
      values: per((s) => num(s.charges) ?? 0, true),
      total: scrips.reduce((t, s) => t + (num(s.charges) ?? 0), 0) },
    { label: "Net", values: per(figure),
      total: scrips.reduce((t, s) => t + figure(s), 0), tone: true },
  ];

  return { rows, totals };
}

export function TradesPage() {
  const { hidden, toggle } = usePrivacy();
  const [kind, setKind] = useState<"all" | "intraday" | "positional">("all");
  const [detail, setDetail] = useState(false);
  const [source, setSource] = useState<"year" | "matched">("year");
  const { data, isLoading, isError, error } = useQuery({
    queryKey: ["trades"],
    queryFn: () => api.get<TradesPayload>("/trades"),
    refetchInterval: 15000,
  });
  const year = useQuery({
    queryKey: ["realised"],
    queryFn: () => api.get<RealisedPayload>("/realised"),
    refetchInterval: 60000,
  });

  if (isError) return <ErrorNote error={error} />;
  if (isLoading || !data || year.isLoading) return <Loading what="trades" />;

  const shown = data.trades.filter((t) => kind === "all" || t.kind === kind);
  const yearData = year.data;
  const showYear = source === "year" && !!yearData?.available;
  const accounts = showYear ? (yearData?.accounts ?? data.accounts) : data.accounts;
  const matrix = showYear
    ? realisedMatrix(yearData!.scrips, accounts)
    : toMatrix(shown, accounts);
  const t = showYear ? yearData!.totals : data.totals;

  return (
    <>
      <PageHeader
        title="Trades"
        subtitle={
          <>
            {showYear
              ? `${yearData!.totals.scrips} scrips since ${yearData!.fy_start}`
              : `${data.totals.trades} closed round trips`}{" "}
            · gross{" "}
            <span className={pnlClass(num(t.gross))}>{signed(num(t.gross))}</span> · net{" "}
            <span className={pnlClass(num(t.net))}>{signed(num(t.net))}</span>
          </>
        }
        actions={
          <div className="flex gap-2">
            <div className="flex rounded border" style={{ borderColor: "var(--border)" }}>
              {([["year to date", "year"], ["matched trades", "matched"]] as const).map(
                ([label, value]) => (
                  <button
                    key={value}
                    onClick={() => setSource(value)}
                    className={`px-3 py-1.5 text-sm ${
                      source === value
                        ? "bg-black/5 font-medium dark:bg-white/10"
                        : "text-[var(--ink-secondary)]"
                    }`}
                    title={
                      value === "year"
                        ? "The broker's own realised P&L per scrip — complete from the start of the year"
                        : "Our FIFO matching of recorded fills — per-trade detail, from the day the agents started"
                    }
                  >
                    {label}
                  </button>
                ),
              )}
            </div>
            <div
              className="flex rounded border"
              style={{ borderColor: "var(--border)", opacity: showYear ? 0.4 : 1 }}
            >
              {([["grid", false], ["detail", true]] as const).map(([label, on]) => (
                <button
                  key={label}
                  onClick={() => setDetail(on)}
                  className={`px-3 py-1.5 text-sm capitalize ${
                    detail === on
                      ? "bg-black/5 font-medium dark:bg-white/10"
                      : "text-[var(--ink-secondary)]"
                  }`}
                >
                  {label}
                </button>
              ))}
            </div>
            <div className="flex rounded border" style={{ borderColor: "var(--border)" }}>
              {(["all", "intraday", "positional"] as const).map((option) => (
                <button
                  key={option}
                  onClick={() => setKind(option)}
                  className={`px-3 py-1.5 text-sm capitalize ${
                    kind === option ? "bg-black/5 dark:bg-white/10 font-medium" : "text-[var(--ink-secondary)]"
                  }`}
                >
                  {option}
                </button>
              ))}
            </div>
            <button
              onClick={toggle}
              className="rounded border px-3 py-1.5 text-sm"
              style={{ borderColor: "var(--border)" }}
            >
              {hidden ? "Show figures" : "Hide figures"}
            </button>
          </div>
        }
      />

      {/* A net total that silently omits the trades it could not cost is a
          smaller number pretending to be a complete one. */}
      {showYear && (
        <div
          className="mb-4 rounded border px-3 py-2 text-sm"
          style={{ borderColor: "var(--border)", color: "var(--ink-secondary)" }}
        >
          The broker's own realised P&amp;L per scrip since {yearData!.fy_start} — complete,
          including shares bought before the dashboard existed and sold this year. Switch
          to <strong>matched trades</strong> for entry and exit prices, holding period and
          the long/short split, which exist from the day the agents started recording.
        </div>
      )}

      {!showYear && (t as TradesPayload["totals"]).trades_without_charges > 0 && (
        <div
          className="mb-4 rounded border px-3 py-2 text-sm"
          style={{ borderColor: "var(--status-warning)", color: "var(--ink)" }}
        >
          {data.totals.trades_without_charges} of {data.totals.trades} trades have no
          charges recorded for the days they touched, so they are shown gross and excluded
          from the net total. Import them with <code>scripts/fetch_history.py</code>.
        </div>
      )}

      {(showYear || !detail) && (
        <Card>
          {matrix.rows.length === 0 ? (
            <MatrixEmpty>
              No {kind === "all" ? "closed trades yet" : `${kind} trades`}.
            </MatrixEmpty>
          ) : (
            <Matrix
              accounts={accounts}
              rows={matrix.rows}
              totals={matrix.totals}
              labelHeader="Symbol"
            />
          )}
        </Card>
      )}

      {!showYear && detail && (
      <Card className="overflow-x-auto">
        {shown.length === 0 ? (
          <Empty what={kind === "all" ? "closed trades yet" : `${kind} trades`} />
        ) : (
          <table className="w-full min-w-[1020px]">
            <thead>
              <tr>
                <Th align="left">Closed</Th>
                <Th align="left">Account</Th>
                <Th align="left">Symbol</Th>
                <Th align="left" help="Held overnight or not — from the days, not the product">
                  Side
                </Th>
                <Th>Qty</Th>
                <Th>Entry</Th>
                <Th>Exit</Th>
                <Th>Gross</Th>
                <Th help="Apportioned from the day's charges by turnover — an estimate">
                  Charges
                </Th>
                <Th>Net</Th>
              </tr>
            </thead>
            <tbody>
              {shown.map((trade, index) => (
                <Row key={`${trade.account}-${trade.symbol}-${trade.closed_day}-${index}`} trade={trade} />
              ))}
            </tbody>
          </table>
        )}
      </Card>
      )}

      <p className="mt-3 text-xs text-[var(--ink-muted)]">
        {!detail &&
          "One row per symbol, one column per account. Cells are net where the charges are known and gross where they are not — the row is marked accordingly. "}
        Matched FIFO from the fills the agents recorded, so per-trade P&amp;L exists from 1
        August onward. Charges are <strong>estimated</strong>: the broker reports them per
        day and per segment, never per symbol, so each trade takes its share of the days it
        touched in proportion to turnover. The exact year-to-date figure is on the
        Portfolio page.
      </p>
    </>
  );
}
