import { useQuery } from "@tanstack/react-query";

import { Card } from "./ui";
import { Empty, Th } from "./DataTable";
import { api } from "../lib/api";
import { money, num, pnlClass, qty as fmtQty, signed } from "../lib/format";
import { Money } from "../lib/privacy";
import type { PositionsPayload, Trade, TradesPayload } from "../lib/types";

/** What has been traded lately — open and closed in one list.
 *
 *  On the order pad because that is where the question comes up: before placing
 *  anything, what am I already in, and what did the last few trades do. Split
 *  across Positions and Trades it takes two pages and a mental join.
 *
 *  Every line carries both the money and the percentage. An amount alone cannot
 *  be judged — ₹12,000 up is a good day on ₹2 lakh and noise on ₹40 lakh — and
 *  a percentage alone hides the size of the bet.
 */
const LIMIT = 20;

interface Line {
  key: string;
  account: string;
  symbol: string;
  side: string;
  qty: number;
  /** What it was entered at. */
  entry: number;
  /** The mark for an open line, the exit for a closed one. */
  out: number;
  pnl: number | null;
  pct: number | null;
  open: boolean;
  /** Present on a closed line only: net figures are after charges, and a null
   *  net means the day's charges are not in yet. */
  estimated?: boolean;
  when: string;
}

function closedLine(trade: Trade): Line {
  const quantity = num(trade.qty) ?? 0;
  const entry = num(trade.entry_price) ?? 0;
  // Net where the day's charges are known, gross where they are not — an
  // unknown cost is not a zero one, and hiding the trade would be worse.
  const net = num(trade.net);
  const pnl = net ?? num(trade.gross);
  const cost = Math.abs(quantity) * entry;
  return {
    key: `c-${trade.account}-${trade.symbol}-${trade.closed_at ?? trade.closed_day}-${trade.entry_price}`,
    account: trade.account,
    symbol: trade.symbol,
    side: trade.direction,
    qty: Math.abs(quantity),
    entry,
    out: num(trade.exit_price) ?? 0,
    pnl,
    pct: cost && pnl !== null ? (pnl / cost) * 100 : null,
    open: false,
    estimated: net === null,
    when: trade.closed_day,
  };
}

export function RecentTrades({ onPick }: { onPick?: (account: string, symbol: string) => void }) {
  const positions = useQuery({
    queryKey: ["positions"],
    queryFn: () => api.get<PositionsPayload>("/positions"),
    refetchInterval: 5000,
  });
  const trades = useQuery({
    queryKey: ["trades"],
    queryFn: () => api.get<TradesPayload>("/trades"),
    refetchInterval: 30_000,
  });

  // Open first: those are live money, and the closed ones are history however
  // recent. Within each group, most recent first.
  const open: Line[] = (positions.data?.positions ?? [])
    .filter((p) => p.net_qty !== 0 && !p.delivery_sale)
    .map((p) => {
      const cost = Math.abs(p.net_qty) * (p.avg_price || p.ltp || 0);
      return {
        key: `o-${p.account}-${p.symbol}-${p.product_type}`,
        account: p.account,
        symbol: p.symbol,
        side: p.net_qty > 0 ? "LONG" : "SHORT",
        qty: Math.abs(p.net_qty),
        entry: p.avg_price,
        out: p.ltp,
        pnl: p.unrealised,
        pct: cost ? (p.unrealised / cost) * 100 : null,
        open: true,
        when: p.opened_today ? "today" : p.carried ? "carried" : "",
      };
    })
    .sort((a, b) => Math.abs(b.pnl ?? 0) - Math.abs(a.pnl ?? 0));

  const closed: Line[] = (trades.data?.trades ?? []).map(closedLine);
  const lines = [...open, ...closed].slice(0, LIMIT);

  return (
    <Card className="mt-4 overflow-x-auto">
      <div
        className="flex items-baseline gap-2 border-b px-4 py-2.5"
        style={{ borderColor: "var(--hairline)" }}
      >
        <span className="text-sm font-semibold">Recent</span>
        <span className="text-xs text-[var(--ink-muted)]">
          {open.length} open, then the latest closed — {lines.length} shown
        </span>
      </div>

      {lines.length === 0 ? (
        <Empty what="trades yet" />
      ) : (
        <table className="w-full min-w-[720px]">
          <thead>
            <tr>
              <Th align="left">Account</Th>
              <Th align="left">Symbol</Th>
              <Th align="left">Side</Th>
              <Th>Qty</Th>
              <Th help="What it was entered at">In</Th>
              <Th help="The mark on an open line, the exit on a closed one">Out</Th>
              <Th help="Unrealised while open, realised net of charges once closed">
                P&amp;L
              </Th>
              <Th align="right">State</Th>
            </tr>
          </thead>
          <tbody>
            {lines.map((line) => (
              <tr
                key={line.key}
                onClick={() => onPick?.(line.account, line.symbol)}
                className={`border-t ${onPick ? "cursor-pointer hover:bg-black/[0.03] dark:hover:bg-white/[0.04]" : ""}`}
                style={{ borderColor: "var(--hairline)" }}
                title={onPick ? "Load this account and symbol into the pad" : undefined}
              >
                <td className="px-3 py-1.5 text-sm text-[var(--ink-muted)]">{line.account}</td>
                <td className="px-3 py-1.5 text-sm font-medium">{line.symbol}</td>
                <td className="px-3 py-1.5">
                  <span
                    className={`text-xs font-semibold ${
                      line.side === "LONG" || line.side === "BUY"
                        ? "text-[var(--gain)]"
                        : "text-[var(--loss)]"
                    }`}
                  >
                    {line.side}
                  </span>
                </td>
                <td className="tnum px-3 py-1.5 text-right text-sm">{fmtQty(line.qty)}</td>
                <td className="tnum px-3 py-1.5 text-right text-sm text-[var(--ink-muted)]">
                  <Money>{money(line.entry)}</Money>
                </td>
                <td className="tnum px-3 py-1.5 text-right text-sm">
                  <Money>{money(line.out)}</Money>
                </td>
                <td className={`tnum px-3 py-1.5 text-right text-sm ${pnlClass(line.pnl)}`}>
                  <strong>
                    <Money>{signed(line.pnl)}</Money>
                  </strong>
                  {line.pct !== null && (
                    <span className="ml-1 text-xs opacity-70">
                      ({line.pct > 0 ? "+" : line.pct < 0 ? "−" : ""}
                      {Math.abs(line.pct).toFixed(2)}%)
                    </span>
                  )}
                  {line.estimated && (
                    <span
                      className="ml-1 text-xs text-[var(--ink-muted)]"
                      title="Gross — the day's charges are not in yet"
                    >
                      gross
                    </span>
                  )}
                </td>
                <td className="px-3 py-1.5 text-right text-xs text-[var(--ink-muted)]">
                  {line.open ? (
                    <span className="font-medium text-[var(--ink-secondary)]">
                      open{line.when && ` · ${line.when}`}
                    </span>
                  ) : (
                    line.when
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </Card>
  );
}
