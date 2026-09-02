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
 *
 *  Ordered by when the trade happened, never by its P&L. Sorting by size looks
 *  reasonable and is unusable: unrealised P&L moves on every tick, so the rows
 *  reshuffled themselves while being read. A row only moves here when something
 *  is actually opened or closed.
 *
 *  Every open position is listed, always. They are what is held, and dropping
 *  one to make room for a closed trade would hide live money. Only the closed
 *  tail is capped.
 */
const CLOSED_SHOWN = 20;

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
  /** What the list is ordered by: entry day for an open line, exit day for a
   *  closed one. Empty sorts last, which is where a position the store never
   *  saw opened belongs — it predates the history. */
  at: string;
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
    at: `${trade.closed_day} ${trade.closed_at ?? ""}`,
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

  const open: Line[] = (positions.data?.positions ?? [])
    .filter((p) => p.net_qty !== 0 && !p.delivery_sale)
    .map((p) => {
      const cost = Math.abs(p.net_qty) * (p.avg_price || p.ltp || 0);
      return {
        at: `${p.opened_day ?? ""} ${p.opened_at ?? ""}`.trim(),
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
        when: p.opened_day ?? (p.carried ? "carried" : ""),
      };
    });

  // Newest first, and stable: the tie-break is the row's own key, so two
  // trades closed in the same second keep their order between refreshes rather
  // than swapping places on whichever arrived first.
  const byTime = (a: Line, b: Line) =>
    b.at.localeCompare(a.at) || a.key.localeCompare(b.key);

  const closed: Line[] = (trades.data?.trades ?? [])
    .map(closedLine)
    .sort(byTime)
    .slice(0, CLOSED_SHOWN);
  const lines = [...open, ...closed].sort(byTime);

  return (
    <Card className="mt-4 overflow-x-auto">
      <div
        className="flex items-baseline gap-2 border-b px-4 py-2.5"
        style={{ borderColor: "var(--hairline)" }}
      >
        <span className="text-sm font-semibold">Recent</span>
        <span className="text-xs text-[var(--ink-muted)]">
          every open position, and the {CLOSED_SHOWN} most recent closes — newest first
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
